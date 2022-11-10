// Copyright 2015-2021 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, num::NonZeroUsize};

use futures::{future::join, Future, StreamExt};
use swim_api::{
    agent::LaneConfig,
    meta::{
        lane::{LaneInfo, LaneKind},
        uplink::NodePulse,
    },
    protocol::{
        agent::{LaneResponse, LaneResponseDecoder},
        map::{MapOperation, MapOperationDecoder},
        WithLenRecognizerDecoder,
    },
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Text;
use swim_runtime::agent::reporting::UplinkReporter;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::{
    config::IntrospectionConfig,
    meta_agent::{
        test_harness::{introspection_agent_test, IntrospectionTestContext},
        PULSE_LANE,
    },
    model::AgentIntrospectionUpdater,
    task::IntrospectionMessage,
};

use super::{run_lanes_descriptor_lane, NodeMetaAgent, LANES_LANE};
use crate::meta_agent::tests::LaneSender;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

struct TestContext {
    shutdown_tx: trigger::Sender,
    updater: AgentIntrospectionUpdater,
    sender: LaneSender,
    receiver: LaneReceiver,
    agg_reporter: UplinkReporter,
}

const SYNC_ID: Uuid = Uuid::from_u128(9727474);

type RespDecoder = LaneResponseDecoder<MapOperationDecoder<Text, LaneInfo>>;

struct LaneReceiver {
    reader: FramedRead<ByteReader, RespDecoder>,
}

impl LaneReceiver {
    fn new(reader: ByteReader) -> Self {
        LaneReceiver {
            reader: FramedRead::new(
                reader,
                LaneResponseDecoder::new(MapOperationDecoder::default()),
            ),
        }
    }

    async fn expect_sync_message(&mut self) -> Option<(Text, LaneInfo)> {
        let record = self
            .reader
            .next()
            .await
            .expect("Expected a record.")
            .expect("Bad response.");
        match record {
            LaneResponse::Synced(id) => {
                assert_eq!(id, SYNC_ID);
                None
            }
            LaneResponse::SyncEvent(id, MapOperation::Update { key, value }) => {
                assert_eq!(id, SYNC_ID);
                Some((key, value))
            }
            ow => panic!("Unexpected record: {:?}", ow),
        }
    }
}

async fn lane_descriptor_test<F, Fut>(test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let (shutdown_tx, shutdown_rx) = trigger::trigger();
    let agg_reporter = UplinkReporter::default();
    let updater = AgentIntrospectionUpdater::new(agg_reporter.reader());

    let handle = updater.make_handle();

    let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
    let lane_task = run_lanes_descriptor_lane(shutdown_rx, handle, (out_tx, in_rx));

    let context = TestContext {
        shutdown_tx,
        updater,
        sender: LaneSender::new(SYNC_ID, in_tx),
        receiver: LaneReceiver::new(out_rx),
        agg_reporter,
    };
    let test_task = test_case(context);
    let (result, output) = join(lane_task, test_task).await;
    assert!(result.is_ok());
    output
}

#[tokio::test]
async fn sync_lane_descriptors() {
    let entries = [
        ("first", LaneKind::Value, UplinkReporter::default()),
        ("second", LaneKind::Map, UplinkReporter::default()),
        ("third", LaneKind::Command, UplinkReporter::default()),
    ];

    let result_map = lane_descriptor_test(|context| {
        let TestContext { updater, .. } = &context;

        for (name, kind, reporter) in &entries {
            updater.add_lane(Text::new(*name), *kind, reporter.reader());
        }
        async move {
            let TestContext {
                shutdown_tx,
                updater: _updater,
                mut sender,
                mut receiver,
                agg_reporter: _agg_reporter,
            } = context;

            sender.sync().await;

            let mut result_map = HashMap::new();

            while let Some((name, info)) = receiver.expect_sync_message().await {
                result_map.insert(name, info);
            }

            shutdown_tx.trigger();
            result_map
        }
    })
    .await;

    let expected: HashMap<_, _> = entries
        .iter()
        .map(|(name, kind, _)| (Text::new(*name), LaneInfo::new(Text::new(*name), *kind)))
        .collect();
    assert_eq!(result_map, expected);
}

#[tokio::test]
async fn resync_lane_descriptors() {
    let entries1 = [
        ("first", LaneKind::Value, UplinkReporter::default()),
        ("second", LaneKind::Map, UplinkReporter::default()),
    ];

    let entries2 = &[("third", LaneKind::Command, UplinkReporter::default())];

    let (result_map1, result_map2) = lane_descriptor_test(|context| {
        let TestContext { updater, .. } = &context;

        for (name, kind, reporter) in &entries1 {
            updater.add_lane(Text::new(*name), *kind, reporter.reader());
        }
        async move {
            let TestContext {
                shutdown_tx,
                updater,
                mut sender,
                mut receiver,
                agg_reporter: _agg_reporter,
            } = context;

            sender.sync().await;

            let mut result_map1 = HashMap::new();

            while let Some((name, info)) = receiver.expect_sync_message().await {
                result_map1.insert(name, info);
            }

            for (name, kind, reporter) in entries2 {
                updater.add_lane(Text::new(*name), *kind, reporter.reader());
            }

            sender.sync().await;

            let mut result_map2 = HashMap::new();

            while let Some((name, info)) = receiver.expect_sync_message().await {
                result_map2.insert(name, info);
            }

            shutdown_tx.trigger();
            (result_map1, result_map2)
        }
    })
    .await;

    let mut expected: HashMap<_, _> = entries1
        .iter()
        .map(|(name, kind, _)| (Text::new(*name), LaneInfo::new(Text::new(*name), *kind)))
        .collect();
    assert_eq!(result_map1, expected);

    expected.extend(
        entries2
            .iter()
            .map(|(name, kind, _)| (Text::new(*name), LaneInfo::new(Text::new(*name), *kind))),
    );

    assert_eq!(result_map2, expected);
}

#[tokio::test(start_paused = true)] //Auto-resume will ensure pulses trigger predictably.
async fn node_meta_agent_pulse_lane() {
    let expected_lane_config = LaneConfig {
        transient: true,
        ..Default::default()
    };
    let route = "swim:meta:node/%2Fnode".parse().expect("Invalid route.");

    let mut lanes = vec![];
    lanes.push((PULSE_LANE.to_string(), LaneKind::Supply));
    lanes.push((LANES_LANE.to_string(), LaneKind::DemandMap));

    introspection_agent_test(
        expected_lane_config,
        lanes,
        route,
        |resolver| NodeMetaAgent::new(IntrospectionConfig::default(), resolver),
        |context| async move {
            let IntrospectionTestContext {
                mut lanes,
                init_done,
                mut queries_rx,
                _reg_rx,
            } = context;

            let _updaters = provide_node(
                &mut queries_rx,
                "/node",
                vec![("value", LaneKind::Value), ("map", LaneKind::Map)],
                |reporter| {
                    reporter.set_uplinks(3);
                    reporter.count_events(67);
                    reporter.count_commands(777);
                },
            )
            .await;

            assert!(init_done.await.is_ok());

            let (_tx, rx) = lanes.get_mut(PULSE_LANE).expect("Lane not defined.");
            let mut receiver = PulseLaneReader::new(rx);
            // The pulse lane should clear the events and commands when it starts to create a clean baseline.
            receiver.expect_pulse(3, 0, 0).await;
            drop(receiver);
            drop(lanes);
        },
    )
    .await;
}

#[tokio::test(start_paused = true)] //Auto-resume will ensure pulses trigger predictably.
async fn node_meta_agent_laneinfo_lane() {
    let expected_lane_config = LaneConfig {
        transient: true,
        ..Default::default()
    };
    let route = "swim:meta:node/%2Fnode".parse().expect("Invalid route.");

    let mut lanes = vec![];
    lanes.push((PULSE_LANE.to_string(), LaneKind::Supply));
    lanes.push((LANES_LANE.to_string(), LaneKind::DemandMap));

    introspection_agent_test(
        expected_lane_config,
        lanes,
        route,
        |resolver| NodeMetaAgent::new(IntrospectionConfig::default(), resolver),
        |context| async move {
            let IntrospectionTestContext {
                mut lanes,
                init_done,
                mut queries_rx,
                _reg_rx,
            } = context;

            let _updaters = provide_node(
                &mut queries_rx,
                "/node",
                vec![("value", LaneKind::Value), ("map", LaneKind::Map)],
                |_| {},
            )
            .await;

            assert!(init_done.await.is_ok());

            let (tx, rx) = lanes.remove(LANES_LANE).expect("Lane not defined.");

            let lanes_meta = sync_lanes_meta(tx, rx).await;

            let mut expected = HashMap::new();
            expected.insert(
                Text::new("value"),
                LaneInfo::new(Text::new("value"), LaneKind::Value),
            );
            expected.insert(
                Text::new("map"),
                LaneInfo::new(Text::new("map"), LaneKind::Map),
            );

            assert_eq!(lanes_meta, expected);

            drop(lanes);
        },
    )
    .await;
}

async fn provide_node(
    queries_rx: &mut mpsc::UnboundedReceiver<IntrospectionMessage>,
    expected_node: &str,
    lanes: Vec<(&str, LaneKind)>,
    init: impl FnOnce(&UplinkReporter),
) -> (AgentIntrospectionUpdater, Vec<UplinkReporter>) {
    if let Some(IntrospectionMessage::IntrospectAgent {
        node_uri,
        responder,
    }) = queries_rx.recv().await
    {
        assert_eq!(node_uri, expected_node);
        let agg_reporter = UplinkReporter::default();
        init(&agg_reporter);
        let updater = AgentIntrospectionUpdater::new(agg_reporter.reader());
        let mut reporters = vec![agg_reporter];
        for (name, kind) in lanes {
            let reporter = UplinkReporter::default();
            updater.add_lane(Text::new(name), kind, reporter.reader());
            reporters.push(reporter);
        }
        assert!(responder.send(Some(updater.make_handle())).is_ok());
        (updater, reporters)
    } else {
        panic!("Did not receive introspection request.");
    }
}

type PulseDec =
    LaneResponseDecoder<WithLenRecognizerDecoder<<NodePulse as RecognizerReadable>::Rec>>;

struct PulseLaneReader<'a> {
    inner: FramedRead<&'a mut ByteReader, PulseDec>,
}

impl<'a> PulseLaneReader<'a> {
    fn new(reader: &'a mut ByteReader) -> Self {
        PulseLaneReader {
            inner: FramedRead::new(
                reader,
                LaneResponseDecoder::new(WithLenRecognizerDecoder::new(
                    NodePulse::make_recognizer(),
                )),
            ),
        }
    }

    async fn expect_pulse(&mut self, links: u64, events: u64, commands: u64) {
        let PulseLaneReader { inner } = self;
        match inner.next().await {
            Some(Ok(LaneResponse::StandardEvent(NodePulse { uplinks }))) => {
                assert_eq!(uplinks.link_count, links);
                assert_eq!(uplinks.event_count, events);
                assert_eq!(uplinks.command_count, commands);
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
}

async fn sync_lanes_meta(tx: ByteWriter, rx: ByteReader) -> HashMap<Text, LaneInfo> {
    let mut sender = LaneSender::new(SYNC_ID, tx);
    let mut receiver = LaneReceiver::new(rx);
    sender.sync().await;
    let mut result_map = HashMap::new();

    while let Some((name, info)) = receiver.expect_sync_message().await {
        result_map.insert(name, info);
    }
    result_map
}
