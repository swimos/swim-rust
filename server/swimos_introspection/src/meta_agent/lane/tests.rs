// Copyright 2015-2023 Swim Inc.
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

use crate::{
    config::IntrospectionConfig,
    meta_agent::{
        test_harness::{introspection_agent_test, IntrospectionTestContext},
        PULSE_LANE,
    },
    model::LaneView,
    route::{LANE_PARAM, NODE_PARAM},
    task::IntrospectionMessage,
};
use futures::StreamExt;
use swimos_api::{
    agent::LaneConfig,
    lane::WarpLaneKind,
    meta::{lane::LaneKind, uplink::LanePulse},
    protocol::{
        agent::{LaneResponse, LaneResponseDecoder},
        WithLenRecognizerDecoder,
    },
};
use swimos_form::structural::read::recognizer::RecognizerReadable;
use swimos_runtime::agent::reporting::UplinkReporter;
use swimos_utilities::io::byte_channel::ByteReader;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;

use super::LaneMetaAgent;

#[tokio::test(start_paused = true)] //Auto-resume will ensure pulses trigger predictably.
async fn run_lane_meta_agent() {
    let expected_lane_config = LaneConfig {
        transient: true,
        ..Default::default()
    };
    let route = "swimos:meta:node/%2Fnode/lane/my_lane"
        .parse()
        .expect("Invalid route.");
    let route_params = [
        (NODE_PARAM.to_string(), "/node".to_string()),
        (LANE_PARAM.to_string(), "my_lane".to_string()),
    ]
    .into_iter()
    .collect();

    let lanes = vec![(PULSE_LANE.to_string(), WarpLaneKind::Supply)];
    introspection_agent_test(
        expected_lane_config,
        lanes,
        route,
        route_params,
        |resolver| LaneMetaAgent::new(IntrospectionConfig::default(), resolver),
        |context| async move {
            let IntrospectionTestContext {
                mut lanes,
                init_done,
                mut queries_rx,
                _reg_rx,
            } = context;
            let _reporter = provide_lane(
                &mut queries_rx,
                "/node",
                "my_lane",
                LaneKind::Value,
                |reporter| {
                    reporter.set_uplinks(3);
                    reporter.count_events(4);
                    reporter.count_commands(5);
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

async fn provide_lane(
    queries_rx: &mut mpsc::UnboundedReceiver<IntrospectionMessage>,
    expected_node: &str,
    expected_lane: &str,
    kind: LaneKind,
    init: impl FnOnce(&UplinkReporter),
) -> UplinkReporter {
    if let Some(IntrospectionMessage::IntrospectLane {
        node_uri,
        lane_name,
        responder,
    }) = queries_rx.recv().await
    {
        assert_eq!(node_uri, expected_node);
        assert_eq!(lane_name, expected_lane);
        let reporter = UplinkReporter::default();
        init(&reporter);
        let view = LaneView::new(kind, reporter.reader());
        assert!(responder.send(Ok(view)).is_ok());
        reporter
    } else {
        panic!("Did not receive introspection request.");
    }
}

type PulseDec =
    LaneResponseDecoder<WithLenRecognizerDecoder<<LanePulse as RecognizerReadable>::Rec>>;

struct PulseLaneReader<'a> {
    inner: FramedRead<&'a mut ByteReader, PulseDec>,
}

impl<'a> PulseLaneReader<'a> {
    fn new(reader: &'a mut ByteReader) -> Self {
        PulseLaneReader {
            inner: FramedRead::new(
                reader,
                LaneResponseDecoder::new(WithLenRecognizerDecoder::new(
                    LanePulse::make_recognizer(),
                )),
            ),
        }
    }

    async fn expect_pulse(&mut self, links: u64, events: u64, commands: u64) {
        let PulseLaneReader { inner } = self;
        match inner.next().await {
            Some(Ok(LaneResponse::StandardEvent(LanePulse { uplink_pulse }))) => {
                assert_eq!(uplink_pulse.link_count, links);
                assert_eq!(uplink_pulse.event_count, events);
                assert_eq!(uplink_pulse.command_count, commands);
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
}
