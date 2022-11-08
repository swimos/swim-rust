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
    meta::lane::{LaneInfo, LaneKind},
    protocol::{
        agent::{LaneResponse, LaneResponseDecoder},
        map::{MapOperation, MapOperationDecoder},
    },
};
use swim_model::Text;
use swim_runtime::agent::reporting::UplinkReporter;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader},
    trigger,
};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::model::AgentIntrospectionUpdater;

use super::run_lanes_descriptor_lane;
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
