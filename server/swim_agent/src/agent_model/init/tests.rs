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

use std::{collections::HashMap, num::NonZeroUsize, time::Duration};

use futures::{future::join, SinkExt, StreamExt};
use swim_api::{
    agent::UplinkKind,
    protocol::{
        agent::{LaneRequest, LaneRequestEncoder, LaneResponse, LaneResponseDecoder},
        map::{
            MapMessage, MapMessageDecoder, MapMessageEncoder, MapOperationEncoder,
            RawMapOperationDecoder,
        },
        WithLenReconEncoder, WithLengthBytesCodec,
    },
};
use swim_model::Text;
use swim_utilities::{algebra::non_zero_usize, io::byte_channel::byte_channel};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::lanes::{MapLane, ValueLane};

use super::{run_lane_initializer, InitializedLane, MapLaneInitializer, ValueLaneInitializer};

struct TestAgent {
    value_lane: ValueLane<i32>,
    map_lane: MapLane<Text, i32>,
}

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            value_lane: ValueLane::new(0, 0),
            map_lane: MapLane::new(1, Default::default()),
        }
    }
}

const VALUE_LANE: fn(&TestAgent) -> &ValueLane<i32> = |agent| &agent.value_lane;
const MAP_LANE: fn(&TestAgent) -> &MapLane<Text, i32> = |agent| &agent.map_lane;
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn init_value_lane() {
    let init = ValueLaneInitializer::new(VALUE_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_lane_initializer(
        "value_lane",
        UplinkKind::Value,
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            LaneRequestEncoder::new(WithLenReconEncoder::default()),
        );
        let mut reader = FramedRead::new(
            &mut out_rx,
            LaneResponseDecoder::new(WithLengthBytesCodec::default()),
        );

        writer
            .send(LaneRequest::Command(46))
            .await
            .expect("Sending value failed.");
        writer
            .send(LaneRequest::<i32>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(LaneResponse::Initialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedLane {
        name,
        kind,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(name, "value_lane");
    assert_eq!(kind, UplinkKind::Value);

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_lane.read(|n| *n), 46);
}

#[tokio::test]
async fn init_value_lane_no_data() {
    let init = ValueLaneInitializer::new(VALUE_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_lane_initializer(
        "value_lane",
        UplinkKind::Value,
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            LaneRequestEncoder::new(WithLenReconEncoder::default()),
        );
        let mut reader = FramedRead::new(
            &mut out_rx,
            LaneResponseDecoder::new(WithLengthBytesCodec::default()),
        );

        writer
            .send(LaneRequest::<i32>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(LaneResponse::Initialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedLane {
        name,
        kind,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(name, "value_lane");
    assert_eq!(kind, UplinkKind::Value);

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_lane.read(|n| *n), 0);
}

#[tokio::test]
async fn init_map_lane() {
    let init = MapLaneInitializer::new(MAP_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = MapMessageDecoder::new(RawMapOperationDecoder::default());
    let init_task = run_lane_initializer(
        "map_lane",
        UplinkKind::Map,
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            LaneRequestEncoder::new(MapMessageEncoder::new(MapOperationEncoder::default())),
        );
        let mut reader = FramedRead::new(
            &mut out_rx,
            LaneResponseDecoder::new(RawMapOperationDecoder::default()),
        );

        writer
            .send(LaneRequest::Command(MapMessage::Update {
                key: Text::new("a"),
                value: 1,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(LaneRequest::Command(MapMessage::Update {
                key: Text::new("b"),
                value: 2,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(LaneRequest::Command(MapMessage::Update {
                key: Text::new("c"),
                value: 3,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(LaneRequest::<MapMessage<Text, i32>>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(LaneResponse::Initialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedLane {
        name,
        kind,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(name, "map_lane");
    assert_eq!(kind, UplinkKind::Map);

    let agent = TestAgent::default();

    init_fn(&agent);

    let lane_map = agent.map_lane.get_map(Clone::clone);
    let mut expected = HashMap::new();
    expected.insert(Text::new("a"), 1);
    expected.insert(Text::new("b"), 2);
    expected.insert(Text::new("c"), 3);
    assert_eq!(lane_map, expected);
}

#[tokio::test]
async fn init_map_lane_no_data() {
    let init = MapLaneInitializer::new(MAP_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = MapMessageDecoder::new(RawMapOperationDecoder::default());
    let init_task = run_lane_initializer(
        "map_lane",
        UplinkKind::Map,
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            LaneRequestEncoder::new(MapMessageEncoder::new(MapOperationEncoder::default())),
        );
        let mut reader = FramedRead::new(
            &mut out_rx,
            LaneResponseDecoder::new(RawMapOperationDecoder::default()),
        );

        writer
            .send(LaneRequest::<MapMessage<Text, i32>>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(LaneResponse::Initialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedLane {
        name,
        kind,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(name, "map_lane");
    assert_eq!(kind, UplinkKind::Map);

    let agent = TestAgent::default();

    init_fn(&agent);

    let lane_map = agent.map_lane.get_map(Clone::clone);
    assert!(lane_map.is_empty());
}
