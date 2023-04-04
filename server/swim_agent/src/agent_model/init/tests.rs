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

use std::{collections::HashMap, num::NonZeroUsize, time::Duration};

use futures::{future::join, SinkExt, StreamExt};
use swim_api::protocol::{
    agent::{
        LaneRequest, LaneRequestEncoder, LaneResponse, LaneResponseDecoder, StoreInitMessage,
        StoreInitMessageEncoder, StoreInitialized, StoreInitializedCodec,
    },
    map::{
        MapMessage, MapMessageDecoder, MapMessageEncoder, MapOperationEncoder,
        RawMapOperationDecoder,
    },
    WithLenReconEncoder, WithLengthBytesCodec,
};
use swim_model::Text;
use swim_utilities::{io::byte_channel::byte_channel, non_zero_usize};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    agent_model::{ItemKind, MapStoreInitializer, ValueStoreInitializer},
    lanes::{MapLane, ValueLane},
    stores::{MapStore, ValueStore},
};

use super::{run_item_initializer, InitializedItem, MapLaneInitializer, ValueLaneInitializer};

struct TestAgent {
    value_lane: ValueLane<i32>,
    map_lane: MapLane<Text, i32>,
    value_store: ValueStore<i32>,
    map_store: MapStore<Text, i32>,
}

impl Default for TestAgent {
    fn default() -> Self {
        Self {
            value_lane: ValueLane::new(0, 0),
            map_lane: MapLane::new(1, Default::default()),
            value_store: ValueStore::new(2, 0),
            map_store: MapStore::new(3, Default::default()),
        }
    }
}

const VALUE_LANE: fn(&TestAgent) -> &ValueLane<i32> = |agent| &agent.value_lane;
const MAP_LANE: fn(&TestAgent) -> &MapLane<Text, i32> = |agent| &agent.map_lane;
const VALUE_STORE: fn(&TestAgent) -> &ValueStore<i32> = |agent| &agent.value_store;
const MAP_STORE: fn(&TestAgent) -> &MapStore<Text, i32> = |agent| &agent.map_store;
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn init_value_lane() {
    let init = ValueLaneInitializer::new(VALUE_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_item_initializer(
        ItemKind::VALUE_LANE,
        "value_lane",
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

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::VALUE_LANE);
    assert_eq!(name, "value_lane");

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_lane.read(|n| *n), 46);
}

#[tokio::test]
async fn init_value_store() {
    let init = ValueStoreInitializer::new(VALUE_STORE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_item_initializer(
        ItemKind::VALUE_STORE,
        "value_store",
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            StoreInitMessageEncoder::new(WithLenReconEncoder::default()),
        );
        let mut reader = FramedRead::new(&mut out_rx, StoreInitializedCodec::default());

        writer
            .send(StoreInitMessage::Command(46))
            .await
            .expect("Sending value failed.");
        writer
            .send(StoreInitMessage::<i32>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(StoreInitialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::VALUE_STORE);
    assert_eq!(name, "value_store");

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_store.read(|n| *n), 46);
}

#[tokio::test]
async fn init_value_lane_no_data() {
    let init = ValueLaneInitializer::new(VALUE_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_item_initializer(
        ItemKind::VALUE_LANE,
        "value_lane",
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

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::VALUE_LANE);
    assert_eq!(name, "value_lane");

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_lane.read(|n| *n), 0);
}

#[tokio::test]
async fn init_value_store_no_data() {
    let init = ValueStoreInitializer::new(VALUE_STORE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = WithLengthBytesCodec::default();
    let init_task = run_item_initializer(
        ItemKind::VALUE_STORE,
        "value_store",
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            StoreInitMessageEncoder::new(WithLenReconEncoder::default()),
        );
        let mut reader = FramedRead::new(&mut out_rx, StoreInitializedCodec::default());

        writer
            .send(StoreInitMessage::<i32>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(StoreInitialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::VALUE_STORE);
    assert_eq!(name, "value_store");

    let agent = TestAgent::default();

    init_fn(&agent);

    assert_eq!(agent.value_store.read(|n| *n), 0);
}

#[tokio::test]
async fn init_map_lane() {
    let init = MapLaneInitializer::new(MAP_LANE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = MapMessageDecoder::new(RawMapOperationDecoder::default());
    let init_task = run_item_initializer(
        ItemKind::MAP_LANE,
        "map_lane",
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

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::MAP_LANE);
    assert_eq!(name, "map_lane");

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
async fn init_map_store() {
    let init = MapStoreInitializer::new(MAP_STORE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = MapMessageDecoder::new(RawMapOperationDecoder::default());
    let init_task = run_item_initializer(
        ItemKind::MAP_STORE,
        "map_store",
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            StoreInitMessageEncoder::new(MapMessageEncoder::new(MapOperationEncoder::default())),
        );
        let mut reader = FramedRead::new(&mut out_rx, StoreInitializedCodec::default());

        writer
            .send(StoreInitMessage::Command(MapMessage::Update {
                key: Text::new("a"),
                value: 1,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(StoreInitMessage::Command(MapMessage::Update {
                key: Text::new("b"),
                value: 2,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(StoreInitMessage::Command(MapMessage::Update {
                key: Text::new("c"),
                value: 3,
            }))
            .await
            .expect("Sending value failed.");
        writer
            .send(StoreInitMessage::<MapMessage<Text, i32>>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(StoreInitialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::MAP_STORE);
    assert_eq!(name, "map_store");

    let agent = TestAgent::default();

    init_fn(&agent);

    let lane_map = agent.map_store.get_map(Clone::clone);
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
    let init_task = run_item_initializer(
        ItemKind::MAP_LANE,
        "map_lane",
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

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::MAP_LANE);
    assert_eq!(name, "map_lane");

    let agent = TestAgent::default();

    init_fn(&agent);

    let lane_map = agent.map_lane.get_map(Clone::clone);
    assert!(lane_map.is_empty());
}

#[tokio::test]
async fn init_map_store_no_data() {
    let init = MapStoreInitializer::new(MAP_STORE);
    let (mut in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, mut out_rx) = byte_channel(BUFFER_SIZE);
    let decoder = MapMessageDecoder::new(RawMapOperationDecoder::default());
    let init_task = run_item_initializer(
        ItemKind::MAP_STORE,
        "map_store",
        (out_tx, in_rx),
        decoder,
        Box::new(init),
    );

    let runtime_task = async move {
        let mut writer = FramedWrite::new(
            &mut in_tx,
            StoreInitMessageEncoder::new(MapMessageEncoder::new(MapOperationEncoder::default())),
        );
        let mut reader = FramedRead::new(&mut out_rx, StoreInitializedCodec::default());

        writer
            .send(StoreInitMessage::<MapMessage<Text, i32>>::InitComplete)
            .await
            .expect("Completing init failed.");

        match reader.next().await {
            Some(Ok(StoreInitialized)) => {}
            ow => panic!("Unexpected response: {:?}", ow),
        }
        (in_tx, out_rx)
    };

    let (result, _io) = tokio::time::timeout(TEST_TIMEOUT, join(init_task, runtime_task))
        .await
        .expect("Test timed out.");

    let InitializedItem {
        item_kind,
        name,
        init_fn,
        io: _io,
    } = result.expect("Initialization failed.");

    assert_eq!(item_kind, ItemKind::MAP_STORE);
    assert_eq!(name, "map_store");

    let agent = TestAgent::default();

    init_fn(&agent);

    let lane_map = agent.map_lane.get_map(Clone::clone);
    assert!(lane_map.is_empty());
}
