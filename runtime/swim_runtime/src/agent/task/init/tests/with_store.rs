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

use std::collections::HashMap;

use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use swim_api::{
    agent::{LaneConfig, UplinkKind},
    error::StoreError,
    meta::lane::LaneKind,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        map::{
            MapMessage, MapMessageDecoder, MapOperation, MapOperationDecoder,
            RawMapOperationEncoder,
        },
        WithLenRecognizerDecoder, WithLengthBytesCodec,
    },
    store::NodePersistenceBase,
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::agent::{
    store::{StoreInitError, StorePersistence},
    task::{
        fake_store::FakeStore, init::tests::TRANSIENT, AgentRuntimeRequest, InitialEndpoints,
        LaneEndpoint,
    },
    AgentExecError, DownlinkRequest, Io,
};

use super::{check_connected, run_test, TestInit, PERSISTENT};

struct NoLanes;

impl TestInit for NoLanes {
    type Output = ();

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let _requests = requests;
            let _downlink_requests = downlink_requests;
            init_complete.trigger();
        }
        .boxed()
    }
}

#[tokio::test]
async fn no_lanes_with_store() {
    let store = StorePersistence(FakeStore::default());
    let (result, _) = run_test(NoLanes, store).await;
    assert!(matches!(result, Err(AgentExecError::NoInitialLanes)));
}

enum Expectation<T> {
    Expect(T),
    Fail,
    Transient,
}

struct SingleValueLane {
    config: LaneConfig,
    expected: Expectation<i32>,
}

const VAL_LANE: &str = "value";

async fn with_store_init_value(input: &mut ByteReader, output: &mut ByteWriter, expected: i32) {
    let mut framed_in = FramedRead::new(
        input,
        LaneRequestDecoder::new(WithLenRecognizerDecoder::new(i32::make_recognizer())),
    );
    match framed_in.next().await {
        Some(Ok(LaneRequest::Command(n))) => {
            assert_eq!(n, expected);
        }
        ow => panic!("Unexpected event: {:?}", ow),
    }
    match framed_in.next().await {
        Some(Ok(LaneRequest::InitComplete)) => {}
        ow => panic!("Unexpected event: {:?}", ow),
    }
    let mut framed_out = FramedWrite::new(
        output,
        LaneResponseEncoder::new(WithLengthBytesCodec::default()),
    );
    framed_out
        .send(LaneResponse::<&[u8]>::Initialized)
        .await
        .expect("Failed to send initialized message.");
}

impl TestInit for SingleValueLane {
    type Output = Io;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let SingleValueLane { config, expected } = self;
            let _downlink_requests = downlink_requests;
            let (promise_tx, promise_rx) = oneshot::channel();
            requests
                .send(AgentRuntimeRequest::AddLane {
                    name: Text::new(VAL_LANE),
                    kind: LaneKind::Value,
                    config,
                    promise: promise_tx,
                })
                .await
                .expect("Requesting new lane failed.");
            let mut lane_io = promise_rx
                .await
                .expect("Request dropped.")
                .expect("Opening new lane failed.");

            if !config.transient {
                if let Expectation::Expect(expected) = expected {
                    let (output, input) = &mut lane_io;
                    with_store_init_value(input, output, expected).await;
                }
            }
            init_complete.trigger();
            lane_io
        }
        .boxed()
    }
}

#[tokio::test]
async fn init_single_value_lane_from_store() {
    let store = FakeStore::new(vec![VAL_LANE]);
    let id = store.id_for(VAL_LANE).expect("Invalid key.");
    store.put_value(id, b"56").expect("Invalid ID");

    let persistence = StorePersistence(store);
    let init = SingleValueLane {
        config: PERSISTENT,
        expected: Expectation::Expect(56),
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { mut endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = endpoints.pop().unwrap();
    assert_eq!(name, "value");
    assert_eq!(kind, UplinkKind::Value);
    check_connected(&mut agent_io, &mut io);
}

#[tokio::test]
async fn init_single_trasient_value_lane_with_store() {
    let store = FakeStore::new(vec![VAL_LANE]);
    let id = store.id_for(VAL_LANE).expect("Invalid key.");
    store.put_value(id, b"56").expect("Invalid ID");

    let persistence = StorePersistence(store);
    let init = SingleValueLane {
        config: TRANSIENT,
        expected: Expectation::Transient,
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { mut endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = endpoints.pop().unwrap();
    assert_eq!(name, "value");
    assert_eq!(kind, UplinkKind::Value);
    check_connected(&mut agent_io, &mut io);
}

#[tokio::test]
async fn failed_value_lane_init_from_store() {
    let store = FakeStore::default();

    let persistence = StorePersistence(store);
    let init = SingleValueLane {
        config: PERSISTENT,
        expected: Expectation::Fail,
    };
    let (initial_result, _agent_io) = run_test(init, persistence).await;

    match initial_result {
        Err(AgentExecError::FailedRestoration {
            lane_name,
            error: StoreInitError::Store(_),
        }) => {
            assert_eq!(lane_name, VAL_LANE);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

struct SingleMapLane {
    config: LaneConfig,
    expected: Expectation<HashMap<i32, i32>>,
}

const MAP_LANE: &str = "map";

impl TestInit for SingleMapLane {
    type Output = Io;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let SingleMapLane { config, expected } = self;
            let _downlink_requests = downlink_requests;
            let (promise_tx, promise_rx) = oneshot::channel();
            requests
                .send(AgentRuntimeRequest::AddLane {
                    name: Text::new(MAP_LANE),
                    kind: LaneKind::Map,
                    config,
                    promise: promise_tx,
                })
                .await
                .expect("Requesting new lane failed.");
            let mut lane_io = promise_rx
                .await
                .expect("Request dropped.")
                .expect("Opening new lane failed.");

            if !config.transient {
                if let Expectation::Expect(expected) = expected {
                    let (output, input) = &mut lane_io;
                    with_store_init_map(input, output, expected).await;
                }
            }
            init_complete.trigger();
            lane_io
        }
        .boxed()
    }
}

type MapReqDecoder = LaneRequestDecoder<MapMessageDecoder<MapOperationDecoder<i32, i32>>>;
type MapRespEncoder = LaneResponseEncoder<RawMapOperationEncoder>;

async fn with_store_init_map(
    input: &mut ByteReader,
    output: &mut ByteWriter,
    expected: HashMap<i32, i32>,
) {
    let mut framed_in = FramedRead::new(input, MapReqDecoder::default());
    let mut init_map = HashMap::new();
    loop {
        match framed_in.next().await {
            Some(Ok(LaneRequest::Command(MapMessage::Update { key, value }))) => {
                init_map.insert(key, value);
            }
            Some(Ok(LaneRequest::InitComplete)) => break,
            ow => panic!("Unexpected event: {:?}", ow),
        }
    }
    assert_eq!(init_map, expected);
    let mut framed_out = FramedWrite::new(output, MapRespEncoder::default());
    framed_out
        .send(LaneResponse::<MapOperation<&[u8], &[u8]>>::Initialized)
        .await
        .expect("Failed to send initialized message.");
}

#[tokio::test]
async fn init_single_map_lane_from_store() {
    let store = FakeStore::new(vec![MAP_LANE]);
    let id = store.id_for(MAP_LANE).expect("Invalid key.");

    let mut data = HashMap::new();
    data.insert(1, 2);
    data.insert(2, 4);

    store.put_map(id, data.clone(), None);

    let persistence = StorePersistence(store);
    let init = SingleMapLane {
        config: PERSISTENT,
        expected: Expectation::Expect(data),
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { mut endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = endpoints.pop().unwrap();
    assert_eq!(name, "map");
    assert_eq!(kind, UplinkKind::Map);
    check_connected(&mut agent_io, &mut io);
}

#[tokio::test]
async fn init_single_transient_map_lane_with_store() {
    let store = FakeStore::new(vec![MAP_LANE]);
    let id = store.id_for(MAP_LANE).expect("Invalid key.");

    let mut data = HashMap::new();
    data.insert(1, 2);
    data.insert(2, 4);

    store.put_map(id, data.clone(), None);

    let persistence = StorePersistence(store);
    let init = SingleMapLane {
        config: TRANSIENT,
        expected: Expectation::Transient,
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { mut endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = endpoints.pop().unwrap();
    assert_eq!(name, "map");
    assert_eq!(kind, UplinkKind::Map);
    check_connected(&mut agent_io, &mut io);
}

#[tokio::test]
async fn failed_map_lane_init_from_store() {
    let store = FakeStore::new(vec![MAP_LANE]);
    let id = store.id_for(MAP_LANE).expect("Invalid key.");

    let mut data = HashMap::new();
    data.insert(1, 2);
    data.insert(2, 4);

    store.put_map(
        id,
        data.clone(),
        Some(StoreError::DelegateMessage("Boom!".to_owned())),
    );

    let persistence = StorePersistence(store);
    let init = SingleMapLane {
        config: PERSISTENT,
        expected: Expectation::Fail,
    };
    let (initial_result, _agent_io) = run_test(init, persistence).await;

    match initial_result {
        Err(AgentExecError::FailedRestoration {
            lane_name,
            error: StoreInitError::Store(_),
        }) => {
            assert_eq!(lane_name, MAP_LANE);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}
