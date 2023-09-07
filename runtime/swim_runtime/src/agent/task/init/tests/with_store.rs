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

use std::collections::HashMap;

use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use swim_api::{
    agent::{LaneConfig, StoreConfig, UplinkKind},
    error::StoreError,
    lane::WarpLaneKind,
    protocol::{
        agent::{
            LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder, StoreInitMessage,
            StoreInitMessageDecoder, StoreInitialized, StoreInitializedCodec,
        },
        map::{
            MapMessage, MapMessageDecoder, MapOperation, MapOperationDecoder,
            RawMapOperationEncoder,
        },
        WithLenRecognizerDecoder, WithLengthBytesCodec,
    },
    store::{NodePersistence, StoreKind},
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::agent::{
    store::{StoreInitError, StorePersistence},
    task::{
        fake_store::FakeStore,
        init::tests::{INIT_STOPPED, NO_LANE, NO_RESPONSE, NO_STORE, TRANSIENT},
        AgentRuntimeRequest, Endpoints, InitialEndpoints, LaneEndpoint, LaneRuntimeSpec,
        StoreEndpoint, StoreRuntimeSpec,
    },
    AgentExecError, Io, LinkRequest,
};

use super::{check_connected, run_test, TestInit, PERSISTENT};

struct NoLanes;

impl TestInit for NoLanes {
    type Output = ();

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let _requests = requests;
            let _link_requests = link_requests;
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
const VAL_STORE: &str = "value_store";

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
    let mut framed_out = FramedWrite::new(output, LaneResponseEncoder::new(WithLengthBytesCodec));
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
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let SingleValueLane { config, expected } = self;
            let _link_requests = link_requests;
            let (promise_tx, promise_rx) = oneshot::channel();
            requests
                .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                    Text::new(VAL_LANE),
                    WarpLaneKind::Value,
                    config,
                    promise_tx,
                )))
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
    let mut store = FakeStore::new(vec![VAL_LANE]);
    let id = store.id_for(VAL_LANE).expect("Invalid key.");
    store.put_value(id, b"56").expect("Invalid ID");

    let persistence = StorePersistence(store);
    let init = SingleValueLane {
        config: PERSISTENT,
        expected: Expectation::Expect(56),
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints {
        endpoints: Endpoints {
            mut lane_endpoints, ..
        },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = lane_endpoints.pop().unwrap();
    assert_eq!(name, "value");
    assert_eq!(kind, UplinkKind::Value);
    check_connected(&mut agent_io, &mut io);
}

#[tokio::test]
async fn init_single_trasient_value_lane_with_store() {
    let mut store = FakeStore::new(vec![VAL_LANE]);
    let id = store.id_for(VAL_LANE).expect("Invalid key.");
    store.put_value(id, b"56").expect("Invalid ID");

    let persistence = StorePersistence(store);
    let init = SingleValueLane {
        config: TRANSIENT,
        expected: Expectation::Transient,
    };
    let (initial_result, mut agent_io) = run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints {
        endpoints: Endpoints {
            mut lane_endpoints, ..
        },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = lane_endpoints.pop().unwrap();
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
            item_name,
            error: StoreInitError::Store(_),
        }) => {
            assert_eq!(item_name, VAL_LANE);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

struct SingleMapLane {
    config: LaneConfig,
    expected: Expectation<HashMap<i32, i32>>,
}

const MAP_LANE: &str = "map";
const MAP_STORE: &str = "map_store";

impl TestInit for SingleMapLane {
    type Output = Io;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        async move {
            let SingleMapLane { config, expected } = self;
            let _link_requests = link_requests;
            let (promise_tx, promise_rx) = oneshot::channel();
            requests
                .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                    Text::new(MAP_LANE),
                    WarpLaneKind::Map,
                    config,
                    promise_tx,
                )))
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

    let InitialEndpoints {
        endpoints: Endpoints {
            mut lane_endpoints, ..
        },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = lane_endpoints.pop().unwrap();
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

    let InitialEndpoints {
        endpoints: Endpoints {
            mut lane_endpoints, ..
        },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    let LaneEndpoint {
        name, kind, mut io, ..
    } = lane_endpoints.pop().unwrap();
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
            item_name,
            error: StoreInitError::Store(_),
        }) => {
            assert_eq!(item_name, MAP_LANE);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

struct ValueStoreInit {
    lane_config: LaneConfig,
    store_config: StoreConfig,
    expected: i32,
}

impl TestInit for ValueStoreInit {
    type Output = (Io, Io);

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        ValueStoreInitTask::new(
            requests,
            link_requests,
            init_complete,
            self.store_config,
            self.lane_config,
            self.expected,
        )
        .run()
        .boxed()
    }
}

async fn with_store_init_value_store(
    input: &mut ByteReader,
    output: &mut ByteWriter,
    expected: i32,
) {
    let mut framed_in = FramedRead::new(
        input,
        StoreInitMessageDecoder::new(WithLenRecognizerDecoder::new(i32::make_recognizer())),
    );
    match framed_in.next().await {
        Some(Ok(StoreInitMessage::Command(n))) => {
            assert_eq!(n, expected);
        }
        ow => panic!("Unexpected event: {:?}", ow),
    }
    match framed_in.next().await {
        Some(Ok(StoreInitMessage::InitComplete)) => {}
        ow => panic!("Unexpected event: {:?}", ow),
    }
    let mut framed_out = FramedWrite::new(output, StoreInitializedCodec);
    framed_out
        .send(StoreInitialized)
        .await
        .expect("Failed to send initialized message.");
}

struct ValueStoreInitTask {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
    store_config: StoreConfig,
    lane_config: LaneConfig,
    expected: i32,
}

impl ValueStoreInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        _dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
        store_config: StoreConfig,
        lane_config: LaneConfig,
        expected: i32,
    ) -> Self {
        ValueStoreInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
            expected,
        }
    }

    async fn run(self) -> (Io, Io) {
        let ValueStoreInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
            expected,
        } = self;
        let (lane_tx, lane_rx) = oneshot::channel();
        let (store_tx, store_rx) = oneshot::channel();

        // At least one lane is required for initialization to succeed.
        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("lane_name"),
                WarpLaneKind::Command,
                lane_config,
                lane_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddStore(StoreRuntimeSpec::new(
                Text::new(VAL_STORE),
                StoreKind::Value,
                store_config,
                store_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        let lane_io = lane_rx.await.expect(NO_RESPONSE).expect(NO_LANE);

        let mut store_io = store_rx.await.expect(NO_RESPONSE).expect(NO_STORE);

        let (tx, rx) = &mut store_io;

        with_store_init_value_store(rx, tx, expected).await;

        init_complete.trigger();
        (lane_io, store_io)
    }
}

#[tokio::test]
async fn init_single_value_store_from_store() {
    let mut store = FakeStore::new(vec![VAL_STORE]);
    let id = store.id_for(VAL_STORE).expect("Invalid key.");
    store.put_value(id, b"689").expect("Invalid ID");

    let persistence = StorePersistence(store);
    let init = ValueStoreInit {
        lane_config: TRANSIENT,
        store_config: Default::default(),
        expected: 689,
    };
    let (initial_result, (_lane_io, (store_io_tx, _store_io_rx))) =
        run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints {
        endpoints:
            Endpoints {
                lane_endpoints,
                mut store_endpoints,
                ..
            },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    assert_eq!(store_endpoints.len(), 1);

    let StoreEndpoint { name, kind, reader } = store_endpoints.pop().unwrap();
    assert_eq!(name, VAL_STORE);
    assert_eq!(kind, StoreKind::Value);
    assert!(byte_channel::are_connected(&store_io_tx, &reader));
}

struct MapStoreInit {
    lane_config: LaneConfig,
    store_config: StoreConfig,
    expected: HashMap<i32, i32>,
}

impl TestInit for MapStoreInit {
    type Output = (Io, Io);

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        MapStoreInitTask::new(
            requests,
            link_requests,
            init_complete,
            self.store_config,
            self.lane_config,
            self.expected,
        )
        .run()
        .boxed()
    }
}

type StoreMapReqDecoder = StoreInitMessageDecoder<MapMessageDecoder<MapOperationDecoder<i32, i32>>>;

async fn with_store_init_map_store(
    input: &mut ByteReader,
    output: &mut ByteWriter,
    expected: HashMap<i32, i32>,
) {
    let mut framed_in = FramedRead::new(input, StoreMapReqDecoder::default());
    let mut actual = HashMap::new();
    loop {
        match framed_in.next().await {
            Some(Ok(StoreInitMessage::Command(MapMessage::Update { key, value }))) => {
                actual.insert(key, value);
            }
            Some(Ok(StoreInitMessage::InitComplete)) => break,
            ow => panic!("Unexpected event: {:?}", ow),
        }
    }
    assert_eq!(actual, expected);
    let mut framed_out = FramedWrite::new(output, StoreInitializedCodec);
    framed_out
        .send(StoreInitialized)
        .await
        .expect("Failed to send initialized message.");
}

struct MapStoreInitTask {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
    store_config: StoreConfig,
    lane_config: LaneConfig,
    expected: HashMap<i32, i32>,
}

impl MapStoreInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        _dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
        store_config: StoreConfig,
        lane_config: LaneConfig,
        expected: impl IntoIterator<Item = (i32, i32)>,
    ) -> Self {
        MapStoreInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
            expected: expected.into_iter().collect(),
        }
    }

    async fn run(self) -> (Io, Io) {
        let MapStoreInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
            expected,
        } = self;
        let (lane_tx, lane_rx) = oneshot::channel();
        let (store_tx, store_rx) = oneshot::channel();

        // At least one lane is required for initialization to succeed.
        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("lane_name"),
                WarpLaneKind::Command,
                lane_config,
                lane_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddStore(StoreRuntimeSpec::new(
                Text::new(MAP_STORE),
                StoreKind::Map,
                store_config,
                store_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        let lane_io = lane_rx.await.expect(NO_RESPONSE).expect(NO_LANE);

        let mut store_io = store_rx.await.expect(NO_RESPONSE).expect(NO_STORE);

        let (tx, rx) = &mut store_io;

        with_store_init_map_store(rx, tx, expected).await;

        init_complete.trigger();
        (lane_io, store_io)
    }
}

#[tokio::test]
async fn init_single_map_store_from_store() {
    let mut store = FakeStore::new(vec![MAP_STORE]);
    let id = store.id_for(MAP_STORE).expect("Invalid key.");
    let mut expected = HashMap::new();
    for i in 1..=3 {
        store
            .update_map(id, i.to_string().as_bytes(), (2 * i).to_string().as_bytes())
            .expect("Invalid ID.");
        expected.insert(i, 2 * i);
    }

    let persistence = StorePersistence(store);
    let init = MapStoreInit {
        lane_config: TRANSIENT,
        store_config: Default::default(),
        expected,
    };
    let (initial_result, (_lane_io, (store_io_tx, _store_io_rx))) =
        run_test(init, persistence).await;

    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints {
        endpoints:
            Endpoints {
                lane_endpoints,
                mut store_endpoints,
                ..
            },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    assert_eq!(store_endpoints.len(), 1);

    let StoreEndpoint { name, kind, reader } = store_endpoints.pop().unwrap();
    assert_eq!(name, MAP_STORE);
    assert_eq!(kind, StoreKind::Map);
    assert!(byte_channel::are_connected(&store_io_tx, &reader));
}
