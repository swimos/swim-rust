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

use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use swim_api::{
    agent::{LaneConfig, StoreConfig, UplinkKind},
    error::OpenStoreError,
    meta::lane::LaneKind,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        map::{MapMessageDecoder, RawMapOperation, RawMapOperationDecoder, RawMapOperationEncoder},
        WithLengthBytesCodec,
    },
    store::{StoreDisabled, StoreKind},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{check_connected, run_test, TestInit, CONFIGS, INIT_STOPPED, NO_LANE, NO_RESPONSE};
use crate::agent::{
    task::{
        init::tests::{run_test_with_reporting, AGENT_ID, TRANSIENT},
        AgentRuntimeRequest, Endpoints, InitialEndpoints, LaneEndpoint, LaneRuntimeSpec,
        StoreRuntimeSpec,
    },
    AgentExecError, Io, LinkRequest,
};

struct NoLanesInit;

struct NoLanesInitTask {
    _requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
}

impl NoLanesInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> Self {
        NoLanesInitTask {
            _requests: requests,
            _dl_requests: dl_requests,
            init_complete,
        }
    }

    async fn run(self) {
        let NoLanesInitTask { init_complete, .. } = self;
        init_complete.trigger();
    }
}

impl TestInit for NoLanesInit {
    type Output = ();

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        let task = NoLanesInitTask::new(requests, link_requests, init_complete);
        task.run().boxed()
    }
}

struct SingleLaneInit {
    config: LaneConfig,
}

async fn no_store_init_value(input: &mut ByteReader, output: &mut ByteWriter) {
    let mut framed_in = FramedRead::new(input, LaneRequestDecoder::new(WithLengthBytesCodec));
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

async fn no_store_init_map(input: &mut ByteReader, output: &mut ByteWriter) {
    let mut framed_in = FramedRead::new(
        input,
        LaneRequestDecoder::new(MapMessageDecoder::new(RawMapOperationDecoder)),
    );
    match framed_in.next().await {
        Some(Ok(LaneRequest::InitComplete)) => {}
        ow => panic!("Unexpected event: {:?}", ow),
    }
    let mut framed_out = FramedWrite::new(output, LaneResponseEncoder::new(RawMapOperationEncoder));
    framed_out
        .send(LaneResponse::<RawMapOperation>::Initialized)
        .await
        .expect("Failed to send initialized message.");
}

struct SingleLaneInitTask {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
    config: LaneConfig,
}

impl SingleLaneInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        SingleLaneInitTask {
            requests,
            _dl_requests: dl_requests,
            init_complete,
            config,
        }
    }

    async fn run(self) -> Io {
        let SingleLaneInitTask {
            requests,
            _dl_requests,
            init_complete,
            config,
        } = self;
        let (lane_tx, lane_rx) = oneshot::channel();
        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("my_lane"),
                LaneKind::Value,
                config,
                lane_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        let mut lane_io = lane_rx.await.expect(NO_RESPONSE).expect(NO_LANE);

        if !config.transient {
            let (output, input) = &mut lane_io;
            no_store_init_value(input, output).await;
        }

        init_complete.trigger();
        lane_io
    }
}

impl TestInit for SingleLaneInit {
    type Output = Io;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        let task = SingleLaneInitTask::new(requests, link_requests, init_complete, self.config);
        task.run().boxed()
    }
}

#[tokio::test]
async fn no_lanes() {
    let (result, _) = run_test(NoLanesInit, StoreDisabled).await;
    assert!(matches!(result, Err(AgentExecError::NoInitialLanes)));
}

#[tokio::test]
async fn single_lane() {
    for config in CONFIGS {
        let init = SingleLaneInit { config: *config };
        let (initial_result, mut agent_io) = run_test(init, StoreDisabled).await;
        let initial = initial_result.expect("No lanes were registered.");

        let InitialEndpoints {
            endpoints:
                Endpoints {
                    mut lane_endpoints,
                    store_endpoints,
                    ..
                },
            ..
        } = initial;

        assert!(store_endpoints.is_empty());

        assert_eq!(lane_endpoints.len(), 1);
        let LaneEndpoint {
            name,
            kind,
            mut io,
            transient,
            reporter,
        } = lane_endpoints.pop().unwrap();
        assert_eq!(name, "my_lane");
        assert_eq!(kind, UplinkKind::Value);
        assert_eq!(transient, config.transient);
        assert!(reporter.is_none());
        check_connected(&mut agent_io, &mut io);
    }
}

#[tokio::test]
async fn single_lane_with_reporting() {
    for config in CONFIGS {
        let init = SingleLaneInit { config: *config };
        let (initial_result, mut agent_io) = run_test_with_reporting(
            init,
            vec![(AGENT_ID, Text::new("my_lane"), LaneKind::Value)],
        )
        .await;
        let initial = initial_result.expect("No lanes were registered.");

        let InitialEndpoints {
            endpoints:
                Endpoints {
                    mut lane_endpoints,
                    store_endpoints,
                    ..
                },
            ..
        } = initial;

        assert!(store_endpoints.is_empty());

        assert_eq!(lane_endpoints.len(), 1);
        let LaneEndpoint {
            name,
            kind,
            mut io,
            transient,
            reporter,
        } = lane_endpoints.pop().unwrap();
        assert_eq!(name, "my_lane");
        assert_eq!(kind, UplinkKind::Value);
        assert_eq!(transient, config.transient);
        assert!(reporter.is_some());
        check_connected(&mut agent_io, &mut io);
    }
}

struct TwoLanesInit {
    config: LaneConfig,
}

struct TwoLanesInitTask {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
    config: LaneConfig,
}

#[derive(Debug)]
struct TwoLanes {
    value: Io,
    map: Io,
}

impl TwoLanesInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        TwoLanesInitTask {
            requests,
            _dl_requests: dl_requests,
            init_complete,
            config,
        }
    }

    async fn run(self) -> TwoLanes {
        let TwoLanesInitTask {
            requests,
            _dl_requests,
            init_complete,
            config,
        } = self;
        let (lane_tx1, lane_rx1) = oneshot::channel();
        let (lane_tx2, lane_rx2) = oneshot::channel();
        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("value_lane"),
                LaneKind::Value,
                config,
                lane_tx1,
            )))
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("map_lane"),
                LaneKind::Map,
                config,
                lane_tx2,
            )))
            .await
            .expect(INIT_STOPPED);

        let mut lane_io1 = lane_rx1.await.expect(NO_RESPONSE).expect(NO_LANE);

        let mut lane_io2 = lane_rx2.await.expect(NO_RESPONSE).expect(NO_LANE);

        if !config.transient {
            let (output1, input1) = &mut lane_io1;
            no_store_init_value(input1, output1).await;
            let (output2, input2) = &mut lane_io2;
            no_store_init_map(input2, output2).await;
        }
        init_complete.trigger();
        TwoLanes {
            value: lane_io1,
            map: lane_io2,
        }
    }
}

impl TestInit for TwoLanesInit {
    type Output = TwoLanes;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        let task = TwoLanesInitTask::new(requests, dl_requests, init_complete, self.config);
        task.run().boxed()
    }
}

#[tokio::test]
async fn two_lanes() {
    for config in CONFIGS {
        let init = TwoLanesInit { config: *config };
        let (initial_result, agent_lanes) = run_test(init, StoreDisabled).await;
        let initial = initial_result.expect("No lanes were registered.");

        let InitialEndpoints {
            endpoints:
                Endpoints {
                    lane_endpoints,
                    store_endpoints,
                    ..
                },
            ..
        } = initial;

        assert!(store_endpoints.is_empty());
        assert_eq!(lane_endpoints.len(), 2);

        let mut seen_value = false;
        let mut seen_map = false;

        let TwoLanes { mut value, mut map } = agent_lanes;

        for LaneEndpoint {
            name,
            kind,
            mut io,
            transient,
            reporter,
        } in lane_endpoints
        {
            match kind {
                UplinkKind::Value => {
                    assert!(!seen_value);
                    seen_value = true;
                    assert_eq!(name, "value_lane");
                    check_connected(&mut value, &mut io);
                }
                UplinkKind::Map => {
                    assert!(!seen_map);
                    seen_map = true;
                    assert_eq!(name, "map_lane");
                    check_connected(&mut map, &mut io);
                }
                _ => panic!("Unexpected supply uplink."),
            }
            assert_eq!(transient, config.transient);
            assert!(reporter.is_none());
        }
    }
}

struct StoresInit {
    lane_config: LaneConfig,
    store_config: StoreConfig,
}

impl TestInit for StoresInit {
    type Output = Io;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output> {
        StoresInitTask::new(
            requests,
            link_requests,
            init_complete,
            self.store_config,
            self.lane_config,
        )
        .run()
        .boxed()
    }
}

struct StoresInitTask {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<LinkRequest>,
    init_complete: trigger::Sender,
    store_config: StoreConfig,
    lane_config: LaneConfig,
}

impl StoresInitTask {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        _dl_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
        store_config: StoreConfig,
        lane_config: LaneConfig,
    ) -> Self {
        StoresInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
        }
    }

    async fn run(self) -> Io {
        let StoresInitTask {
            requests,
            _dl_requests,
            init_complete,
            store_config,
            lane_config,
        } = self;
        let (lane_tx, lane_rx) = oneshot::channel();
        let (store_tx1, store_rx1) = oneshot::channel();
        let (store_tx2, store_rx2) = oneshot::channel();

        // At least one lane is required for initialization to succeed.
        requests
            .send(AgentRuntimeRequest::AddLane(LaneRuntimeSpec::new(
                Text::new("lane_name"),
                LaneKind::Command,
                lane_config,
                lane_tx,
            )))
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddStore(StoreRuntimeSpec::new(
                Text::new("value_store_name"),
                StoreKind::Value,
                store_config,
                store_tx1,
            )))
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddStore(StoreRuntimeSpec::new(
                Text::new("map_store_name"),
                StoreKind::Map,
                store_config,
                store_tx2,
            )))
            .await
            .expect(INIT_STOPPED);

        let lane_io = lane_rx.await.expect(NO_RESPONSE).expect(NO_LANE);

        let value_store_result = store_rx1.await.expect(NO_RESPONSE);

        assert_eq!(
            value_store_result.err(),
            Some(OpenStoreError::StoresNotSupported)
        );

        let map_store_result = store_rx2.await.expect(NO_RESPONSE);
        assert_eq!(
            map_store_result.err(),
            Some(OpenStoreError::StoresNotSupported)
        );

        init_complete.trigger();
        lane_io
    }
}

#[tokio::test]
async fn stores_not_supported() {
    let init = StoresInit {
        lane_config: TRANSIENT,
        store_config: Default::default(),
    };
    let (initial_result, _lane_io) = run_test(init, StoreDisabled).await;
    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints {
        endpoints:
            Endpoints {
                lane_endpoints,
                store_endpoints,
                ..
            },
        ..
    } = initial;

    assert_eq!(lane_endpoints.len(), 1);
    assert!(store_endpoints.is_empty());
}
