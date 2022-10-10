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

use std::{num::NonZeroUsize, time::Duration};

use futures::{
    future::{join, BoxFuture},
    FutureExt, SinkExt, StreamExt,
};
use swim_api::{
    agent::{LaneConfig, UplinkKind},
    error::StoreError,
    protocol::{
        agent::{
            LaneRequest, LaneRequestDecoder, LaneRequestEncoder, LaneResponse, LaneResponseEncoder,
        },
        WithLengthBytesCodec,
    },
};
use swim_model::Text;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{self, byte_channel, ByteWriter},
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::agent::{
    store::{InitFut, Initializer, StoreInitError},
    task::{InitialEndpoints, LaneEndpoint},
    AgentExecError, AgentRuntimeRequest, DownlinkRequest, Io,
};

trait TestInit {
    type Output;

    fn create(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self;

    fn run_test(self) -> BoxFuture<'static, Self::Output>;
}

struct NoLanesInit {
    _requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<DownlinkRequest>,
    init_complete: trigger::Sender,
}

impl NoLanesInit {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
    ) -> Self {
        NoLanesInit {
            _requests: requests,
            _dl_requests: dl_requests,
            init_complete,
        }
    }

    async fn run(self) {
        let NoLanesInit { init_complete, .. } = self;
        init_complete.trigger();
    }
}

impl TestInit for NoLanesInit {
    type Output = ();

    fn create(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        _config: LaneConfig,
    ) -> Self {
        NoLanesInit::new(requests, downlink_requests, init_complete)
    }

    fn run_test(self) -> BoxFuture<'static, Self::Output> {
        self.run().boxed()
    }
}

struct SingleLaneInit {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<DownlinkRequest>,
    init_complete: trigger::Sender,
    config: LaneConfig,
}

const INIT_STOPPED: &str = "Inialization task stopped.";
const NO_RESPONSE: &str = "Initialization task did not provide a response.";
const NO_LANE: &str = "Initialization task failed to create the lane";

impl SingleLaneInit {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        SingleLaneInit {
            requests,
            _dl_requests: dl_requests,
            init_complete,
            config,
        }
    }

    async fn run(self) -> Io {
        let SingleLaneInit {
            requests,
            _dl_requests,
            init_complete,
            config,
        } = self;
        let (lane_tx, lane_rx) = oneshot::channel();
        requests
            .send(AgentRuntimeRequest::AddLane {
                name: Text::new("my_lane"),
                kind: UplinkKind::Value,
                config,
                promise: lane_tx,
            })
            .await
            .expect(INIT_STOPPED);

        let lane_io = lane_rx.await.expect(NO_RESPONSE).expect(NO_LANE);

        init_complete.trigger();
        lane_io
    }
}

impl TestInit for SingleLaneInit {
    type Output = Io;

    fn create(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        SingleLaneInit::new(requests, downlink_requests, init_complete, config)
    }

    fn run_test(self) -> BoxFuture<'static, Self::Output> {
        self.run().boxed()
    }
}

const DL_CHAN_SIZE: usize = 8;
const INIT_TIMEOUT: Duration = Duration::from_secs(5);

async fn run_test<T: TestInit>() -> (
    Result<InitialEndpoints, AgentExecError>,
    <T as TestInit>::Output,
) {
    let (req_tx, req_rx) = mpsc::channel(8);
    let (done_tx, done_rx) = trigger::trigger();
    let (dl_tx, dl_rx) = mpsc::channel(DL_CHAN_SIZE);

    let runtime = super::AgentInitTask::new(req_rx, dl_tx, done_rx, INIT_TIMEOUT);
    let test = T::create(req_tx, dl_rx, done_tx, LaneConfig::default());

    join(runtime.run(), test.run_test()).await
}

#[tokio::test]
async fn no_lanes() {
    let (initial, _) = run_test::<NoLanesInit>().await;
    assert!(initial.is_err());
}

fn check_connected(first: &mut Io, second: &mut Io) {
    let (tx1, rx1) = first;
    let (tx2, rx2) = second;

    assert!(byte_channel::are_connected(tx1, rx2));
    assert!(byte_channel::are_connected(tx2, rx1));
}

#[tokio::test]
async fn single_lane() {
    let (initial_result, mut agent_io) = run_test::<SingleLaneInit>().await;
    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { mut endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 1);
    let LaneEndpoint { name, kind, mut io } = endpoints.pop().unwrap();
    assert_eq!(name, "my_lane");
    assert_eq!(kind, UplinkKind::Value);
    check_connected(&mut agent_io, &mut io);
}

struct TwoLanesInit {
    requests: mpsc::Sender<AgentRuntimeRequest>,
    _dl_requests: mpsc::Receiver<DownlinkRequest>,
    init_complete: trigger::Sender,
    config: LaneConfig,
}

#[derive(Debug)]
struct TwoLanes {
    value: Io,
    map: Io,
}

impl TwoLanesInit {
    fn new(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        TwoLanesInit {
            requests,
            _dl_requests: dl_requests,
            init_complete,
            config,
        }
    }

    async fn run(self) -> TwoLanes {
        let TwoLanesInit {
            requests,
            _dl_requests,
            init_complete,
            config,
        } = self;
        let (lane_tx1, lane_rx1) = oneshot::channel();
        let (lane_tx2, lane_rx2) = oneshot::channel();
        requests
            .send(AgentRuntimeRequest::AddLane {
                name: Text::new("value_lane"),
                kind: UplinkKind::Value,
                config,
                promise: lane_tx1,
            })
            .await
            .expect(INIT_STOPPED);

        requests
            .send(AgentRuntimeRequest::AddLane {
                name: Text::new("map_lane"),
                kind: UplinkKind::Map,
                config,
                promise: lane_tx2,
            })
            .await
            .expect(INIT_STOPPED);

        let lane_io1 = lane_rx1.await.expect(NO_RESPONSE).expect(NO_LANE);

        let lane_io2 = lane_rx2.await.expect(NO_RESPONSE).expect(NO_LANE);

        init_complete.trigger();
        TwoLanes {
            value: lane_io1,
            map: lane_io2,
        }
    }
}

impl TestInit for TwoLanesInit {
    type Output = TwoLanes;

    fn create(
        requests: mpsc::Sender<AgentRuntimeRequest>,
        dl_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
        config: LaneConfig,
    ) -> Self {
        TwoLanesInit::new(requests, dl_requests, init_complete, config)
    }

    fn run_test(self) -> BoxFuture<'static, Self::Output> {
        self.run().boxed()
    }
}

#[tokio::test]
async fn two_lanes() {
    let (initial_result, agent_lanes) = run_test::<TwoLanesInit>().await;
    let initial = initial_result.expect("No lanes were registered.");

    let InitialEndpoints { endpoints, .. } = initial;

    assert_eq!(endpoints.len(), 2);

    let mut seen_value = false;
    let mut seen_map = false;

    let TwoLanes { mut value, mut map } = agent_lanes;

    for LaneEndpoint { name, kind, mut io } in endpoints {
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
        }
    }
}

#[derive(Default)]
struct DummtInit {
    error: Option<StoreInitError>,
}

impl<'a> Initializer<'a> for DummtInit {
    fn initialize<'b>(self: Box<Self>, writer: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            if let Some(err) = self.error {
                Err(err)
            } else {
                let mut framed = FramedWrite::new(
                    writer,
                    LaneRequestEncoder::new(WithLengthBytesCodec::default()),
                );
                framed.send(LaneRequest::<&[u8]>::InitComplete).await?;
                Ok(())
            }
        }
        .boxed()
    }
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const LANE_INIT_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn run_initializer_success() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
        let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
        let init_task = super::lane_initialization(
            Text::new("lane"),
            UplinkKind::Value,
            LANE_INIT_TIMEOUT,
            tx_in,
            rx_out,
            Box::new(DummtInit::default()),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(
                rx_in,
                LaneRequestDecoder::new(WithLengthBytesCodec::default()),
            );
            let mut framed_write = FramedWrite::new(
                tx_out,
                LaneResponseEncoder::new(WithLengthBytesCodec::default()),
            );

            assert!(matches!(
                framed_read.next().await,
                Some(Ok(LaneRequest::InitComplete))
            ));
            assert!(framed_write
                .send(LaneResponse::<&[u8]>::Initialized)
                .await
                .is_ok());
        };

        let (result, _) = join(init_task, test_task).await;
        let LaneEndpoint { name, kind, .. } = result.expect("Init failed.");
        assert_eq!(name, "lane");
        assert_eq!(kind, UplinkKind::Value);
    })
    .await
    .expect("Test timed out.")
}

#[tokio::test]
async fn run_initializer_failed_init() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (tx_in, _rx_in) = byte_channel(BUFFER_SIZE);
        let (_tx_out, rx_out) = byte_channel(BUFFER_SIZE);
        let init = DummtInit {
            error: Some(StoreInitError::Store(StoreError::KeyspaceNotFound)),
        };
        let init_task = super::lane_initialization(
            Text::new("lane"),
            UplinkKind::Value,
            LANE_INIT_TIMEOUT,
            tx_in,
            rx_out,
            Box::new(init),
        );

        let result = init_task.await;
        match result {
            Err(AgentExecError::FailedRestoration {
                lane_name,
                error: StoreInitError::Store(StoreError::KeyspaceNotFound),
            }) => {
                assert_eq!(lane_name, "lane");
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn run_initializer_bad_response() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
        let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
        let init_task = super::lane_initialization(
            Text::new("lane"),
            UplinkKind::Value,
            LANE_INIT_TIMEOUT,
            tx_in,
            rx_out,
            Box::new(DummtInit::default()),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(
                rx_in,
                LaneRequestDecoder::new(WithLengthBytesCodec::default()),
            );
            let mut framed_write = FramedWrite::new(
                tx_out,
                LaneResponseEncoder::new(WithLengthBytesCodec::default()),
            );

            assert!(matches!(
                framed_read.next().await,
                Some(Ok(LaneRequest::InitComplete))
            ));
            assert!(framed_write
                .send(LaneResponse::StandardEvent(&[0]))
                .await
                .is_ok());
        };

        let (result, _) = join(init_task, test_task).await;
        match result {
            Err(AgentExecError::FailedRestoration {
                lane_name,
                error: StoreInitError::NoAckFromLane,
            }) => {
                assert_eq!(lane_name, "lane");
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
    })
    .await
    .expect("Test timed out.")
}

#[tokio::test]
async fn run_initializer_timeout() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
        let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);
        let init_task = super::lane_initialization(
            Text::new("lane"),
            UplinkKind::Value,
            Duration::from_millis(100),
            tx_in,
            rx_out,
            Box::new(DummtInit::default()),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(
                rx_in,
                LaneRequestDecoder::new(WithLengthBytesCodec::default()),
            );

            assert!(matches!(
                framed_read.next().await,
                Some(Ok(LaneRequest::InitComplete))
            ));
            tx_out
        };

        let (result, _tx_out) = join(init_task, test_task).await;
        match result {
            Err(AgentExecError::FailedRestoration {
                lane_name,
                error: StoreInitError::LaneInitiailizationTimeout,
            }) => {
                assert_eq!(lane_name, "lane");
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
    })
    .await
    .expect("Test timed out.")
}
