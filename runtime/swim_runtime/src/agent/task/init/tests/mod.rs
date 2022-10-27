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
    meta::lane::LaneKind,
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
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::agent::{
    store::{AgentPersistence, InitFut, Initializer, StoreInitError},
    task::{InitialEndpoints, LaneEndpoint},
    AgentExecError, AgentRuntimeRequest, DownlinkRequest, Io,
};

mod no_store;
mod with_store;

trait TestInit {
    type Output;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        downlink_requests: mpsc::Receiver<DownlinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output>;
}

const INIT_STOPPED: &str = "Inialization task stopped.";
const NO_RESPONSE: &str = "Initialization task did not provide a response.";
const NO_LANE: &str = "Initialization task failed to create the lane";

const DL_CHAN_SIZE: usize = 8;
const INIT_TIMEOUT: Duration = Duration::from_secs(5);

const TRANSIENT: LaneConfig = LaneConfig {
    input_buffer_size: BUFFER_SIZE,
    output_buffer_size: BUFFER_SIZE,
    transient: true,
};

const PERSISTENT: LaneConfig = LaneConfig {
    input_buffer_size: BUFFER_SIZE,
    output_buffer_size: BUFFER_SIZE,
    transient: false,
};

const CONFIGS: &[LaneConfig] = &[TRANSIENT, PERSISTENT];

async fn run_test<T: TestInit, Store>(
    init: T,
    store: Store,
) -> (
    Result<InitialEndpoints, AgentExecError>,
    <T as TestInit>::Output,
)
where
    Store: AgentPersistence + Clone + Send + Sync,
{
    let (req_tx, req_rx) = mpsc::channel(8);
    let (done_tx, done_rx) = trigger::trigger();
    let (dl_tx, dl_rx) = mpsc::channel(DL_CHAN_SIZE);

    let runtime =
        super::AgentInitTask::with_store(req_rx, dl_tx, done_rx, INIT_TIMEOUT, None, store);
    let test = init.run_test(req_tx, dl_rx, done_tx);

    join(runtime.run(), test).await
}

fn check_connected(first: &mut Io, second: &mut Io) {
    let (tx1, rx1) = first;
    let (tx2, rx2) = second;

    assert!(byte_channel::are_connected(tx1, rx2));
    assert!(byte_channel::are_connected(tx2, rx1));
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
            LaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
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
            LaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
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
            LaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
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
            LaneKind::Value,
            Duration::from_millis(100),
            None,
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
