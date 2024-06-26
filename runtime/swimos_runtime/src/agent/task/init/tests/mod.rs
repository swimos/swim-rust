// Copyright 2015-2024 Swim Inc.
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

use futures::{
    future::{join, join3, BoxFuture},
    FutureExt, SinkExt, StreamExt, TryFutureExt,
};
use swimos_agent_protocol::encoding::lane::{
    RawValueLaneRequestDecoder, RawValueLaneRequestEncoder, RawValueLaneResponseEncoder,
};
use swimos_agent_protocol::{LaneRequest, LaneResponse};
use swimos_api::{
    agent::{LaneConfig, LaneKind, UplinkKind, WarpLaneKind},
    error::StoreError,
    persistence::StoreDisabled,
};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{self, byte_channel, ByteWriter},
    future::RetryStrategy,
    non_zero_usize, trigger,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::agent::{
    reporting::UplinkReporter,
    store::{AgentPersistence, InitFut, Initializer, StoreInitError},
    task::{InitialEndpoints, LaneEndpoint, LinksTaskConfig},
    AgentExecError, AgentRuntimeRequest, Io, LinkRequest, NodeReporting,
    UplinkReporterRegistration,
};

use super::InitTaskConfig;

mod no_store;
mod with_store;

trait TestInit {
    type Output;

    fn run_test(
        self,
        requests: mpsc::Sender<AgentRuntimeRequest>,
        link_requests: mpsc::Receiver<LinkRequest>,
        init_complete: trigger::Sender,
    ) -> BoxFuture<'static, Self::Output>;
}

const INIT_STOPPED: &str = "Inialization task stopped.";
const NO_RESPONSE: &str = "Initialization task did not provide a response.";
const NO_LANE: &str = "Initialization task failed to create the lane";
const NO_STORE: &str = "Initialization task failed to create the store";

const DL_CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);
const INIT_TIMEOUT: Duration = Duration::from_secs(5);
const AD_HOC_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);

const INIT_CONFIG: InitTaskConfig = InitTaskConfig {
    ad_hoc_queue_size: DL_CHAN_SIZE,
    item_init_timeout: INIT_TIMEOUT,
    external_links: LinksTaskConfig {
        buffer_size: BUFFER_SIZE,
        retry_strategy: RetryStrategy::none(),
        timeout_delay: AD_HOC_TIMEOUT,
    },
    http_lane_channel_size: HTTP_CHAN_SIZE,
};

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
    let (dl_tx, dl_rx) = mpsc::channel(DL_CHAN_SIZE.get());

    let runtime = super::AgentInitTask::with_store(
        AGENT_ID,
        req_rx,
        dl_tx,
        done_rx,
        INIT_CONFIG,
        None,
        store,
    );
    let test = init.run_test(req_tx, dl_rx, done_tx);

    tokio::time::timeout(TEST_TIMEOUT, join(runtime.run().map_ok(|(ep, _)| ep), test))
        .await
        .expect("Timed out.")
}

fn check_connected(first: &mut Io, second: &mut Io) {
    let (tx1, rx1) = first;
    let (tx2, rx2) = second;

    assert!(byte_channel::are_connected(tx1, rx2));
    assert!(byte_channel::are_connected(tx2, rx1));
}

#[derive(Default)]
struct DummyInit {
    error: Option<StoreInitError>,
}

impl<'a> Initializer<'a> for DummyInit {
    fn initialize<'b>(self: Box<Self>, writer: &'b mut ByteWriter) -> InitFut<'b>
    where
        'a: 'b,
    {
        async move {
            if let Some(err) = self.error {
                Err(err)
            } else {
                let mut framed = FramedWrite::new(writer, RawValueLaneRequestEncoder::default());
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
            WarpLaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
            tx_in,
            rx_out,
            Box::<DummyInit>::default(),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(rx_in, RawValueLaneRequestDecoder::default());
            let mut framed_write = FramedWrite::new(tx_out, RawValueLaneResponseEncoder::default());

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
        let init = DummyInit {
            error: Some(StoreInitError::Store(StoreError::KeyspaceNotFound)),
        };
        let init_task = super::lane_initialization(
            Text::new("lane"),
            WarpLaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
            tx_in,
            rx_out,
            Box::new(init),
        );

        let result = init_task.await;
        match result {
            Err(StoreInitError::Store(StoreError::KeyspaceNotFound)) => {}
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
            WarpLaneKind::Value,
            LANE_INIT_TIMEOUT,
            None,
            tx_in,
            rx_out,
            Box::<DummyInit>::default(),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(rx_in, RawValueLaneRequestDecoder::default());
            let mut framed_write = FramedWrite::new(tx_out, RawValueLaneResponseEncoder::default());

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
            Err(StoreInitError::NoAckFromItem) => {}
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
            WarpLaneKind::Value,
            Duration::from_millis(100),
            None,
            tx_in,
            rx_out,
            Box::<DummyInit>::default(),
        );

        let test_task = async {
            let mut framed_read = FramedRead::new(rx_in, RawValueLaneRequestDecoder::default());

            assert!(matches!(
                framed_read.next().await,
                Some(Ok(LaneRequest::InitComplete))
            ));
            tx_out
        };

        let (result, _tx_out) = join(init_task, test_task).await;
        match result {
            Err(StoreInitError::ItemInitializationTimeout) => {}
            ow => panic!("Unexpected result: {:?}", ow),
        }
    })
    .await
    .expect("Test timed out.")
}

async fn provide_reporting(
    mut rx: mpsc::Receiver<UplinkReporterRegistration>,
    expected: Vec<(Uuid, Text, LaneKind)>,
) {
    let mut expected_map: HashMap<_, _> = expected
        .into_iter()
        .map(|(id, name, kind)| (id, (name, kind)))
        .collect();
    while let Some(UplinkReporterRegistration {
        agent_id,
        lane_name,
        kind,
        ..
    }) = rx.recv().await
    {
        let (expected_name, expected_kind) = expected_map.remove(&agent_id).expect("Unexpected ID");
        assert_eq!(lane_name, expected_name);
        assert_eq!(kind, expected_kind);
        if expected_map.is_empty() {
            break;
        }
    }
    assert!(expected_map.is_empty());
}

const AGENT_ID: Uuid = Uuid::from_u128(123);

async fn run_test_with_reporting<T: TestInit>(
    init: T,
    expected: Vec<(Uuid, Text, LaneKind)>,
) -> (
    Result<InitialEndpoints, AgentExecError>,
    <T as TestInit>::Output,
) {
    let (req_tx, req_rx) = mpsc::channel(8);
    let (done_tx, done_rx) = trigger::trigger();
    let (dl_tx, dl_rx) = mpsc::channel(DL_CHAN_SIZE.get());

    let aggregate_reporter = UplinkReporter::default();
    let (reg_tx, reg_rx) = mpsc::channel(8);
    let reporting = NodeReporting::new(AGENT_ID, aggregate_reporter, reg_tx);

    let runtime = super::AgentInitTask::with_store(
        AGENT_ID,
        req_rx,
        dl_tx,
        done_rx,
        INIT_CONFIG,
        Some(reporting),
        StoreDisabled,
    );
    let test = init.run_test(req_tx, dl_rx, done_tx);
    let reporting_task = provide_reporting(reg_rx, expected);

    let (res, output, _) = join3(runtime.run().map_ok(|(ep, _)| ep), test, reporting_task).await;
    (res, output)
}
