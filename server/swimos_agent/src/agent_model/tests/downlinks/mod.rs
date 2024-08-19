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

mod empty_agent;
mod start_dl_lifecycle;

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use empty_agent::EmptyAgent;
use futures::{
    future::{join, ready, BoxFuture},
    FutureExt,
};
use parking_lot::Mutex;
use start_dl_lifecycle::StartDownlinkLifecycle;
use swimos_api::{
    address::Address,
    agent::{
        AgentConfig, AgentContext, AgentTask, DownlinkKind, HttpLaneRequestChannel, LaneConfig,
        StoreKind, WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError},
};
use swimos_model::Text;
use swimos_utilities::{
    byte_channel::{byte_channel, ByteReader, ByteWriter},
    future::{IntervalStrategy, Quantity, RetryStrategy},
    non_zero_usize,
};
use tokio::sync::mpsc;

use crate::{
    agent_lifecycle::HandlerContext,
    agent_model::AgentModel,
    event_handler::{EventHandler, HandlerActionExt},
};

use super::make_uri;

#[derive(Debug)]
struct DownlinkResponses {
    errors: Vec<DownlinkRuntimeError>,
    io: Option<(ByteWriter, ByteReader)>,
}

impl DownlinkResponses {
    fn new(errors: Vec<DownlinkRuntimeError>, io: Option<(ByteWriter, ByteReader)>) -> Self {
        DownlinkResponses { errors, io }
    }

    fn pop(&mut self) -> Result<(ByteWriter, ByteReader), DownlinkRuntimeError> {
        if let Some(error) = self.errors.pop() {
            Err(error)
        } else if let Some(io) = self.io.take() {
            Ok(io)
        } else {
            panic!("Exhausted");
        }
    }
}

#[derive(Clone, Debug)]
struct DlTestContext {
    expected_address: Address<Text>,
    expected_kind: DownlinkKind,
    responses: Arc<Mutex<DownlinkResponses>>,
    ad_hoc_channel: Arc<Mutex<(ByteReader, Option<ByteWriter>)>>,
}

impl DlTestContext {
    fn new(
        expected_address: Address<Text>,
        expected_kind: DownlinkKind,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        let (ad_hoc_writer, ad_hoc_reader) = byte_channel(BUFFER_SIZE);
        DlTestContext {
            expected_address,
            expected_kind,
            responses: Arc::new(Mutex::new(DownlinkResponses::new(vec![], Some(io)))),
            ad_hoc_channel: Arc::new(Mutex::new((ad_hoc_reader, Some(ad_hoc_writer)))),
        }
    }
}

impl AgentContext for DlTestContext {
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        let DlTestContext { ad_hoc_channel, .. } = self;
        ready(Ok(ad_hoc_channel.lock().1.take().expect("Reader taken twice."))).boxed()
    }

    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected call.");
    }

    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        let DlTestContext {
            expected_address,
            expected_kind,
            responses,
            ..
        } = self;
        let addr = Address::new(host, node, lane);
        assert_eq!(addr, expected_address.borrow_parts());
        assert_eq!(kind, *expected_kind);
        let result = responses.lock().pop();
        ready(result).boxed()
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected call.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected call.");
    }
}

struct TestContext {
    rx: mpsc::UnboundedReceiver<DlResult>,
}

fn config() -> AgentConfig {
    AgentConfig {
        keep_linked_retry: RetryStrategy::Interval(IntervalStrategy {
            retry: Quantity::Finite(2),
            delay: None,
        }),
        ..Default::default()
    }
}

async fn init_agent(context: Box<DlTestContext>) -> (AgentTask, TestContext) {
    let address = addr();

    let (tx, rx) = mpsc::unbounded_channel();

    let model =
        AgentModel::<EmptyAgent, StartDownlinkLifecycle>::from_fn(EmptyAgent::default, move || {
            StartDownlinkLifecycle::new(address.clone(), Box::new(on_done(tx.clone())))
        });

    let task = model
        .initialize_agent(make_uri(), HashMap::new(), config(), context)
        .await
        .expect("Initialization failed.");
    (task, TestContext { rx })
}

type DlResult = Result<(), DownlinkRuntimeError>;
type BoxEh = Box<dyn EventHandler<EmptyAgent> + Send + 'static>;

fn on_done(tx: mpsc::UnboundedSender<DlResult>) -> impl FnOnce(DlResult) -> BoxEh + Send + 'static {
    move |result: DlResult| {
        let context: HandlerContext<EmptyAgent> = Default::default();
        Box::new(
            context
                .effect(move || {
                    assert!(tx.send(result).is_ok());
                })
                .followed_by(context.stop()),
        )
    }
}

fn addr() -> Address<Text> {
    Address::new(None, Text::new("/node"), Text::new("lane"))
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const TEST_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn immediately_successful_downlink() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, _out_rx) = byte_channel(BUFFER_SIZE);

        let agent_context = DlTestContext::new(addr(), DownlinkKind::Value, (out_tx, in_rx));

        let (agent_task, TestContext { mut rx }) = init_agent(Box::new(agent_context)).await;

        let check = async move {
            let result = rx.recv().await.expect("Result expected.");
            assert!(result.is_ok());
        };

        let (result, _) = join(agent_task, check).await;
        assert!(result.is_ok());
    })
    .await
    .expect("Test timed out.");
}
