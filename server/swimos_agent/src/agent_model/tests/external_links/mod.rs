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

mod commander_lifecycle;
mod empty_agent;
mod start_dl_lifecycle;

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use commander_lifecycle::CommanderLifecycle;
use empty_agent::EmptyAgent;
use futures::{
    future::{join, ready, BoxFuture},
    FutureExt, StreamExt,
};
use parking_lot::Mutex;
use start_dl_lifecycle::StartDownlinkLifecycle;
use swimos_agent_protocol::{encoding::command::CommandMessageDecoder, CommandMessage};
use swimos_api::{
    address::Address,
    agent::{
        AgentConfig, AgentContext, AgentTask, DownlinkKind, HttpLaneRequestChannel, LaneConfig,
        StoreKind, WarpLaneKind,
    },
    error::{AgentRuntimeError, DownlinkFailureReason, DownlinkRuntimeError, OpenStoreError},
};
use swimos_model::{Text, Value};
use swimos_utilities::{
    byte_channel::{byte_channel, ByteReader, ByteWriter},
    future::{IntervalStrategy, Quantity, RetryStrategy},
    non_zero_usize, trigger,
};
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;

use crate::{
    agent_lifecycle::HandlerContext,
    agent_model::AgentModel,
    event_handler::{BoxEventHandler, HandlerActionExt},
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
    cmd_channel: Arc<Mutex<(Option<ByteReader>, Option<ByteWriter>)>>,
}

impl DlTestContext {
    fn new(
        expected_address: Address<Text>,
        expected_kind: DownlinkKind,
        io: (ByteWriter, ByteReader),
    ) -> Self {
        let (cmd_writer, cmd_reader) = byte_channel(BUFFER_SIZE);
        DlTestContext {
            expected_address,
            expected_kind,
            responses: Arc::new(Mutex::new(DownlinkResponses::new(vec![], Some(io)))),
            cmd_channel: Arc::new(Mutex::new((Some(cmd_reader), Some(cmd_writer)))),
        }
    }

    fn take_reader(&self) -> ByteReader {
        self.cmd_channel.lock().0.take().expect("Already taken.")
    }

    fn with_errors(
        expected_address: Address<Text>,
        expected_kind: DownlinkKind,
        errors: Vec<DownlinkRuntimeError>,
        io: Option<(ByteWriter, ByteReader)>,
    ) -> Self {
        let (cmd_writer, cmd_reader) = byte_channel(BUFFER_SIZE);
        DlTestContext {
            expected_address,
            expected_kind,
            responses: Arc::new(Mutex::new(DownlinkResponses::new(errors, io))),
            cmd_channel: Arc::new(Mutex::new((Some(cmd_reader), Some(cmd_writer)))),
        }
    }
}

impl AgentContext for DlTestContext {
    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        let DlTestContext { cmd_channel, .. } = self;
        ready(Ok(cmd_channel
            .lock()
            .1
            .take()
            .expect("Reader taken twice.")))
        .boxed()
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
        async move { result }.boxed()
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

struct TestContextDl {
    _tx: mpsc::UnboundedSender<DlResult>,
    rx: mpsc::UnboundedReceiver<DlResult>,
}

struct TestContextCmd {
    rx: ByteReader,
    stop_tx: trigger::Sender,
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

async fn init_agent_dl(context: Box<DlTestContext>) -> (AgentTask, TestContextDl) {
    let address = addr();

    let (tx, rx) = mpsc::unbounded_channel();
    let tx_cpy = tx.clone();

    let model =
        AgentModel::<EmptyAgent, StartDownlinkLifecycle>::from_fn(EmptyAgent::default, move || {
            StartDownlinkLifecycle::new(address.clone(), Box::new(on_done(tx.clone())))
        });

    let task = model
        .initialize_agent(make_uri(), HashMap::new(), config(), context)
        .await
        .expect("Initialization failed.");
    (task, TestContextDl { rx, _tx: tx_cpy })
}

async fn init_agent_cmd(context: Box<DlTestContext>) -> (AgentTask, TestContextCmd) {
    let address = addr();
    let (stop_tx, stop_rx) = trigger::trigger();

    let model =
        AgentModel::<EmptyAgent, CommanderLifecycle>::from_fn(EmptyAgent::default, move || {
            CommanderLifecycle::new(address.clone(), stop_rx.clone())
        });

    let rx = context.take_reader();

    let task = model
        .initialize_agent(make_uri(), HashMap::new(), config(), context)
        .await
        .expect("Initialization failed.");
    (task, TestContextCmd { stop_tx, rx })
}

type DlResult = Result<(), DownlinkRuntimeError>;
type BoxEh = BoxEventHandler<'static, EmptyAgent>;

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

        let (agent_task, TestContextDl { mut rx, _tx }) =
            init_agent_dl(Box::new(agent_context)).await;

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

#[tokio::test]
async fn immediate_fatal_error_downlink() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let agent_context = DlTestContext::with_errors(
            addr(),
            DownlinkKind::Value,
            vec![DownlinkRuntimeError::DownlinkConnectionFailed(
                DownlinkFailureReason::InvalidUrl,
            )],
            None,
        );

        let (agent_task, TestContextDl { mut rx, _tx }) =
            init_agent_dl(Box::new(agent_context)).await;

        let check = async move {
            let result = rx.recv().await.expect("Result expected.");

            assert!(matches!(
                result,
                Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::InvalidUrl
                ))
            ));
        };

        let (result, _) = join(agent_task, check).await;
        assert!(result.is_ok());
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn error_recovery_open_downlink() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, _out_rx) = byte_channel(BUFFER_SIZE);

        let agent_context = DlTestContext::with_errors(
            addr(),
            DownlinkKind::Value,
            vec![DownlinkRuntimeError::DownlinkConnectionFailed(
                DownlinkFailureReason::DownlinkStopped,
            )],
            Some((out_tx, in_rx)),
        );

        let (agent_task, TestContextDl { mut rx, _tx }) =
            init_agent_dl(Box::new(agent_context)).await;

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

#[tokio::test]
async fn eventual_fatal_error_downlink() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let agent_context = DlTestContext::with_errors(
            addr(),
            DownlinkKind::Value,
            vec![
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::DownlinkStopped,
                ),
                DownlinkRuntimeError::DownlinkConnectionFailed(DownlinkFailureReason::InvalidUrl),
            ],
            None,
        );

        let (agent_task, TestContextDl { mut rx, _tx }) =
            init_agent_dl(Box::new(agent_context)).await;

        let check = async move {
            let result = rx.recv().await.expect("Result expected.");

            assert!(matches!(
                result,
                Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::InvalidUrl
                ))
            ));
        };

        let (result, _) = join(agent_task, check).await;
        assert!(result.is_ok());
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn exhaust_open_downlink_retries() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, _out_rx) = byte_channel(BUFFER_SIZE);

        let agent_context = DlTestContext::with_errors(
            addr(),
            DownlinkKind::Value,
            vec![
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::DownlinkStopped,
                ),
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::DownlinkStopped,
                ),
                DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::DownlinkStopped,
                ),
            ],
            Some((out_tx, in_rx)),
        );

        let (agent_task, TestContextDl { mut rx, _tx }) =
            init_agent_dl(Box::new(agent_context)).await;

        let check = async move {
            let result = rx.recv().await.expect("Result expected.");

            assert!(matches!(
                result,
                Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::DownlinkStopped
                ))
            ));
        };

        let (result, _) = join(agent_task, check).await;
        assert!(result.is_ok());
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn agent_with_commander() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, _out_rx) = byte_channel(BUFFER_SIZE);

        let agent_context = DlTestContext::new(addr(), DownlinkKind::Value, (out_tx, in_rx));

        let (agent_task, TestContextCmd { rx, stop_tx }) =
            init_agent_cmd(Box::new(agent_context)).await;

        let mut cmd_msgs = FramedRead::new(rx, CommandMessageDecoder::<Text, Value>::default());

        let check = async move {
            let msg = cmd_msgs
                .next()
                .await
                .expect("Result expected.")
                .expect("Invalid message.");
            let id = match msg {
                CommandMessage::Register { address, id } => {
                    assert_eq!(address, addr());
                    id
                }
                ow => panic!("Unexpected message: {:?}", ow),
            };

            let msg1 = cmd_msgs
                .next()
                .await
                .expect("Result expected.")
                .expect("Invalid message.");
            let msg2 = cmd_msgs
                .next()
                .await
                .expect("Result expected.")
                .expect("Invalid message.");

            assert_eq!(
                msg1,
                CommandMessage::<Text, Value>::registered(id, Value::from(7), true)
            );
            assert_eq!(
                msg2,
                CommandMessage::<Text, Value>::registered(id, Value::from(22), true)
            );

            assert!(stop_tx.trigger());
        };

        let (result, _) = join(agent_task, check).await;
        assert!(result.is_ok());
    })
    .await
    .expect("Test timed out.");
}
