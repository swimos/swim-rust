// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::lane::channels::task::{LaneIoError, LaneUplinks, UplinkChannels};
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{UplinkAction, UplinkError, UplinkStateMachine};
use crate::agent::lane::channels::{
    AgentExecutionConfig, AgentExecutionContext, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::routing::{RoutingAddr, ServerRouter, TaggedClientEnvelope, TaggedEnvelope};
use common::model::Value;
use common::routing::RoutingError;
use common::sink::item::ItemSink;
use common::topic::{MpscTopic, Topic};
use common::warp::envelope::{Envelope, OutgoingLinkMessage};
use common::warp::path::RelativePath;
use futures::future::{join3, BoxFuture};
use futures::stream::{BoxStream, FusedStream};
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_form::{Form, FormDeserializeErr};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;

#[test]
fn lane_io_err_display_update() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_update_err(
        route,
        UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted),
    );

    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelops was invalid: Malformatted"
        ]
    );
}

#[test]
fn lane_io_err_display_uplink() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_uplink_errors(
        route,
        vec![UplinkErrorReport {
            error: UplinkError::SenderDropped,
            addr: RoutingAddr::remote(1),
        }],
    );
    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- uplink_errors =",
            "* Uplink to Remote Endpoint (1) failed: Uplink send channel was dropped."
        ]
    );
}

#[test]
fn lane_io_err_display_both() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::new(
        route,
        UpdateError::BadEnvelopeBody(FormDeserializeErr::Malformatted),
        vec![UplinkErrorReport {
            error: UplinkError::SenderDropped,
            addr: RoutingAddr::remote(1),
        }],
    );
    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelops was invalid: Malformatted",
            "- uplink_errors =",
            "* Uplink to Remote Endpoint (1) failed: Uplink send channel was dropped."
        ]
    );
}

#[derive(Clone)]
struct TestUplinkSpawner {
    respond_tx: mpsc::Sender<TaggedAction>,
    fail_on: HashSet<RoutingAddr>,
    fatal_errors: bool,
}

impl TestUplinkSpawner {
    fn new(
        respond_tx: mpsc::Sender<TaggedAction>,
        fail_on: Vec<RoutingAddr>,
        fatal_errors: bool,
    ) -> Self {
        TestUplinkSpawner {
            respond_tx,
            fail_on: fail_on.into_iter().collect(),
            fatal_errors,
        }
    }
}

impl LaneUplinks for TestUplinkSpawner {
    fn make_task<Handler, Top, Context>(
        &self,
        _message_handler: Arc<Handler>,
        channels: UplinkChannels<Top>,
        _route: RelativePath,
        _context: &Context,
    ) -> BoxFuture<'static, ()>
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: Topic<Handler::Event> + Send + 'static,
        Context: AgentExecutionContext,
    {
        let TestUplinkSpawner {
            mut respond_tx,
            fail_on,
            fatal_errors,
        } = self.clone();

        let UplinkChannels {
            mut actions,
            mut error_collector,
            ..
        } = channels;

        async move {
            while let Some(act) = actions.recv().await {
                let TaggedAction(addr, _) = &act;
                if fail_on.contains(addr) {
                    let error = if fatal_errors {
                        UplinkError::LaneStoppedReporting
                    } else {
                        UplinkError::FailedTransaction(TransactionError::HighContention {
                            num_failed: 10,
                        })
                    };
                    assert!(error_collector
                        .send(UplinkErrorReport { error, addr: *addr })
                        .await
                        .is_ok());
                } else {
                    assert!(respond_tx.send(act).await.is_ok());
                }
            }
        }
        .boxed()
    }
}

#[derive(Debug)]
struct Message(i32);

impl Form for Message {
    fn as_value(&self) -> Value {
        Value::Int32Value(self.0)
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        i32::try_from_value(value).map(|n| Message(n))
    }
}

impl From<Message> for Value {
    fn from(msg: Message) -> Self {
        Value::Int32Value(msg.0)
    }
}

struct TestHandler(Arc<Mutex<Vec<i32>>>);

impl TestHandler {
    fn new() -> Self {
        TestHandler(Default::default())
    }
}
struct DummyUplink;

impl UplinkStateMachine<i32> for DummyUplink {
    type Msg = Message;

    fn message_for(&self, event: i32) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(Some(Message(event)))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        _updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = i32> + Send + Unpin + 'a,
    {
        futures::stream::empty().boxed()
    }
}

struct TestUpdater(Arc<Mutex<Vec<i32>>>);

impl LaneUpdate for TestUpdater {
    type Msg = Message;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<Self::Msg, Err>> + Send + 'static,
        Err: Send,
        UpdateError: From<Err>,
    {
        let TestUpdater(values) = self;

        async move {
            pin_mut!(messages);
            loop {
                if let Some(msg) = messages.next().await {
                    match msg {
                        Ok(msg) => {
                            if msg.0 < 0 {
                                break Err(UpdateError::BadEnvelopeBody(
                                    FormDeserializeErr::Malformatted,
                                ));
                            } else {
                                values.lock().await.push(msg.0);
                            }
                        }
                        Err(e) => {
                            break Err(e.into());
                        }
                    }
                } else {
                    break Ok(());
                }
            }
        }
        .boxed()
    }
}

impl LaneMessageHandler for TestHandler {
    type Event = i32;
    type Uplink = DummyUplink;
    type Update = TestUpdater;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        DummyUplink
    }

    fn make_update(&self) -> Self::Update {
        TestUpdater(self.0.clone())
    }
}

#[derive(Clone)]
struct TestContext(
    mpsc::Sender<BoxFuture<'static, ()>>,
    mpsc::Sender<TaggedEnvelope>,
    Arc<Mutex<Option<trigger::Sender>>>,
);

impl TestContext {
    async fn stop(&self) {
        if let Some(tx) = self.2.lock().await.take() {
            tx.trigger();
        }
    }
}

struct TestRouter(mpsc::Sender<TaggedEnvelope>);

struct TestSender {
    addr: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl<'a> ItemSink<'a, Envelope> for TestSender {
    type Error = RoutingError;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        let tagged = TaggedEnvelope(self.addr, value);
        async move {
            self.inner
                .send(tagged)
                .await
                .map_err(|_| RoutingError::RouterDropped)
        }
        .boxed()
    }
}

impl ServerRouter for TestRouter {
    type Sender = TestSender;

    fn get_sender(&mut self, addr: RoutingAddr) -> Result<Self::Sender, RoutingError> {
        Ok(TestSender {
            addr,
            inner: self.0.clone(),
        })
    }
}

impl AgentExecutionContext for TestContext {
    type Router = TestRouter;

    fn router_handle(&self) -> Self::Router {
        TestRouter(self.1.clone())
    }

    fn spawner(&self) -> Sender<BoxFuture<'static, ()>> {
        self.0.clone()
    }
}

fn default_buffer() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn yield_after() -> NonZeroUsize {
    NonZeroUsize::new(256).unwrap()
}

fn make_config() -> AgentExecutionConfig {
    AgentExecutionConfig {
        max_concurrency: 1,
        action_buffer: default_buffer(),
        update_buffer: default_buffer(),
        uplink_err_buffer: default_buffer(),
        max_fatal_uplink_errors: 1,
        max_uplink_start_attempts: default_buffer(),
    }
}

fn route() -> RelativePath {
    RelativePath::new("node", "lane")
}

#[derive(Clone)]
struct TaskInput {
    envelope_tx: mpsc::Sender<TaggedClientEnvelope>,
    _event_tx: mpsc::Sender<i32>,
}

impl TaskInput {
    async fn send_link(&mut self, addr: RoutingAddr) {
        let env = TaggedClientEnvelope(addr, OutgoingLinkMessage::link("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_sync(&mut self, addr: RoutingAddr) {
        let env = TaggedClientEnvelope(addr, OutgoingLinkMessage::sync("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_unlink(&mut self, addr: RoutingAddr) {
        let env = TaggedClientEnvelope(addr, OutgoingLinkMessage::unlink("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_command(&mut self, addr: RoutingAddr, value: i32) {
        let env = TaggedClientEnvelope(
            addr,
            OutgoingLinkMessage::make_command("node", "lane", Some(value.into_value())),
        );
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn cause_update_error(&mut self, addr: RoutingAddr) {
        self.send_command(addr, -1).await;
    }
}

struct TaskOutput(mpsc::Receiver<TaggedAction>, Arc<Mutex<Vec<i32>>>);

impl TaskOutput {
    async fn check_history(&self, expected: Vec<i32>) {
        assert_eq!(&*self.1.lock().await, &expected);
    }

    async fn take_actions(&mut self, n: usize) -> Vec<TaggedAction> {
        tokio::time::timeout(
            Duration::from_secs(1),
            (&mut self.0).take(n).collect::<Vec<_>>(),
        )
        .await
        .expect("Timeout awaiting outputs.")
    }
}

fn make_task(
    fail_on: Vec<RoutingAddr>,
    fatal_errors: bool,
    config: AgentExecutionConfig,
    context: TestContext,
) -> (
    TaskInput,
    TaskOutput,
    BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>,
) {
    let (respond_tx, respond_rx) = mpsc::channel(5);
    let (envelope_tx, envelope_rx) = mpsc::channel(5);
    let (event_tx, event_rx) = mpsc::channel(5);

    let handler = TestHandler::new();
    let output = TaskOutput(respond_rx, handler.0.clone());
    let uplinks = TestUplinkSpawner::new(respond_tx, fail_on, fatal_errors);
    let (topic, _rec) = MpscTopic::new(event_rx, default_buffer(), yield_after());

    let task = super::run_lane_io(
        handler,
        uplinks,
        envelope_rx,
        topic,
        config,
        context,
        route(),
    )
    .boxed();
    let input = TaskInput {
        envelope_tx,
        _event_tx: event_tx,
    };

    (input, output, task)
}

fn make_context() -> (
    TestContext,
    mpsc::Receiver<TaggedEnvelope>,
    BoxFuture<'static, ()>,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(5);
    let (router_tx, router_rx) = mpsc::channel(5);
    let (stop_tx, stop_rx) = trigger::trigger();

    let context = TestContext(spawn_tx, router_tx, Arc::new(Mutex::new(Some(stop_tx))));
    let spawn_task = spawn_rx
        .take_until_completes(stop_rx)
        .for_each_concurrent(None, |t| t)
        .boxed();

    (context, router_rx, spawn_task)
}

#[tokio::test]
async fn handle_link_request() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, mut outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_link(addr).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Link)] if a == &addr));

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    assert!(result.is_ok());

    outputs.check_history(vec![]).await;
}

#[tokio::test]
async fn handle_sync_request() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, mut outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_sync(addr).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Sync)] if a == &addr));

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    assert!(result.is_ok());

    outputs.check_history(vec![]).await;
}

#[tokio::test]
async fn handle_unlink_request() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, mut outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_unlink(addr).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(
            matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Unlink)] if a == &addr)
        );

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    assert!(result.is_ok());

    outputs.check_history(vec![]).await;
}

#[tokio::test]
async fn handle_command() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let io_task = async move {
        inputs.send_command(addr, 87).await;

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    assert!(result.is_ok());

    outputs.check_history(vec![87]).await;
}

#[tokio::test]
async fn handle_multiple() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, mut outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_sync(addr).await;
        inputs.send_command(addr, 87).await;
        inputs.send_unlink(addr).await;
        inputs.send_command(addr, 65).await;

        let actions = outputs_ref.take_actions(2).await;

        assert!(matches!(actions.as_slice(), [
            TaggedAction(a, UplinkAction::Sync),
            TaggedAction(b, UplinkAction::Unlink)
        ] if a == &addr && b == &addr));

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    assert!(result.is_ok());

    outputs.check_history(vec![87, 65]).await;
}

#[tokio::test]
async fn fail_on_update_error() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();
    let (mut inputs, outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let _inputs_clone = inputs.clone();

    let io_task = async move {
        inputs.cause_update_error(addr).await;

        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    match result {
        Err(LaneIoError {
            route,
            update_error,
            uplink_errors,
        }) => {
            assert_eq!(route, RelativePath::new("node", "lane"));
            assert!(matches!(
                update_error,
                Some(UpdateError::BadEnvelopeBody(
                    FormDeserializeErr::Malformatted
                ))
            ));
            assert!(uplink_errors.is_empty());
        }
        _ => {
            panic!("Unexpected success.");
        }
    }

    outputs.check_history(vec![]).await;
}

#[tokio::test]
async fn continue_after_non_fatal_uplink_err() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();

    let bad_addr = RoutingAddr::remote(7);

    let (mut inputs, mut outputs, main_task) =
        make_task(vec![bad_addr], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_sync(bad_addr).await;
        inputs.send_link(addr).await;
        inputs.send_command(addr, 12).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Link)] if a == &addr));

        drop(inputs);
        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    match result.as_ref().map(|v| v.as_slice()) {
        Ok([UplinkErrorReport { error, addr: a }]) => {
            assert!(matches!(
                error,
                UplinkError::FailedTransaction(TransactionError::HighContention { num_failed: 10 })
            ));
            assert_eq!(a, &bad_addr);
        }
        ow => {
            panic!("Unexpected result: {:?}.", ow);
        }
    }

    outputs.check_history(vec![12]).await;
}

#[tokio::test]
async fn fail_after_too_many_fatal_uplink_errors() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();

    let bad_addr1 = RoutingAddr::remote(7);
    let bad_addr2 = RoutingAddr::remote(8);

    let (mut inputs, outputs, main_task) =
        make_task(vec![bad_addr1, bad_addr2], true, config, context.clone());

    let _inputs_clone = inputs.clone();

    let io_task = async move {
        inputs.send_sync(bad_addr1).await;
        inputs.send_sync(bad_addr2).await;

        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    match result {
        Err(LaneIoError {
            route,
            update_error: None,
            uplink_errors,
        }) => {
            assert_eq!(route, RelativePath::new("node", "lane"));
            match uplink_errors.as_slice() {
                [UplinkErrorReport {
                    error: error1,
                    addr: a1,
                }, UplinkErrorReport {
                    error: error2,
                    addr: a2,
                }] => {
                    assert!(matches!(error1, UplinkError::LaneStoppedReporting));
                    assert_eq!(a1, &bad_addr1);

                    assert!(matches!(error2, UplinkError::LaneStoppedReporting));
                    assert_eq!(a2, &bad_addr2);
                }
                ow => {
                    panic!("Unexpected uplink errors: {:?}.", ow);
                }
            }
        }
        ow => {
            panic!("Unexpected result: {:?}.", ow);
        }
    }

    outputs.check_history(vec![]).await;
}

#[tokio::test]
async fn report_uplink_failures_on_update_failure() {
    let (context, _envelope_rx, spawn_task) = make_context();
    let config = make_config();

    let bad_addr = RoutingAddr::remote(22);

    let (mut inputs, mut outputs, main_task) =
        make_task(vec![bad_addr], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let _inputs_clone = inputs.clone();

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs.send_link(bad_addr).await;
        inputs.send_link(addr).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Link)] if a == &addr));

        inputs.cause_update_error(addr).await;

        context.stop().await;
    };

    let (_, result, _) = join3(spawn_task, main_task, io_task).await;
    match result {
        Err(LaneIoError {
            route,
            update_error,
            uplink_errors,
        }) => {
            assert_eq!(route, RelativePath::new("node", "lane"));
            assert!(matches!(
                update_error,
                Some(UpdateError::BadEnvelopeBody(
                    FormDeserializeErr::Malformatted
                ))
            ));
            match uplink_errors.as_slice() {
                [UplinkErrorReport { error, addr: a }] => {
                    assert!(matches!(
                        error,
                        UplinkError::FailedTransaction(TransactionError::HighContention {
                            num_failed: 10
                        })
                    ));
                    assert_eq!(a, &bad_addr);
                }
                ow => {
                    panic!("Unexpected uplink errors: {:?}.", ow);
                }
            }
        }
        _ => {
            panic!("Unexpected success.");
        }
    }

    outputs.check_history(vec![]).await;
}
