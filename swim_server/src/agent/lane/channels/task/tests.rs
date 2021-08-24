// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::{LaneIoError, LaneUplinks, UplinkChannels};
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{
    PeelResult, UplinkAction, UplinkError, UplinkStateMachine,
};
use crate::agent::lane::channels::{
    AgentExecutionConfig, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::agent::lane::model::action::{Action, ActionLane};
use crate::agent::lane::model::command::{Command, CommandLane};
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::model::supply::SupplyLane;
use crate::agent::Eff;
use crate::meta::accumulate_metrics;
use crate::meta::log::make_node_logger;
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::lane::LanePulse;
use crate::meta::metric::node::NodePulse;
use crate::meta::metric::uplink::{UplinkActionObserver, WarpUplinkPulse};
use crate::meta::metric::{aggregator_sink, AggregatorError, NodeMetricAggregator};
use crate::meta::pulse::PulseLanes;
use futures::future::{join, join3, ready, BoxFuture};
use futures::stream::{once, BoxStream, FusedStream};
use futures::{Future, FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::{HashMap, HashSet};
use std::convert::{identity, TryFrom};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_common::form::{Form, FormErr};
use swim_common::model::Value;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::error::RouterError;
use swim_common::routing::error::RoutingError;
use swim_common::routing::error::SendError;
use swim_common::routing::{
    BidirectionalRoute, ConnectionDropped, Route, Router, RoutingAddr, TaggedClientEnvelope,
    TaggedEnvelope, TaggedSender,
};
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::{Envelope, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;
use url::Url;
use utilities::instant::AtomicInstant;
use utilities::sync::{promise, topic, trigger};
use utilities::uri::RelativeUri;

const METRIC_SAMPLE_RATE: Duration = Duration::from_millis(100);

#[test]
fn lane_io_err_display_update() {
    let route = RelativePath::new("node", "lane");
    let err =
        LaneIoError::for_update_err(route, UpdateError::BadEnvelopeBody(FormErr::Malformatted));

    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelope was invalid: Malformatted"
        ]
    );
}

#[test]
fn lane_io_err_display_uplink() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_uplink_errors(
        route,
        vec![UplinkErrorReport {
            error: UplinkError::ChannelDropped,
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
            "* Uplink to Remote(1) failed: Uplink send channel was dropped."
        ]
    );

    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_uplink_errors(
        route,
        vec![UplinkErrorReport {
            error: UplinkError::ChannelDropped,
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
            "* Uplink to Remote(1) failed: Uplink send channel was dropped."
        ]
    );
}

#[test]
fn lane_io_err_display_both() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::new(
        route,
        UpdateError::BadEnvelopeBody(FormErr::Malformatted),
        vec![UplinkErrorReport {
            error: UplinkError::ChannelDropped,
            addr: RoutingAddr::remote(1),
        }],
    );
    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelope was invalid: Malformatted",
            "- uplink_errors =",
            "* Uplink to Remote(1) failed: Uplink send channel was dropped."
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
        _action_observer: UplinkActionObserver,
    ) -> BoxFuture<'static, ()>
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: DeferredSubscription<Handler::Event>,
        Context: AgentExecutionContext,
    {
        let TestUplinkSpawner {
            respond_tx,
            fail_on,
            fatal_errors,
        } = self.clone();

        let UplinkChannels {
            mut actions,
            error_collector,
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

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
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
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = i32> + Send + Unpin + 'a,
    {
        once(ready(PeelResult::Complete(updates))).boxed()
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
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send,
        UpdateError: From<Err>,
    {
        let TestUpdater(values) = self;

        async move {
            pin_mut!(messages);
            loop {
                if let Some(msg) = messages.next().await {
                    match msg {
                        Ok((_, msg)) => {
                            if msg.0 < 0 {
                                break Err(UpdateError::BadEnvelopeBody(FormErr::Malformatted));
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
struct TestContext {
    scheduler: mpsc::Sender<Eff>,
    messages: mpsc::Sender<TaggedEnvelope>,
    trigger: Arc<Mutex<Option<trigger::Sender>>>,
    _drop_tx: Arc<promise::Sender<ConnectionDropped>>,
    drop_rx: promise::Receiver<ConnectionDropped>,
    uri: RelativeUri,
    aggregator: NodeMetricAggregator,
    _metrics_jh: Arc<JoinHandle<Result<(), AggregatorError>>>,
    uplinks_idle_since: Arc<AtomicInstant>,
}

impl TestContext {
    async fn stop(&self) {
        if let Some(tx) = self.trigger.lock().await.take() {
            tx.trigger();
        }
    }
}

struct TestRouter {
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

#[derive(Clone, Debug)]
struct TestSender {
    addr: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl<'a> ItemSink<'a, Envelope> for TestSender {
    type Error = SendError;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        let tagged = TaggedEnvelope(self.addr, value);
        async move {
            self.inner.send(tagged).await.map_err(|err| {
                let TaggedEnvelope(_, envelope) = err.0;
                SendError::new(RoutingError::RouterDropped, envelope)
            })
        }
        .boxed()
    }
}

impl Router for TestRouter {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        let TestRouter {
            sender, drop_rx, ..
        } = self;
        ready(Ok(Route::new(
            TaggedSender::new(addr, sender.clone()),
            drop_rx.clone(),
        )))
        .boxed()
    }

    fn resolve_bidirectional(
        &mut self,
        _host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        //Todo dm
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'static, Result<RoutingAddr, RouterError>> {
        panic!("Unexpected resolution attempt.")
    }
}

impl AgentExecutionContext for TestContext {
    type Router = TestRouter;

    fn router_handle(&self) -> Self::Router {
        let TestContext {
            messages, drop_rx, ..
        } = self;
        TestRouter {
            sender: messages.clone(),
            drop_rx: drop_rx.clone(),
        }
    }

    fn spawner(&self) -> Sender<Eff> {
        self.scheduler.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.uri
    }

    fn metrics(&self) -> NodeMetricAggregator {
        self.aggregator.clone()
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.uplinks_idle_since
    }
}

fn default_buffer() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn make_config() -> AgentExecutionConfig {
    AgentExecutionConfig::with(
        default_buffer(),
        1,
        1,
        Duration::from_secs(5),
        None,
        Duration::from_secs(60),
    )
}

fn route() -> RelativePath {
    RelativePath::new("node", "lane")
}

struct TaskInput {
    envelope_tx: mpsc::Sender<TaggedClientEnvelope>,
    _event_tx: Option<topic::Sender<i32>>,
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

    async fn send_raw(&mut self, addr: RoutingAddr, value: Value) {
        let env = TaggedClientEnvelope(
            addr,
            OutgoingLinkMessage::make_command("node", "lane", Some(value)),
        );
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn cause_update_error(&mut self, addr: RoutingAddr) {
        self.send_command(addr, -1).await;
    }
}

struct TaskOutput(ReceiverStream<TaggedAction>, Arc<Mutex<Vec<i32>>>);

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
    let (event_tx, event_rx) = topic::channel(NonZeroUsize::new(5).unwrap());

    let handler = TestHandler::new();
    let output = TaskOutput(ReceiverStream::new(respond_rx), handler.0.clone());
    let uplinks = TestUplinkSpawner::new(respond_tx, fail_on, fatal_errors);
    let topic = event_rx.subscriber();

    let task = super::run_lane_io(
        handler,
        uplinks,
        ReceiverStream::new(envelope_rx),
        topic,
        config,
        context,
        route(),
    )
    .boxed();
    let input = TaskInput {
        envelope_tx,
        _event_tx: Some(event_tx),
    };

    (input, output, task)
}

fn make_aggregator(
    stop_rx: trigger::Receiver,
) -> (
    NodeMetricAggregator,
    impl Future<Output = Result<(), AggregatorError>>,
    MetricReceivers,
) {
    let buffer_size = 8;

    let path = RelativePath::new("node", "lane");

    let (uplink_tx, uplink_rx) = mpsc::channel(buffer_size);
    let lane = SupplyLane::new(uplink_tx);
    let mut uplinks = HashMap::new();
    uplinks.insert(path.clone(), lane);

    let (lane_tx, lane_rx) = mpsc::channel(buffer_size);
    let lane = SupplyLane::new(lane_tx);
    let mut lanes = HashMap::new();
    lanes.insert(path.clone(), lane);

    let (node_tx, node_rx) = mpsc::channel(buffer_size);
    let node = SupplyLane::new(node_tx);

    let receivers = MetricReceivers {
        uplink: uplink_rx,
        lane: lane_rx,
        node: node_rx,
    };

    let lanes = PulseLanes {
        uplinks,
        lanes,
        node,
    };

    let RelativePath { node, lane } = path;
    let uri = RelativeUri::try_from(format!("/{}/{}", node, lane)).unwrap();

    let config = MetricAggregatorConfig {
        sample_rate: METRIC_SAMPLE_RATE,
        buffer_size: NonZeroUsize::new(10).unwrap(),
        yield_after: NonZeroUsize::new(256).unwrap(),
        backpressure_config: KeyedBackpressureConfig {
            buffer_size: NonZeroUsize::new(2).unwrap(),
            yield_after: NonZeroUsize::new(256).unwrap(),
            bridge_buffer_size: NonZeroUsize::new(4).unwrap(),
            cache_size: NonZeroUsize::new(4).unwrap(),
        },
    };

    let (aggregator, task) =
        NodeMetricAggregator::new(uri.clone(), stop_rx, config, lanes, make_node_logger(uri));

    (aggregator, task, receivers)
}

struct MetricReceivers {
    uplink: mpsc::Receiver<WarpUplinkPulse>,
    lane: mpsc::Receiver<LanePulse>,
    node: mpsc::Receiver<NodePulse>,
}

fn make_context() -> (
    TestContext,
    mpsc::Receiver<TaggedEnvelope>,
    BoxFuture<'static, ()>,
    MetricReceivers,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(5);
    let (router_tx, router_rx) = mpsc::channel(5);
    let (stop_tx, stop_rx) = trigger::trigger();

    let (drop_tx, drop_rx) = promise::promise();
    let (aggregator, aggregator_task, receivers) = make_aggregator(stop_rx.clone());

    let context = TestContext {
        scheduler: spawn_tx,
        messages: router_tx,
        trigger: Arc::new(Mutex::new(Some(stop_tx))),
        _drop_tx: Arc::new(drop_tx),
        drop_rx,
        uri: RelativeUri::try_from("/mock/router".to_string()).unwrap(),
        aggregator,
        _metrics_jh: Arc::new(tokio::spawn(aggregator_task)),
        uplinks_idle_since: Arc::new(AtomicInstant::new(Instant::now())),
    };
    let spawn_task = ReceiverStream::new(spawn_rx)
        .take_until(stop_rx)
        .for_each_concurrent(None, |t| t)
        .boxed();

    (context, router_rx, spawn_task, receivers)
}

#[tokio::test]
async fn handle_link_request() {
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
    let config = make_config();
    let (mut inputs, outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let inputs_ref = &mut inputs;

    let io_task = async move {
        inputs_ref.cause_update_error(addr).await;

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
                Some(UpdateError::BadEnvelopeBody(FormErr::Malformatted))
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _) = make_context();
    let config = make_config();

    let bad_addr1 = RoutingAddr::remote(7);
    let bad_addr2 = RoutingAddr::remote(8);

    let (mut inputs, outputs, main_task) =
        make_task(vec![bad_addr1, bad_addr2], true, config, context.clone());

    let inputs_ref = &mut inputs;

    let io_task = async move {
        inputs_ref.send_sync(bad_addr1).await;
        inputs_ref.send_sync(bad_addr2).await;

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
    let (context, _envelope_rx, spawn_task, _) = make_context();
    let config = make_config();

    let bad_addr = RoutingAddr::remote(22);

    let (mut inputs, mut outputs, main_task) =
        make_task(vec![bad_addr], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let inputs_ref = &mut inputs;

    let outputs_ref = &mut outputs;

    let io_task = async move {
        inputs_ref.send_link(bad_addr).await;
        inputs_ref.send_link(addr).await;

        let actions = outputs_ref.take_actions(1).await;

        assert!(matches!(actions.as_slice(), [TaggedAction(a, UplinkAction::Link)] if a == &addr));

        inputs_ref.cause_update_error(addr).await;

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
                Some(UpdateError::BadEnvelopeBody(FormErr::Malformatted))
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

fn make_action_lane_task<Context: AgentExecutionContext + Send + Sync + 'static>(
    config: AgentExecutionConfig,
    context: Context,
) -> (
    impl Future<Output = Result<Vec<UplinkErrorReport>, LaneIoError>>,
    TaskInput,
) {
    let (feedback_tx, mut feedback_rx) = mpsc::channel::<Action<i32, i32>>(5);

    let mock_lifecycle = async move {
        while let Some(Action { command, responder }) = feedback_rx.recv().await {
            if let Some(responder) = responder {
                assert!(responder.send(command * 2).is_ok());
            }
        }
    };
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedClientEnvelope>(5);

    let lane: ActionLane<i32, i32> = ActionLane::new(feedback_tx);

    let task = super::run_action_lane_io(
        lane,
        ReceiverStream::new(envelope_rx),
        config,
        context,
        route(),
    );

    let input = TaskInput {
        envelope_tx,
        _event_tx: None,
    };

    (join(mock_lifecycle, task).map(|(_, r)| r), input)
}

fn make_command_lane_task<Context: AgentExecutionContext + Send + Sync + 'static>(
    config: AgentExecutionConfig,
    context: Context,
) -> (
    impl Future<Output = Result<Vec<UplinkErrorReport>, LaneIoError>>,
    TaskInput,
) {
    let (feedback_tx, mut feedback_rx) = mpsc::channel::<Command<i32>>(5);

    let mock_lifecycle = async move {
        while let Some(Command { command, responder }) = feedback_rx.recv().await {
            if let Some(responder) = responder {
                assert!(responder.send(command * 2).is_ok());
            }
        }
    };
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedClientEnvelope>(5);

    let lane: CommandLane<i32> = CommandLane::new(feedback_tx);

    let task = super::run_command_lane_io(
        lane,
        ReceiverStream::new(envelope_rx),
        config,
        context,
        route(),
    );

    let input = TaskInput {
        envelope_tx,
        _event_tx: None,
    };

    (join(mock_lifecycle, task).map(|(_, r)| r), input)
}

async fn expect_broadcast_envelopes(
    count: i32,
    router_rx: &mut mpsc::Receiver<TaggedEnvelope>,
    expected_addr: &HashMap<RoutingAddr, i32>,
    expected_envelope: Envelope,
) {
    let mut received_addr = HashMap::new();
    for _ in 0..count {
        let TaggedEnvelope(addr, env) = router_rx.recv().await.expect("Channel closed");
        *received_addr.entry(addr).or_insert(0) += 1;
        assert_eq!(env, expected_envelope);
    }

    assert_eq!(received_addr, *expected_addr);
}

async fn expect_envelope(
    router_rx: &mut mpsc::Receiver<TaggedEnvelope>,
    expected_addr: RoutingAddr,
    expected_envelope: Envelope,
) {
    let TaggedEnvelope(addr, env) = router_rx.recv().await.expect("Channel closed");
    assert_eq!(addr, expected_addr);
    assert_eq!(env, expected_envelope);
}

#[tokio::test]
async fn handle_action_lane_link_request() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn handle_action_lane_sync_request() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_sync(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::synced(&route.node, &route.lane),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn handle_action_lane_immediate_unlink_request() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_unlink(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;

        drop(input);

        router_rx
    };

    let (_, result, mut router_rx) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));

    assert!(router_rx.recv().now_or_never().and_then(identity).is_none());
}

#[tokio::test]
async fn action_lane_responses_when_linked() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_command(addr, 2).await;

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::make_event(&route.node, &route.lane, Some(4.into())),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn action_lane_multiple_links() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_link(addr2).await;
        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_command(addr1, 2).await;

        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::make_event(&route.node, &route.lane, Some(4.into())),
        )
        .await;

        input.send_command(addr2, 3).await;

        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::make_event(&route.node, &route.lane, Some(6.into())),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked(&route.node, &route.lane);

        let TaggedEnvelope(rec_addr1, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr1 == addr1 || rec_addr1 == addr2);
        assert_eq!(env, expected_unlink);

        let TaggedEnvelope(rec_addr2, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr2 == addr1 || rec_addr2 == addr2);
        assert_ne!(rec_addr1, rec_addr2);
        assert_eq!(env, expected_unlink);
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn handle_action_lane_update_failure() {
    let (context, _router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let input_ref = &mut input;

    let io_task = async move {
        input_ref.send_raw(addr, Value::text("0")).await;
    };

    let (_, result, _) = join3(spawn_task, timeout(Duration::from_secs(5), task), io_task).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn handle_command_lane_update_failure() {
    let (context, _router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_command_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let input_ref = &mut input;

    let io_task = async move {
        input_ref.send_raw(addr, Value::text("0")).await;
    };

    let (_, result, _) = join3(spawn_task, timeout(Duration::from_secs(5), task), io_task).await;
    assert!(result.is_err());
}

#[derive(Debug)]
struct RouteReceiver {
    receiver: Option<mpsc::Receiver<TaggedEnvelope>>,
    _drop_tx: promise::Sender<ConnectionDropped>,
}

impl RouteReceiver {
    fn new(
        receiver: mpsc::Receiver<TaggedEnvelope>,
        drop_tx: promise::Sender<ConnectionDropped>,
    ) -> Self {
        RouteReceiver {
            receiver: Some(receiver),
            _drop_tx: drop_tx,
        }
    }

    fn taken(drop_tx: promise::Sender<ConnectionDropped>) -> Self {
        RouteReceiver {
            receiver: None,
            _drop_tx: drop_tx,
        }
    }
}

#[derive(Debug)]
struct MultiTestContextInner {
    router_addr: RoutingAddr,
    senders: HashMap<RoutingAddr, Route>,
    receivers: HashMap<RoutingAddr, RouteReceiver>,
}

impl MultiTestContextInner {
    fn new(router_addr: RoutingAddr) -> Self {
        MultiTestContextInner {
            router_addr,
            senders: HashMap::new(),
            receivers: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct MultiTestContext(
    Arc<parking_lot::Mutex<MultiTestContextInner>>,
    mpsc::Sender<Eff>,
    RelativeUri,
    Arc<AtomicInstant>,
);

impl MultiTestContext {
    fn new(router_addr: RoutingAddr, spawner: mpsc::Sender<Eff>) -> Self {
        MultiTestContext(
            Arc::new(parking_lot::Mutex::new(MultiTestContextInner::new(
                router_addr,
            ))),
            spawner,
            RelativeUri::try_from("/mock/router".to_string()).unwrap(),
            Arc::new(AtomicInstant::new(Instant::now())),
        )
    }

    fn take_receiver(&self, addr: RoutingAddr) -> Option<mpsc::Receiver<TaggedEnvelope>> {
        let mut lock = self.0.lock();
        if lock.senders.contains_key(&addr) {
            lock.receivers
                .get_mut(&addr)
                .and_then(|rr| rr.receiver.take())
        } else {
            let (tx, rx) = mpsc::channel(5);
            let (drop_tx, drop_rx) = promise::promise();
            lock.senders
                .insert(addr, Route::new(TaggedSender::new(addr, tx), drop_rx));
            lock.receivers.insert(addr, RouteReceiver::taken(drop_tx));
            Some(rx)
        }
    }
}

#[derive(Debug, Clone)]
struct MultiTestRouter(Arc<parking_lot::Mutex<MultiTestContextInner>>);

impl AgentExecutionContext for MultiTestContext {
    type Router = MultiTestRouter;

    fn router_handle(&self) -> Self::Router {
        MultiTestRouter(self.0.clone())
    }

    fn spawner(&self) -> Sender<Eff> {
        self.1.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.2
    }

    fn metrics(&self) -> NodeMetricAggregator {
        aggregator_sink()
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.3
    }
}

impl Router for MultiTestRouter {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move {
            let mut lock = self.0.lock();
            if let Some(sender) = lock.senders.get(&addr) {
                Ok(sender.clone())
            } else {
                let (tx, rx) = mpsc::channel(5);
                let (drop_tx, drop_rx) = promise::promise();
                let route = Route::new(TaggedSender::new(addr, tx), drop_rx);
                lock.senders.insert(addr, route.clone());
                lock.receivers.insert(addr, RouteReceiver::new(rx, drop_tx));
                Ok(route)
            }
        }
        .boxed()
    }

    fn resolve_bidirectional(
        &mut self,
        _host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        //Todo dm
        unimplemented!()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        panic!("Unexpected resolution attempt.")
    }
}

fn make_multi_context() -> (MultiTestContext, BoxFuture<'static, ()>) {
    let (spawn_tx, spawn_rx) = mpsc::channel(5);

    let context = MultiTestContext::new(RoutingAddr::plane(1024), spawn_tx);
    let spawn_task = ReceiverStream::new(spawn_rx)
        .for_each_concurrent(None, |t| t)
        .boxed();

    (context, spawn_task)
}

#[tokio::test]
async fn handle_action_lane_non_fatal_uplink_error() {
    let route = route();

    let (context, spawn_task) = make_multi_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context.clone());

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let mut router_rx1 = context.take_receiver(addr1).unwrap();
    let mut router_rx2 = context.take_receiver(addr2).unwrap();

    let router_rx2_ref = &mut router_rx2;

    drop(context);

    let io_task = async move {
        input.send_link(addr1).await;
        expect_envelope(
            &mut router_rx1,
            addr1,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        drop(router_rx1);
        input.send_command(addr1, 3).await;

        input.send_link(addr2).await;
        expect_envelope(
            router_rx2_ref,
            addr2,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        drop(input);
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    match result.as_ref().map(|v| v.as_slice()) {
        Ok([report]) => {
            let UplinkErrorReport { error, addr } = report;
            assert_eq!(*addr, addr1);
            assert!(matches!(error, UplinkError::ChannelDropped));
        }
        ow => {
            panic!("Unexpected result {:?}", ow);
        }
    }
}

#[tokio::test]
async fn handle_command_lane_link_request() {
    let route = route();
    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();
    let (task, mut input) = make_command_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked(&route.node, &route.lane),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn command_lane_multiple_links() {
    let route = route();

    let (context, mut router_rx, spawn_task, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_command_lane_task(config, context);

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_link(addr2).await;
        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_command(addr1, 2).await;

        let expected_addr: HashMap<_, _> = [(addr1, 1), (addr2, 1)].iter().cloned().collect();

        expect_broadcast_envelopes(
            2,
            &mut router_rx,
            &expected_addr,
            Envelope::make_event(&route.node, &route.lane, Some(4.into())),
        )
        .await;

        input.send_command(addr2, 3).await;

        expect_broadcast_envelopes(
            2,
            &mut router_rx,
            &expected_addr,
            Envelope::make_event(&route.node, &route.lane, Some(6.into())),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked(&route.node, &route.lane);

        let TaggedEnvelope(rec_addr1, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr1 == addr1 || rec_addr1 == addr2);
        assert_eq!(env, expected_unlink);

        let TaggedEnvelope(rec_addr2, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr2 == addr1 || rec_addr2 == addr2);
        assert_ne!(rec_addr1, rec_addr2);
        assert_eq!(env, expected_unlink);
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

async fn expect_envelopes<I>(
    addrs: I,
    router_rx: &mut mpsc::Receiver<TaggedEnvelope>,
    expected_envelope: Envelope,
) where
    I: IntoIterator<Item = RoutingAddr>,
{
    for addr in addrs {
        expect_envelope(router_rx, addr, expected_envelope.clone()).await;
    }
}

#[tokio::test]
async fn stateless_lane_metrics() {
    let route = route();

    let (context, mut router_rx, spawn_task, metrics) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        input.send_link(addr2).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::linked(&route.node, &route.lane),
        )
        .await;

        input.send_sync(addr1).await;
        input.send_sync(addr2).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::synced(&route.node, &route.lane),
        )
        .await;

        for i in 0..5 {
            let expected = i * 2;
            input.send_command(addr1, i).await;
            input.send_command(addr2, i).await;

            expect_envelopes(
                vec![addr1, addr2],
                &mut router_rx,
                Envelope::make_event(&route.node, &route.lane, Some(expected.into())),
            )
            .await;
        }

        // sleep for long enough that the next message will force the metrics to be flushed.
        sleep(METRIC_SAMPLE_RATE * 2).await;

        input.send_command(addr1, 6).await;
        input.send_command(addr2, 6).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::make_event(&route.node, &route.lane, Some(12.into())),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked(&route.node, &route.lane);

        let TaggedEnvelope(rec_addr1, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr1 == addr1 || rec_addr1 == addr2);
        assert_eq!(env, expected_unlink);

        let TaggedEnvelope(rec_addr2, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr2 == addr1 || rec_addr2 == addr2);
        assert_ne!(rec_addr1, rec_addr2);
        assert_eq!(env, expected_unlink);
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;
    let MetricReceivers { uplink, lane, node } = metrics;

    let uplink_task = accumulate_metrics(uplink);
    let lane_task = accumulate_metrics(lane);
    let node_task = accumulate_metrics(node);

    sleep(METRIC_SAMPLE_RATE).await;
    let (uplink, lane, node) = join3(uplink_task, lane_task, node_task).await;

    assert_eq!(uplink.link_count, 2);
    assert_eq!(uplink.event_count, 10);
    assert_eq!(uplink.command_count, 11);

    assert_eq!(lane.uplink_pulse.link_count, 2);
    assert_eq!(lane.uplink_pulse.event_count, 10);
    assert_eq!(lane.uplink_pulse.command_count, 11);

    assert_eq!(node.uplinks.link_count, 2);
    assert_eq!(node.uplinks.event_count, 10);
    assert_eq!(node.uplinks.command_count, 11);

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn stateful_lane_metrics() {
    let (context, _envelope_rx, spawn_task, metrics) = make_context();
    let config = make_config();
    let (mut inputs, _outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let io_task = async move {
        inputs.send_sync(addr).await;
        inputs.send_command(addr, 87).await;
        inputs.send_unlink(addr).await;
        inputs.send_command(addr, 65).await;

        // sleep for long enough that the next message will force the metrics to be flushed.
        sleep(METRIC_SAMPLE_RATE * 2).await;

        inputs.send_command(addr, 6).await;

        sleep(METRIC_SAMPLE_RATE * 2).await;

        drop(inputs);
        context.stop().await;
    };

    let MetricReceivers { uplink, lane, node } = metrics;
    let uplink_task = accumulate_metrics(uplink);
    let lane_task = accumulate_metrics(lane);
    let node_task = accumulate_metrics(node);

    let left = join3(spawn_task, main_task, io_task);
    let right = join3(uplink_task, lane_task, node_task);
    let (_, (uplink, lane, node)) = join(left, right).await;

    assert_eq!(uplink.event_count, 2);
    assert_eq!(uplink.command_count, 3);

    assert_eq!(lane.uplink_pulse.event_count, 2);
    assert_eq!(lane.uplink_pulse.command_count, 3);

    assert_eq!(node.uplinks.event_count, 2);
    assert_eq!(node.uplinks.command_count, 3);
}
