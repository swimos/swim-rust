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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::{LaneIoError, LaneUplinks, UplinkChannels};
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{
    PeelResult, UplinkAction, UplinkError, UplinkStateMachine,
};
use crate::agent::lane::channels::{
    AgentExecutionConfig, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::agent::lane::model::action::{Action, ActionLane};
use crate::agent::lane::model::command::Command;
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::model::command::Commander;
use crate::agent::model::supply::{into_try_send, SupplyLane};
use crate::agent::{CommandLaneIo, Eff};
use crate::meta::accumulate_metrics;
use futures::future::{join, join3, ready, BoxFuture};
use futures::stream::{once, BoxStream, FusedStream};
use futures::{Future, FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use server_store::agent::mock::MockNodeStore;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::collections::{HashMap, HashSet};
use std::convert::{identity, TryFrom};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::primitive::I32Recognizer;
use swim_form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use swim_form::structural::read::ReadError;
use swim_form::structural::write::{StructuralWritable, StructuralWriter};
use swim_metrics::config::MetricAggregatorConfig;
use swim_metrics::lane::LanePulse;
use swim_metrics::node::NodePulse;
use swim_metrics::uplink::{MetricBackpressureConfig, UplinkObserver, WarpUplinkPulse};
use swim_metrics::{AggregatorError, MetaPulseLanes, NodeMetricAggregator};
use swim_model::path::{Path, RelativePath};
use swim_model::Value;
use swim_runtime::compat::RequestMessage;
use swim_runtime::error::ConnectionDropped;
use swim_runtime::remote::router::fixture::{empty, remote_router_resolver, RouterCallback};
use swim_runtime::remote::router::{PlaneRoutingRequest, TaggedRouter};
use swim_runtime::remote::RawRoute;
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::item_sink::ItemSink;
use swim_utilities::future::item_sink::SendError;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::sync::circular_buffer;
use swim_utilities::sync::topic;
use swim_utilities::time::AtomicInstant;
use swim_utilities::trigger::{self, promise};
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

#[test]
fn lane_io_err_display_update() {
    let route = RelativePath::new("node", "lane");
    let err = LaneIoError::for_update_err(
        route,
        UpdateError::BadEnvelopeBody(ReadError::UnexpectedItem),
    );

    let string = format!("{}", err);
    let lines = string.lines().collect::<Vec<_>>();
    assert_eq!(
        lines,
        vec![
            "IO tasks failed for lane: \"RelativePath[node, lane]\".",
            "- update_error = The body of an incoming envelope was invalid: Unexpected item in record."
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
        UpdateError::BadEnvelopeBody(ReadError::UnexpectedItem),
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
            "- update_error = The body of an incoming envelope was invalid: Unexpected item in record.",
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
        _action_observer: UplinkObserver,
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

impl StructuralWritable for Message {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_i32(self.0)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

struct MessageRecognizer(I32Recognizer);

impl RecognizerReadable for Message {
    type Rec = MessageRecognizer;
    type AttrRec = SimpleAttrBody<MessageRecognizer>;
    type BodyRec = SimpleRecBody<MessageRecognizer>;

    fn make_recognizer() -> Self::Rec {
        MessageRecognizer(I32Recognizer)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

impl Recognizer for MessageRecognizer {
    type Target = Message;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let MessageRecognizer(inner) = self;
        inner.feed_event(input).map(|r| r.map(Message))
    }

    fn reset(&mut self) {
        let MessageRecognizer(inner) = self;
        inner.reset();
    }
}

impl From<Message> for Value {
    fn from(msg: Message) -> Self {
        msg.structure()
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
                                break Err(UpdateError::BadEnvelopeBody(ReadError::UnexpectedItem));
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
    router: TaggedRouter<Path>,
    scheduler: mpsc::Sender<Eff>,
    trigger: Arc<Mutex<Option<trigger::Sender>>>,
    _drop_tx: Arc<promise::Sender<ConnectionDropped>>,
    drop_rx: promise::Receiver<ConnectionDropped>,
    uri: RelativeUri,
    aggregator: NodeMetricAggregator,
    uplinks_idle_since: Arc<AtomicInstant>,
}

impl TestContext {
    async fn stop(&self) {
        if let Some(tx) = self.trigger.lock().await.take() {
            tx.trigger();
        }
    }
}

#[derive(Clone, Debug)]
struct TestSender {
    addr: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl<'a> ItemSink<'a, Envelope> for TestSender {
    type Error = SendError<Envelope>;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        let tagged = TaggedEnvelope(self.addr, value);
        async move {
            self.inner.send(tagged).await.map_err(|err| {
                let TaggedEnvelope(_, envelope) = err.0;
                SendError(envelope)
            })
        }
        .boxed()
    }
}

struct TestRouter {
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl RouterCallback<PlaneRoutingRequest> for TestRouter {
    fn call(&mut self, arg: PlaneRoutingRequest) -> BoxFuture<()> {
        match arg {
            PlaneRoutingRequest::Endpoint { request, .. } => {
                let TestRouter {
                    sender, drop_rx, ..
                } = self;

                let _ = request.send(Ok(RawRoute::new(sender.clone(), drop_rx.clone())));
            }
            req => {
                panic!("Unexpected request: {:?}", req)
            }
        }

        ready(()).boxed()
    }
}

impl AgentExecutionContext for TestContext {
    type Store = SwimNodeStore<MockPlaneStore>;

    fn router_handle(&self) -> TaggedRouter<Path> {
        self.router.clone()
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

    fn store(&self) -> Self::Store {
        MockNodeStore::mock()
    }
}

fn default_buffer() -> NonZeroUsize {
    non_zero_usize!(5)
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
    envelope_tx: mpsc::Sender<RequestMessage<Value>>,
    _event_tx: Option<topic::Sender<i32>>,
}

impl TaskInput {
    async fn send_link(&mut self, addr: RoutingAddr) {
        let env = RequestMessage::link(addr, RelativePath::new("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_sync(&mut self, addr: RoutingAddr) {
        let env = RequestMessage::sync(addr, RelativePath::new("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_unlink(&mut self, addr: RoutingAddr) {
        let env = RequestMessage::unlink(addr, RelativePath::new("node", "lane"));
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_command(&mut self, addr: RoutingAddr, value: i32) {
        let env = RequestMessage::command(addr, RelativePath::new("node", "lane"), value.into());
        assert!(self.envelope_tx.send(env).await.is_ok())
    }

    async fn send_raw(&mut self, addr: RoutingAddr, value: Value) {
        let env = RequestMessage::command(addr, RelativePath::new("node", "lane"), value);
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
    let (event_tx, event_rx) = topic::channel(non_zero_usize!(5));

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
    uplinks.insert(path.clone(), into_try_send(lane));

    let (lane_tx, lane_rx) = mpsc::channel(buffer_size);
    let lane = SupplyLane::new(lane_tx);
    let mut lanes = HashMap::new();
    lanes.insert(path.clone(), into_try_send(lane));

    let (node_tx, node_rx) = mpsc::channel(buffer_size);
    let node = into_try_send(SupplyLane::new(node_tx));

    let receivers = MetricReceivers {
        uplink: uplink_rx,
        lane: lane_rx,
        node: node_rx,
    };

    let lanes = MetaPulseLanes {
        uplinks,
        lanes,
        node,
    };

    let RelativePath { node, lane } = path;
    let uri = RelativeUri::try_from(format!("/{}/{}", node, lane)).unwrap();

    let config = MetricAggregatorConfig {
        sample_rate: Duration::from_secs(u64::MAX),
        buffer_size: non_zero_usize!(10),
        yield_after: non_zero_usize!(256),
        backpressure_config: MetricBackpressureConfig {
            buffer_size: non_zero_usize!(2),
            yield_after: non_zero_usize!(256),
            bridge_buffer_size: non_zero_usize!(4),
            cache_size: non_zero_usize!(4),
        },
    };

    let (aggregator, task) = NodeMetricAggregator::new(uri, stop_rx, config, lanes);

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
    impl Future<Output = Result<(), AggregatorError>>,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(5);
    let (router_tx, router_rx) = mpsc::channel(5);
    let (stop_tx, stop_rx) = trigger::trigger();

    let (drop_tx, drop_rx) = promise::promise();
    let (aggregator, aggregator_task, receivers) = make_aggregator(stop_rx.clone());
    let (router, _jh) = remote_router_resolver(router_tx, drop_rx.clone());

    let context = TestContext {
        router: router.untagged(),
        scheduler: spawn_tx,
        trigger: Arc::new(Mutex::new(Some(stop_tx))),
        _drop_tx: Arc::new(drop_tx),
        drop_rx,
        uri: RelativeUri::try_from("/mock/router".to_string()).unwrap(),
        aggregator,
        uplinks_idle_since: Arc::new(AtomicInstant::new(Instant::now().into_std())),
    };
    let spawn_task = ReceiverStream::new(spawn_rx)
        .take_until(stop_rx)
        .for_each_concurrent(None, |t| t)
        .boxed();

    (context, router_rx, spawn_task, receivers, aggregator_task)
}

#[tokio::test]
async fn handle_link_request() {
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
                Some(UpdateError::BadEnvelopeBody(ReadError::UnexpectedItem))
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
    let (context, _envelope_rx, spawn_task, _, _) = make_context();
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
                Some(UpdateError::BadEnvelopeBody(ReadError::UnexpectedItem))
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
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(5);

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
    let (commander_tx, mut commander_rx) = mpsc::channel::<Command<i32>>(5);
    let (mut commands_tx, commands_rx) = circular_buffer::channel(non_zero_usize!(8));

    let mock_lifecycle = async move {
        while let Some(Command { command, responder }) = commander_rx.recv().await {
            if let Some(responder) = responder {
                assert!(responder.trigger());
            }
            assert!(commands_tx.try_send(command).is_ok());
        }
        let _ = commands_tx;
    };
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(5);

    let lane_io: CommandLaneIo<i32> = CommandLaneIo::new(Commander(commander_tx), commands_rx);

    let task = super::run_command_lane_io(
        lane_io,
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

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn handle_action_lane_sync_request() {
    let route = route();

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_sync(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::synced()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn handle_action_lane_immediate_unlink_request() {
    let route = route();

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_unlink(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
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

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_command(addr, 2).await;

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(4)
                .done(),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn action_lane_multiple_links() {
    let route = route();

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_action_lane_task(config, context);

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_link(addr2).await;
        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_command(addr1, 2).await;

        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(4)
                .done(),
        )
        .await;

        input.send_command(addr2, 3).await;

        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(6)
                .done(),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked()
            .node_uri(&route.node)
            .lane_uri(&route.lane)
            .done();

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
    let (context, _router_rx, spawn_task, _, _) = make_context();
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
    let (context, _router_rx, spawn_task, _, _) = make_context();
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
pub struct MultiTestContextInner {
    router_addr: RoutingAddr,
    senders: HashMap<RoutingAddr, RawRoute>,
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
    TaggedRouter<Path>,
    Arc<parking_lot::Mutex<MultiTestContextInner>>,
    mpsc::Sender<Eff>,
    RelativeUri,
    Arc<AtomicInstant>,
);

impl MultiTestContext {
    fn new(router_addr: RoutingAddr, spawner: mpsc::Sender<Eff>) -> Self {
        let context_inner = Arc::new(parking_lot::Mutex::new(MultiTestContextInner::new(
            router_addr,
        )));

        let (router, _task) = router::remote(context_inner.clone());

        MultiTestContext(
            router.untagged(),
            context_inner,
            spawner,
            RelativeUri::try_from("/mock/router".to_string()).unwrap(),
            Arc::new(AtomicInstant::new(Instant::now().into_std())),
        )
    }

    fn take_receiver(&self, addr: RoutingAddr) -> Option<mpsc::Receiver<TaggedEnvelope>> {
        let mut lock = self.1.lock();
        if let std::collections::hash_map::Entry::Vacant(e) = lock.senders.entry(addr) {
            let (tx, rx) = mpsc::channel(5);
            let (drop_tx, drop_rx) = promise::promise();

            e.insert(RawRoute::new(tx, drop_rx));
            lock.receivers.insert(addr, RouteReceiver::taken(drop_tx));
            Some(rx)
        } else {
            lock.receivers
                .get_mut(&addr)
                .and_then(|rr| rr.receiver.take())
        }
    }
}

impl AgentExecutionContext for MultiTestContext {
    type Store = SwimNodeStore<MockPlaneStore>;

    fn router_handle(&self) -> TaggedRouter<Path> {
        self.0.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.2.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.3
    }

    fn metrics(&self) -> NodeMetricAggregator {
        NodeMetricAggregator::new(
            RelativeUri::try_from("/test").unwrap(),
            trigger::trigger().1,
            MetricAggregatorConfig::default(),
            MetaPulseLanes {
                uplinks: Default::default(),
                lanes: Default::default(),
                node: Box::new(SupplyLane::new(mpsc::channel(1).0)),
            },
        )
        .0
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.4
    }

    fn store(&self) -> Self::Store {
        MockNodeStore::mock()
    }
}

mod router {
    use crate::agent::lane::channels::task::tests::{MultiTestContextInner, RouteReceiver};
    use futures_util::future::BoxFuture;
    use std::sync::Arc;
    use swim_model::path::Path;
    use swim_runtime::remote::router::fixture::{invalid, router_fixture, RouterCallback};
    use swim_runtime::remote::router::{PlaneRoutingRequest, RemoteRoutingRequest, Router};
    use swim_runtime::remote::RawRoute;
    use swim_utilities::trigger::promise;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    pub fn remote(
        inner: Arc<parking_lot::Mutex<MultiTestContextInner>>,
    ) -> (Router<Path>, JoinHandle<()>) {
        router_fixture(invalid, RemoteRouter(inner), invalid)
    }

    struct RemoteRouter(Arc<parking_lot::Mutex<MultiTestContextInner>>);

    impl RouterCallback<RemoteRoutingRequest> for RemoteRouter {
        fn call(&mut self, request: RemoteRoutingRequest) -> BoxFuture<()> {
            println!("Router callback");

            let inner = self.0.clone();

            Box::pin(async move {
                let mut lock = inner.lock();

                match request {
                    RemoteRoutingRequest::Endpoint { addr, request } => {
                        let result = if let Some(sender) = lock.senders.get(&addr) {
                            Ok(sender.clone())
                        } else {
                            let (tx, rx) = mpsc::channel(5);
                            let (drop_tx, drop_rx) = promise::promise();
                            let route = RawRoute::new(tx, drop_rx);
                            lock.senders.insert(addr, route.clone());
                            lock.receivers.insert(addr, RouteReceiver::new(rx, drop_tx));
                            Ok(route)
                        };

                        let _ = request.send(result);
                    }
                    req => panic!("Unexpected request: {:?}", req),
                }
            })
        }
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
        println!("Sending link");

        input.send_link(addr1).await;

        println!("Sent link");

        expect_envelope(
            &mut router_rx1,
            addr1,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        println!("Got env");

        drop(router_rx1);

        println!("Sending command");

        input.send_command(addr1, 3).await;

        println!("Sending link 2");

        input.send_link(addr2).await;

        println!("Sent link 2");

        expect_envelope(
            router_rx2_ref,
            addr2,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        println!("Got env 2");

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
    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();
    let (task, mut input) = make_command_lane_task(config, context);

    let addr = RoutingAddr::remote(5);

    let io_task = async move {
        input.send_link(addr).await;
        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        drop(input);

        expect_envelope(
            &mut router_rx,
            addr,
            Envelope::unlinked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;
    };

    let (_, result, _) = join3(spawn_task, task, io_task).await;

    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn command_lane_multiple_links() {
    let route = route();

    let (context, mut router_rx, spawn_task, _, _) = make_context();
    let config = make_config();

    let (task, mut input) = make_command_lane_task(config, context);

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        expect_envelope(
            &mut router_rx,
            addr1,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_link(addr2).await;
        expect_envelope(
            &mut router_rx,
            addr2,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_command(addr1, 2).await;

        let expected_addr: HashMap<_, _> = [(addr1, 1), (addr2, 1)].iter().cloned().collect();

        expect_broadcast_envelopes(
            2,
            &mut router_rx,
            &expected_addr,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(2)
                .done(),
        )
        .await;

        input.send_command(addr2, 3).await;

        expect_broadcast_envelopes(
            2,
            &mut router_rx,
            &expected_addr,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(3)
                .done(),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked()
            .node_uri(&route.node)
            .lane_uri(&route.lane)
            .done();

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

    let (context, mut router_rx, spawn_task, metrics, metric_task) = make_context();
    let config = make_config();

    let (main_task, mut input) = make_action_lane_task(config, context.clone());

    let addr1 = RoutingAddr::remote(5);
    let addr2 = RoutingAddr::remote(10);

    let io_task = async move {
        input.send_link(addr1).await;
        input.send_link(addr2).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::linked()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        input.send_sync(addr1).await;
        input.send_sync(addr2).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::synced()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .done(),
        )
        .await;

        for i in 0..5 {
            let expected = i * 2;
            input.send_command(addr1, i).await;
            input.send_command(addr2, i).await;

            expect_envelopes(
                vec![addr1, addr2],
                &mut router_rx,
                Envelope::event()
                    .node_uri(&route.node)
                    .lane_uri(&route.lane)
                    .body(expected)
                    .done(),
            )
            .await;
        }

        input.send_command(addr1, 6).await;
        input.send_command(addr2, 6).await;

        expect_envelopes(
            vec![addr1, addr2],
            &mut router_rx,
            Envelope::event()
                .node_uri(&route.node)
                .lane_uri(&route.lane)
                .body(12)
                .done(),
        )
        .await;

        drop(input);

        let expected_unlink = Envelope::unlinked()
            .node_uri(&route.node)
            .lane_uri(&route.lane)
            .done();

        let TaggedEnvelope(rec_addr1, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr1 == addr1 || rec_addr1 == addr2);
        assert_eq!(env, expected_unlink);

        let TaggedEnvelope(rec_addr2, env) = router_rx.recv().await.expect("Channel closed");
        assert!(rec_addr2 == addr1 || rec_addr2 == addr2);
        assert_ne!(rec_addr1, rec_addr2);
        assert_eq!(env, expected_unlink);
    };

    let MetricReceivers { uplink, lane, node } = metrics;
    let uplink_task = accumulate_metrics(uplink);
    let lane_task = accumulate_metrics(lane);
    let node_task = accumulate_metrics(node);

    let spawn_handle = tokio::spawn(spawn_task);
    let metric_handle = tokio::spawn(metric_task);
    let main_handle = tokio::spawn(main_task);

    let (uplink, lane, node) = io_task
        .then(|_| async move {
            let task_result = main_handle.await.unwrap();
            assert!(matches!(task_result, Ok(errs) if errs.is_empty()));

            context.stop().await;
            assert!(spawn_handle.await.is_ok());
            assert!(metric_handle.await.is_ok());
            join3(uplink_task, lane_task, node_task).await
        })
        .await;

    assert_eq!(uplink.link_count, 2);
    assert_eq!(uplink.event_count, 12);
    assert_eq!(uplink.command_count, 12);

    assert_eq!(lane.uplink_pulse.link_count, 2);
    assert_eq!(lane.uplink_pulse.event_count, 12);
    assert_eq!(lane.uplink_pulse.command_count, 12);

    assert_eq!(node.uplinks.link_count, 2);
    assert_eq!(node.uplinks.event_count, 12);
    assert_eq!(node.uplinks.command_count, 12);
}

#[tokio::test]
async fn stateful_lane_metrics() {
    let (context, _envelope_rx, spawn_task, metrics, metric_task) = make_context();
    let config = make_config();
    let (mut inputs, _outputs, main_task) = make_task(vec![], false, config, context.clone());

    let addr = RoutingAddr::remote(4);

    let io_task = async move {
        inputs.send_sync(addr).await;
        inputs.send_command(addr, 87).await;
        inputs.send_unlink(addr).await;
        inputs.send_command(addr, 65).await;
        inputs.send_command(addr, 6).await;

        drop(inputs);
    };

    let MetricReceivers { uplink, lane, node } = metrics;
    let uplink_task = accumulate_metrics(uplink);
    let lane_task = accumulate_metrics(lane);
    let node_task = accumulate_metrics(node);

    let spawn_handle = tokio::spawn(spawn_task);
    let metric_handle = tokio::spawn(metric_task);

    let (uplink, lane, node) = io_task
        .then(|_| async move {
            assert!(main_task.await.is_ok());
            context.stop().await;
            assert!(spawn_handle.await.is_ok());
            assert!(metric_handle.await.is_ok());
            join3(uplink_task, lane_task, node_task).await
        })
        .await;

    assert_eq!(uplink.event_count, 3);
    assert_eq!(uplink.command_count, 3);

    assert_eq!(lane.uplink_pulse.event_count, 3);
    assert_eq!(lane.uplink_pulse.command_count, 3);

    assert_eq!(node.uplinks.event_count, 3);
    assert_eq!(node.uplinks.command_count, 3);
}
