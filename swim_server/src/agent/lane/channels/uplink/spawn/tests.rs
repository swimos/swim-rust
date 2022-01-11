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
use crate::agent::lane::channels::task::{LaneUplinks, UplinkChannels};
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::channels::uplink::spawn::{SpawnerUplinkFactory, UplinkErrorReport};
use crate::agent::lane::channels::uplink::{
    PeelResult, UplinkAction, UplinkError, UplinkStateMachine,
};
use crate::agent::lane::channels::{AgentExecutionConfig, LaneMessageHandler, TaggedAction};
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::Eff;
use futures::future::{join, join3, BoxFuture};
use futures::stream::iter;
use futures::stream::{BoxStream, FusedStream};
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use server_store::agent::mock::MockNodeStore;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_form::structural::read::ReadError;
use swim_form::Form;
use swim_metrics::config::MetricAggregatorConfig;
use swim_metrics::{MetaPulseLanes, NodeMetricAggregator};
use swim_model::path::{Path, RelativePath};
use swim_model::Value;
use swim_runtime::error::ConnectionDropped;
use swim_runtime::remote::router::fixture::{plane_router_resolver, remote_router_resolver};
use swim_runtime::remote::router::TaggedRouter;
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::item_sink::ItemSink;
use swim_utilities::future::item_sink::SendError;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::sync::topic;
use swim_utilities::time::AtomicInstant;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Barrier};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

const INIT: i32 = 42;

#[derive(Debug, Form)]
struct Message(i32);

//A minimal suite of fake uplink and router implementations which which to test the spawner.

struct TestHandler(mpsc::Sender<i32>, i32);

struct TestStateMachine(i32);

struct TestUpdater(mpsc::Sender<i32>);

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

impl LaneMessageHandler for TestHandler {
    type Event = i32;
    type Uplink = TestStateMachine;
    type Update = TestUpdater;

    fn make_uplink(&self, _addr: RoutingAddr) -> Self::Uplink {
        TestStateMachine(self.1)
    }

    fn make_update(&self) -> Self::Update {
        TestUpdater(self.0.clone())
    }
}

impl From<Message> for Value {
    fn from(msg: Message) -> Self {
        Value::Int32Value(msg.0)
    }
}

impl UplinkStateMachine<i32> for TestStateMachine {
    type Msg = Message;

    fn message_for(&self, event: i32) -> Result<Option<Self::Msg>, UplinkError> {
        if event >= 0 {
            Ok(Some(Message(event)))
        } else {
            Err(UplinkError::InconsistentForm(ReadError::UnexpectedItem))
        }
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = i32> + Send + Unpin + 'a,
    {
        let TestStateMachine(n) = self;
        iter(
            vec![
                PeelResult::Output(Ok(Message(*n))),
                PeelResult::Complete(updates),
            ]
            .into_iter(),
        )
        .boxed()
    }
}

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
        let TestUpdater(tx) = self;

        async move {
            pin_mut!(messages);
            while let Some(Ok((_, Message(n)))) = messages.next().await {
                if tx.send(n).await.is_err() {
                    break;
                }
            }
            Ok(())
        }
        .boxed()
    }
}

fn default_buffer() -> NonZeroUsize {
    non_zero_usize!(5)
}

fn route() -> RelativePath {
    RelativePath::new("node", "lane")
}

struct UplinkSpawnerInputs {
    action_tx: Option<mpsc::Sender<TaggedAction>>,
    event_tx: topic::Sender<i32>,
}

impl UplinkSpawnerInputs {
    async fn action(&mut self, addr: RoutingAddr, action: UplinkAction) {
        if let Some(action_tx) = &mut self.action_tx {
            assert!(action_tx.send(TaggedAction(addr, action)).await.is_ok())
        }
    }

    async fn generate_event(&mut self, event: i32) {
        assert!(self.event_tx.send(event).await.is_ok())
    }

    fn drop_action_tx(&mut self) {
        self.action_tx = None
    }
}

impl UplinkSpawnerOutputs {
    async fn take_router_events(&mut self, n: usize) -> Vec<TaggedEnvelope> {
        tokio::time::timeout(
            Duration::from_secs(1),
            (&mut self.router_rx).take(n).collect::<Vec<_>>(),
        )
        .await
        .expect("Timeout awaiting outputs.")
    }

    fn split(self, expected: HashSet<RoutingAddr>) -> (UplinkSpawnerSplitOutputs, Eff) {
        let UplinkSpawnerOutputs {
            _update_rx,
            mut router_rx,
        } = self;
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        for addr in expected.iter() {
            let (tx, rx) = mpsc::channel(5);
            txs.insert(*addr, tx);
            rxs.insert(*addr, rx);
        }
        let task = async move {
            while let Some(TaggedEnvelope(addr, envelope)) = router_rx.next().await {
                if let Some(tx) = txs.get_mut(&addr) {
                    assert!(tx.send(envelope).await.is_ok());
                } else {
                    panic!("Unexpected address: {}", addr);
                }
            }
        };
        (
            UplinkSpawnerSplitOutputs {
                _update_rx,
                router_rxs: rxs,
            },
            task.boxed(),
        )
    }
}

struct UplinkSpawnerOutputs {
    _update_rx: mpsc::Receiver<i32>,
    router_rx: ReceiverStream<TaggedEnvelope>,
}

struct UplinkSpawnerSplitOutputs {
    _update_rx: mpsc::Receiver<i32>,
    router_rxs: HashMap<RoutingAddr, mpsc::Receiver<Envelope>>,
}

struct RouterChannel(ReceiverStream<Envelope>);

impl RouterChannel {
    async fn take_router_events(&mut self, n: usize) -> Vec<Envelope> {
        tokio::time::timeout(
            Duration::from_secs(1),
            (&mut self.0).take(n).collect::<Vec<_>>(),
        )
        .await
        .expect("Timeout awaiting outputs.")
    }
}

impl UplinkSpawnerSplitOutputs {
    pub fn take_addr(&mut self, addr: RoutingAddr) -> RouterChannel {
        RouterChannel(ReceiverStream::new(self.router_rxs.remove(&addr).unwrap()))
    }
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

struct TestContext {
    router: TaggedRouter<Path>,
    spawner: mpsc::Sender<Eff>,
    _drop_tx: promise::Sender<ConnectionDropped>,
    uri: RelativeUri,
    uplinks_idle_since: Arc<AtomicInstant>,
}

impl TestContext {
    fn new(spawner: mpsc::Sender<Eff>, messages: mpsc::Sender<TaggedEnvelope>) -> Self {
        let (drop_tx, drop_rx) = promise::promise();
        let (router, _jh) = remote_router_resolver(messages, drop_rx);

        TestContext {
            router: router.untagged(),
            spawner,
            _drop_tx: drop_tx,
            uri: RelativeUri::try_from("/mock/router".to_string()).unwrap(),
            uplinks_idle_since: Arc::new(AtomicInstant::new(Instant::now().into_std())),
        }
    }
}

impl AgentExecutionContext for TestContext {
    type Store = SwimNodeStore<MockPlaneStore>;

    fn router_handle(&self) -> TaggedRouter<Path> {
        self.router.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.spawner.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.uri
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
        &self.uplinks_idle_since
    }

    fn store(&self) -> Self::Store {
        MockNodeStore::mock()
    }
}

/// Create a spawner connected to a complete test harness.
fn make_test_harness() -> (
    UplinkSpawnerInputs,
    UplinkSpawnerOutputs,
    BoxFuture<'static, Vec<UplinkErrorReport>>,
) {
    let (tx_up, rx_up) = mpsc::channel(5);
    let (tx_event, rx_event) = topic::channel(non_zero_usize!(5));
    let (tx_act, rx_act) = mpsc::channel(5);
    let (tx_router, rx_router) = mpsc::channel(5);

    let (spawn_tx, spawn_rx) = mpsc::channel(5);
    let spawn_task = ReceiverStream::new(spawn_rx).for_each_concurrent(None, |task| task);

    let (error_tx, error_rx) = mpsc::channel(5);
    let error_task = ReceiverStream::new(error_rx).collect::<Vec<_>>();

    let handler = Arc::new(TestHandler(tx_up, INIT));

    let factory = SpawnerUplinkFactory(make_config());

    let channels = UplinkChannels::new(rx_event.subscriber(), rx_act, error_tx);

    let context = TestContext::new(spawn_tx, tx_router);
    let observer = context.metrics().uplink_observer_for_path(route());

    let spawner_task = factory.make_task(handler, channels, route(), &context, observer);

    let errs = join3(spawn_task, spawner_task, error_task)
        .map(|(_, _, errs)| errs)
        .boxed();

    (
        UplinkSpawnerInputs {
            event_tx: tx_event,
            action_tx: Some(tx_act),
        },
        UplinkSpawnerOutputs {
            _update_rx: rx_up,
            router_rx: ReceiverStream::new(rx_router),
        },
        errs,
    )
}

#[tokio::test]
async fn link_to_lane() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Link).await;

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::linked().node_uri("node").lane_uri("lane").done()
            )]
        );

        inputs.drop_action_tx();

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn immediate_unlink() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Unlink).await;

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

fn event_envelope(n: i32) -> Envelope {
    Envelope::event()
        .node_uri("node")
        .lane_uri("lane")
        .body(n)
        .done()
}

#[tokio::test]
async fn receive_event() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Link).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::linked().node_uri("node").lane_uri("lane").done()
            )]
        );
        inputs.generate_event(13).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(addr, event_envelope(13))]
        );

        inputs.drop_action_tx();

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn sync_with_lane() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Sync).await;

        assert_eq!(
            outputs.take_router_events(3).await,
            vec![
                TaggedEnvelope(
                    addr,
                    Envelope::linked().node_uri("node").lane_uri("lane").done()
                ),
                TaggedEnvelope(addr, event_envelope(INIT)),
                TaggedEnvelope(
                    addr,
                    Envelope::synced().node_uri("node").lane_uri("lane").done()
                )
            ]
        );

        inputs.drop_action_tx();

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn receive_event_after_sync() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Sync).await;

        assert_eq!(
            outputs.take_router_events(3).await,
            vec![
                TaggedEnvelope(
                    addr,
                    Envelope::linked().node_uri("node").lane_uri("lane").done()
                ),
                TaggedEnvelope(addr, event_envelope(INIT)),
                TaggedEnvelope(
                    addr,
                    Envelope::synced().node_uri("node").lane_uri("lane").done()
                )
            ]
        );

        inputs.generate_event(13).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(addr, event_envelope(13))]
        );

        inputs.drop_action_tx();

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn relink_for_same_addr() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Link).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::linked().node_uri("node").lane_uri("lane").done()
            )]
        );

        inputs.action(addr, UplinkAction::Unlink).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );

        inputs.action(addr, UplinkAction::Link).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::linked().node_uri("node").lane_uri("lane").done()
            )]
        );

        inputs.drop_action_tx();

        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn sync_lane_twice() {
    let (mut inputs, outputs, spawn_task) = make_test_harness();

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let mut addrs = HashSet::new();
    addrs.insert(addr1);
    addrs.insert(addr2);

    let (mut split_outputs, split_task) = outputs.split(addrs);

    let mut outputs1 = split_outputs.take_addr(addr1);
    let mut outputs2 = split_outputs.take_addr(addr2);

    let barrier1 = Arc::new(Barrier::new(3));
    let barrier2 = barrier1.clone();
    let barrier3 = barrier1.clone();

    let inputs_task = async move {
        inputs.action(addr1, UplinkAction::Sync).await;
        inputs.action(addr2, UplinkAction::Sync).await;
        barrier1.wait().await;
        inputs.drop_action_tx();
        inputs
    };

    let io_task1 = async move {
        assert_eq!(
            outputs1.take_router_events(3).await,
            vec![
                Envelope::linked().node_uri("node").lane_uri("lane").done(),
                event_envelope(INIT),
                Envelope::synced().node_uri("node").lane_uri("lane").done()
            ]
        );

        barrier2.wait().await;

        assert_eq!(
            outputs1.take_router_events(1).await,
            vec![Envelope::unlinked()
                .node_uri("node")
                .lane_uri("lane")
                .done()]
        );
    };

    let io_task2 = async move {
        assert_eq!(
            outputs2.take_router_events(3).await,
            vec![
                Envelope::linked().node_uri("node").lane_uri("lane").done(),
                event_envelope(INIT),
                Envelope::synced().node_uri("node").lane_uri("lane").done()
            ]
        );

        barrier3.wait().await;

        assert_eq!(
            outputs2.take_router_events(1).await,
            vec![Envelope::unlinked()
                .node_uri("node")
                .lane_uri("lane")
                .done()]
        );
    };

    let ((_inputs, _, _), _, errs) = join3(
        join3(inputs_task, io_task1, io_task2),
        split_task,
        spawn_task,
    )
    .await;
    assert!(errs.is_empty());
}

#[tokio::test]
async fn uplink_failure() {
    let (mut inputs, mut outputs, spawn_task) = make_test_harness();

    let addr = RoutingAddr::remote(1);

    let io_task = async move {
        inputs.action(addr, UplinkAction::Link).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::linked().node_uri("node").lane_uri("lane").done()
            )]
        );
        inputs.generate_event(-1).await;
        assert_eq!(
            outputs.take_router_events(1).await,
            vec![TaggedEnvelope(
                addr,
                Envelope::unlinked()
                    .node_uri("node")
                    .lane_uri("lane")
                    .done()
            )]
        );

        inputs.drop_action_tx();
    };

    let (_, errs) = join(io_task, spawn_task).await;
    assert!(
        matches!(errs.as_slice(), [UplinkErrorReport { error: UplinkError::InconsistentForm(ReadError::UnexpectedItem), addr: a }] if *a == addr)
    );
}
