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

#[cfg(feature = "persistence")]
mod store_agent;

mod data_macro_agent;
mod declarive_macro_agent;
mod derive;
mod reporting_agent;
mod reporting_macro_agent;
pub(crate) mod test_clock;

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{
    ActionLaneLifecycle, CommandLaneLifecycle, StatefulLaneLifecycle,
};
use crate::agent::lane::model::action::{Action, ActionLane};
use crate::agent::lane::model::command::{Command, CommandLane};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use crate::agent::lane::LaneModel;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::tests::reporting_agent::TestAgentConfig;
use crate::agent::tests::reporting_macro_agent::ReportingAgentEvent;
use crate::agent::tests::stub_router::SingleChannelRouter;
use crate::agent::tests::test_clock::TestClock;
use crate::agent::{
    ActionLifecycleTasks, AgentContext, AgentParameters, CommandLifecycleTasks, Lane, LaneTasks,
    LifecycleTasks, MapLifecycleTasks, SwimAgent, ValueLifecycleTasks,
};
use crate::interface::ServerDownlinksConfig;
use crate::meta::info::LaneKind;
use crate::meta::log::NodeLogger;
use crate::plane::provider::AgentProvider;
use crate::routing::TopLevelServerRouterFactory;
use futures::future::{join, BoxFuture};
use futures::Stream;
use server_store::agent::mock::MockNodeStore;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use swim_async_runtime::task;
use swim_client::configuration::DownlinkConnectionsConfig;
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_client::router::ClientRouterFactory;
use swim_runtime::task;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_utilities::trigger::Receiver;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;

mod stub_router {
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use std::sync::Arc;
    use swim_runtime::error::ResolutionError;
    use swim_utilities::routing::uri::RelativeUri;
    use swim_utilities::trigger::promise;
    use tokio::sync::mpsc;
    use url::Url;

    #[derive(Clone)]
    pub struct SingleChannelRouter {
        router_addr: RoutingAddr,
        inner: mpsc::Sender<TaggedEnvelope>,
        _drop_tx: Arc<promise::Sender<ConnectionDropped>>,
        drop_rx: promise::Receiver<ConnectionDropped>,
    }

    impl SingleChannelRouter {
        pub(crate) fn new(router_addr: RoutingAddr) -> Self {
            let (tx, rx) = promise::promise();
            let (env_tx, mut env_rx) = mpsc::channel(16);
            tokio::spawn(async move { while let Some(_) = env_rx.recv().await {} });
            SingleChannelRouter {
                router_addr,
                inner: env_tx,
                _drop_tx: Arc::new(tx),
                drop_rx: rx,
            }
        }
    }

    impl Router for SingleChannelRouter {
        fn resolve_sender(
            &mut self,
            addr: RoutingAddr,
        ) -> BoxFuture<Result<Route, ResolutionError>> {
            async move {
                let SingleChannelRouter { inner, drop_rx, .. } = self;
                let route = Route::new(TaggedSender::new(addr, inner.clone()), drop_rx.clone());
                Ok(route)
            }
            .boxed()
        }

        fn lookup(
            &mut self,
            _host: Option<Url>,
            _route: RelativeUri,
        ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
            panic!("Unexpected resolution attempt.")
        }
    }
}

struct TestAgent<Lane> {
    name: &'static str,
    lane: Lane,
}

#[derive(Clone)]
struct TestResults<Lane: LaneModel> {
    start_agent: Option<&'static str>,
    start_model: Option<Lane>,
    event_agent: Option<&'static str>,
    event_model: Option<Lane>,
    events: Vec<Lane::Event>,
}

impl<Lane: LaneModel> Default for TestResults<Lane> {
    fn default() -> Self {
        TestResults {
            start_agent: None,
            start_model: None,
            event_agent: None,
            event_model: None,
            events: vec![],
        }
    }
}

#[derive(Clone)]
struct TestLifecycle<Lane: LaneModel>(Arc<Mutex<TestResults<Lane>>>);

impl<Lane: LaneModel> Default for TestLifecycle<Lane> {
    fn default() -> Self {
        TestLifecycle(Arc::new(Mutex::new(TestResults::default())))
    }
}

impl<'a, Lane> StatefulLaneLifecycle<'a, Lane, TestAgent<Lane>> for TestLifecycle<Lane>
where
    Lane: LaneModel + Send + Sync + 'static,
    Lane: Clone,
    Lane::Event: Clone + Send + Sync + 'static,
{
    type StartFuture = BoxFuture<'a, ()>;
    type EventFuture = BoxFuture<'a, ()>;

    fn on_start<C: AgentContext<TestAgent<Lane>>>(
        &'a self,
        model: &'a Lane,
        context: &'a C,
    ) -> Self::StartFuture
    where
        C: AgentContext<TestAgent<Lane>> + Send + Sync + 'a,
    {
        Box::pin(async move {
            let mut lock = self.0.lock().await;
            assert!(lock.start_model.is_none());
            assert!(lock.start_agent.is_none());
            lock.start_agent = Some(context.agent().name);
            lock.start_model = Some(model.clone());
        })
    }

    fn on_event<C>(
        &'a mut self,
        event: &'a Lane::Event,
        model: &'a Lane,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<TestAgent<Lane>> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let mut lock = self.0.lock().await;
            lock.event_agent = Some(context.agent().name);
            lock.event_model = Some(model.clone());
            lock.events.push(event.clone());
        })
    }
}

impl<'a> ActionLaneLifecycle<'a, String, usize, TestAgent<ActionLane<String, usize>>>
    for TestLifecycle<ActionLane<String, usize>>
{
    type ResponseFuture = BoxFuture<'a, usize>;

    fn on_command<C: AgentContext<TestAgent<ActionLane<String, usize>>>>(
        &'a self,
        command: String,
        model: &'a ActionLane<String, usize>,
        context: &'a C,
    ) -> Self::ResponseFuture
    where
        C: AgentContext<TestAgent<ActionLane<String, usize>>> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let mut lock = self.0.lock().await;
            lock.event_agent = Some(context.agent().name);
            lock.event_model = Some(model.clone());
            lock.events.push(command.clone());
            command.len()
        })
    }
}

impl<'a> CommandLaneLifecycle<'a, String, TestAgent<CommandLane<String>>>
    for TestLifecycle<CommandLane<String>>
{
    type ResponseFuture = BoxFuture<'a, ()>;

    fn on_command<C: AgentContext<TestAgent<CommandLane<String>>>>(
        &'a self,
        command: &'a String,
        model: &'a CommandLane<String>,
        context: &'a C,
    ) -> Self::ResponseFuture
    where
        C: AgentContext<TestAgent<CommandLane<String>>> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let mut lock = self.0.lock().await;
            lock.event_agent = Some(context.agent().name);
            lock.event_model = Some(model.clone());
            lock.events.push(command.clone());
        })
    }
}

struct TestContext<Lane> {
    lane: Arc<TestAgent<Lane>>,
    uri: RelativeUri,
    closed: trigger::Receiver,
}

impl<Lane> TestContext<Lane> {
    fn new(lane: Arc<TestAgent<Lane>>, uri: &str, closed: trigger::Receiver) -> Self {
        TestContext {
            lane,
            uri: uri.parse().unwrap(),
            closed,
        }
    }
}

impl<Lane> AgentContext<TestAgent<Lane>> for TestContext<Lane>
where
    Lane: LaneModel + Send + Sync + 'static,
{
    fn downlinks_context(&self) -> ClientContext<Path> {
        panic!("Unexpected downlink context")
    }

    fn schedule<Effect, Str, Sch>(&self, _effects: Str, _schedule: Sch) -> BoxFuture<'_, ()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        panic!("Unexpected schedule.")
    }

    fn agent(&self) -> &TestAgent<Lane> {
        self.lane.as_ref()
    }

    fn node_uri(&self) -> &RelativeUri {
        &self.uri
    }

    fn agent_stop_event(&self) -> Receiver {
        self.closed.clone()
    }

    fn parameter(&self, _key: &str) -> Option<&String> {
        None
    }

    fn parameters(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn logger(&self) -> NodeLogger {
        panic!("Unexpected log event")
    }
}

fn proj<Lane>() -> impl Fn(&TestAgent<Lane>) -> &Lane {
    |agent: &TestAgent<Lane>| &agent.lane
}

#[tokio::test]
async fn value_lane_start_task() {
    let (_tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<ValueLane<String>> = TestLifecycle::default();

    let tasks = ValueLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    });

    assert_eq!(tasks.kind(), LaneKind::Value);

    assert_eq!(tasks.name(), "lane".to_string());

    let lane = ValueLane::new("".to_string());

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });

    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    tasks.start(&context).await;

    let lock = lifecycle.0.lock().await;

    assert_eq!(lock.start_agent, Some("agent"));
    assert!(matches!(&lock.start_model, Some(l) if ValueLane::same_lane(&l, &lane)));
    assert!(lock.event_agent.is_none());
    assert!(lock.event_model.is_none());
    assert!(lock.events.is_empty());
}

#[tokio::test]
async fn value_lane_events_task() {
    let (tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<ValueLane<String>> = TestLifecycle::default();

    let tasks = Box::new(ValueLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    let lane = ValueLane::new("".to_string());

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let a = Arc::new("a".to_string());
    let b = Arc::new("b".to_string());
    let c = Arc::new("c".to_string());

    let clones = vec![a.clone(), b.clone(), c.clone()];

    let send = async move {
        for x in clones.into_iter() {
            let _ = tx.send(x).await;
        }
        drop(tx);
    };

    join(events, send).await;

    let lock = lifecycle.0.lock().await;

    let expected_first = ValueLaneEvent {
        previous: None,
        current: a.clone(),
    };

    let expected_second = ValueLaneEvent {
        previous: Some(a),
        current: b.clone(),
    };

    let expected_third = ValueLaneEvent {
        previous: Some(b),
        current: c,
    };

    assert_eq!(lock.event_agent, Some("agent"));
    assert!(matches!(&lock.event_model, Some(l) if ValueLane::same_lane(&l, &lane)));
    assert!(lock.start_agent.is_none());
    assert!(lock.start_model.is_none());
    assert!(
        matches!(lock.events.as_slice(), [first, second, third] if expected_first.eq(first) && expected_second.eq(second) && expected_third.eq(third))
    )
}

#[tokio::test]
async fn value_lane_events_task_termination() {
    let (_tx, rx) = mpsc::channel(5);
    let (stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<ValueLane<String>> = TestLifecycle::default();

    let tasks = Box::new(ValueLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    let lane = ValueLane::new("".to_string());

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let event_task = task::spawn(timeout(Duration::from_secs(1), events));
    stop.trigger();

    assert!(event_task.await.is_ok());
}

#[tokio::test]
async fn map_lane_start_task() {
    let (_tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<MapLane<String, String>> = TestLifecycle::default();

    let tasks = MapLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    });

    assert_eq!(tasks.name(), "lane".to_string());

    let lane: MapLane<String, String> = MapLane::new();

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    tasks.start(&context).await;

    let lock = lifecycle.0.lock().await;

    assert_eq!(lock.start_agent, Some("agent"));
    assert!(matches!(&lock.start_model, Some(l) if MapLane::same_lane(&l, &lane)));
    assert!(lock.event_agent.is_none());
    assert!(lock.event_model.is_none());
    assert!(lock.events.is_empty());
}

#[tokio::test]
async fn map_lane_events_task() {
    let (tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<MapLane<String, String>> = TestLifecycle::default();

    let tasks = Box::new(MapLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    assert_eq!(tasks.kind(), LaneKind::Map);

    let lane = MapLane::new();

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let v = Arc::new("v".to_string());
    let clear = MapLaneEvent::Clear;
    let upd = MapLaneEvent::Update("k1".to_string(), v.clone());
    let rem = MapLaneEvent::Remove("k2".to_string());
    let map_events = vec![clear, upd, rem];

    let send = async move {
        for x in map_events.into_iter() {
            let _ = tx.send(x).await;
        }
        drop(tx);
    };

    join(events, send).await;

    let lock = lifecycle.0.lock().await;

    assert_eq!(lock.event_agent, Some("agent"));
    assert!(matches!(&lock.event_model, Some(l) if MapLane::same_lane(&l, &lane)));
    assert!(lock.start_agent.is_none());
    assert!(lock.start_model.is_none());
    assert!(matches!(lock.events.as_slice(), [
        MapLaneEvent::Clear,
        MapLaneEvent::Update(k1, value),
        MapLaneEvent::Remove(k2)
        ] if k1 == &"k1".to_string() && Arc::ptr_eq(value, &v) && k2 == &"k2".to_string()))
}

#[tokio::test]
async fn map_lane_events_task_termination() {
    let (_tx, rx) = mpsc::channel(5);
    let (stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<MapLane<String, String>> = TestLifecycle::default();

    let tasks = Box::new(MapLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    let lane = MapLane::new();

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let event_task = task::spawn(timeout(Duration::from_secs(1), events));
    stop.trigger();

    assert!(event_task.await.is_ok());
}

#[tokio::test]
async fn action_lane_events_task() {
    let (tx_lane, _rx_lane) = mpsc::channel(5);
    let (tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<ActionLane<String, usize>> = TestLifecycle::default();

    let tasks = Box::new(ActionLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    assert_eq!(tasks.kind(), LaneKind::Action);

    assert_eq!(tasks.name(), "lane".to_string());

    let lane = ActionLane::new(tx_lane);

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();

    let clones = vec![a.clone(), b.clone(), c.clone()];

    let send = async move {
        for x in clones.into_iter() {
            let _ = tx.send(Action::forget(x)).await;
        }
        drop(tx);
    };

    join(events, send).await;

    let lock = lifecycle.0.lock().await;

    assert_eq!(lock.event_agent, Some("agent"));
    assert!(matches!(&lock.event_model, Some(l) if ActionLane::same_lane(&l, &lane)));
    assert!(lock.start_agent.is_none());
    assert!(lock.start_model.is_none());
    assert!(
        matches!(lock.events.as_slice(), [a, b, c,] if a == &"a".to_string() && b == &"b".to_string() && c == &"c".to_string())
    )
}

#[tokio::test]
async fn action_lane_events_task_termination() {
    let (tx, rx) = mpsc::channel(5);
    let (stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<ActionLane<String, usize>> = TestLifecycle::default();

    let tasks = Box::new(ActionLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    let lane = ActionLane::new(tx);

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let event_task = task::spawn(timeout(Duration::from_secs(1), events));
    stop.trigger();

    assert!(event_task.await.is_ok());
}

#[tokio::test]
async fn command_lane_events_task() {
    let (tx_lane, _rx_lane) = mpsc::channel(5);
    let (tx, rx) = mpsc::channel(5);
    let (_stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<CommandLane<String>> = TestLifecycle::default();

    let tasks = Box::new(CommandLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    assert_eq!(tasks.kind(), LaneKind::Command);

    assert_eq!(tasks.name(), "lane".to_string());

    let lane = CommandLane::new(tx_lane);

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let a = "a".to_string();
    let b = "b".to_string();
    let c = "c".to_string();

    let clones = vec![a.clone(), b.clone(), c.clone()];

    let send = async move {
        for x in clones.into_iter() {
            let _ = tx.send(Command::forget(x)).await;
        }
        drop(tx);
    };

    join(events, send).await;

    let lock = lifecycle.0.lock().await;

    assert_eq!(lock.event_agent, Some("agent"));
    assert!(matches!(&lock.event_model, Some(l) if CommandLane::same_lane(&l, &lane)));
    assert!(lock.start_agent.is_none());
    assert!(lock.start_model.is_none());
    assert!(
        matches!(lock.events.as_slice(), [a, b, c,] if a == &"a".to_string() && b == &"b".to_string() && c == &"c".to_string())
    )
}

#[tokio::test]
async fn command_lane_events_task_terminates() {
    let (tx, rx) = mpsc::channel(5);
    let (stop, stop_sig) = trigger::trigger();

    let lifecycle: TestLifecycle<CommandLane<String>> = TestLifecycle::default();

    let tasks = Box::new(CommandLifecycleTasks(LifecycleTasks {
        name: "lane".to_string(),
        lifecycle: lifecycle.clone(),
        event_stream: ReceiverStream::new(rx),
        projection: proj(),
    }));

    let lane = CommandLane::new(tx);

    let agent = Arc::new(TestAgent {
        name: "agent",
        lane: lane.clone(),
    });
    let context = TestContext::new(agent.clone(), "/test", stop_sig);

    let events = tasks.events(context);

    let event_task = task::spawn(timeout(Duration::from_secs(1), events));
    stop.trigger();

    assert!(event_task.await.is_ok());
}

#[tokio::test]
async fn agent_loop() {
    let (tx, rx) = mpsc::channel(5);
    let config = TestAgentConfig::new(tx);
    let agent_lifecycle = config.agent_lifecycle();

    let provider = AgentProvider::new(config.clone(), agent_lifecycle);
    run_agent_test(provider, config, rx).await;
}

pub async fn run_agent_test<Agent, Config, Lifecycle>(
    provider: AgentProvider<Agent, Config, Lifecycle>,
    config: Config,
    mut rx: mpsc::Receiver<ReportingAgentEvent>,
) where
    Agent: SwimAgent<Config> + Debug,
    Config: Send + Sync + Clone + Debug + 'static,
    Lifecycle: AgentLifecycle<Agent> + Send + Sync + Clone + Debug + 'static,
{
    let uri = "/test".parse().unwrap();
    let buffer_size = non_zero_usize!(10);
    let clock = TestClock::default();

    let exec_config = AgentExecutionConfig::with(
        buffer_size,
        1,
        0,
        Duration::from_secs(1),
        None,
        Duration::from_secs(60),
    );

    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());

    let parameters = AgentParameters::new(config, exec_config, uri, HashMap::new());

    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        non_zero_usize!(8),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    let client = ClientContext::new(downlinks);

    // The ReportingAgent is carefully contrived such that its lifecycle events all trigger in
    // a specific order. We can then safely expect these events in that order to verify the agent
    // loop.
    let (_, agent_proc) = provider.run(
        parameters,
        clock.clone(),
        client,
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::plane(1024)),
        MockNodeStore::mock(),
    );

    let agent_task = swim_async_runtime::task::spawn(agent_proc);

    async fn expect(rx: &mut mpsc::Receiver<ReportingAgentEvent>, expected: ReportingAgentEvent) {
        let result = rx.recv().await;
        assert!(result.is_some());
        let event = result.unwrap();
        assert_eq!(event, expected);
    }

    expect(&mut rx, ReportingAgentEvent::AgentStart).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;
    expect(&mut rx, ReportingAgentEvent::Command("Name0".to_string())).await;
    expect(&mut rx, ReportingAgentEvent::DemandLaneEvent(0)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name0".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(1)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DemandMapLaneEvent("Name0".to_string(), 1),
    )
    .await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;

    expect(&mut rx, ReportingAgentEvent::Command("Name1".to_string())).await;
    expect(&mut rx, ReportingAgentEvent::DemandLaneEvent(1)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name1".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(2)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DemandMapLaneEvent("Name1".to_string(), 1),
    )
    .await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;
    expect(&mut rx, ReportingAgentEvent::Command("Name2".to_string())).await;
    expect(&mut rx, ReportingAgentEvent::DemandLaneEvent(2)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name2".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(3)).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DemandMapLaneEvent("Name2".to_string(), 1),
    )
    .await;

    drop(envelope_tx);

    assert!(agent_task.await.is_ok());
}
