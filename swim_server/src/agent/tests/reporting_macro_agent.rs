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

use crate::agent;
use crate::agent::lane::lifecycle::{
    ActionLaneLifecycle, StatefulLaneLifecycle, StatefulLaneLifecycleBase,
};
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapLaneWatch};
use crate::agent::lane::model::value::{ValueLane, ValueLaneWatch};
use crate::agent::lane::strategy::Queue;
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::Lane;
use crate::agent::Stream;
use crate::agent::{AgentContext, LaneTasks, SwimAgent};
use futures::future::{ready, Ready};
use futures::{FutureExt, StreamExt};
use futures_util::future::BoxFuture;
use pin_utils::pin_mut;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::stm::Stm;
use stm::transaction::atomically;
use stm::var::TVar;
use tokio::sync::{mpsc, Mutex};
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::future::SwimStreamExt;
use url::Url;
use crate::agent::tests::test_clock::TestClock;
use utilities::sync::trigger;

const ON_COMMAND: &str = "On command handler";
const COMMANDED: &str = "Command received";
const ON_EVENT: &str = "On event handler";

/// An agent for use in tests of the agent execution loop. All events that occur in the lifecycle
/// vents of the agent and its lanes are reported on an MPSC channel. When the agent starts it
/// creates a periodic schedule that fires every second. For each event of the schedule, an entry
/// will be inserted into the the `data` lane with keys "Name0", "Name1" and so forth. For each
/// entry inserted, the value of the `total` lane will be incremented by the inserted value.
#[derive(Debug)]
pub struct ReportingAgent {
    data: MapLane<String, i32>,
    total: ValueLane<i32>,
    action: CommandLane<String>,
}

/// Type of the events that will be reported by the agent.
#[derive(Debug, PartialEq, Eq)]
pub enum ReportingAgentEvent {
    AgentStart,
    Command(String),
    TransactionFailed,
    DataEvent(MapLaneEvent<String, i32>),
    TotalEvent(i32),
}

/// Collects the events from the agent life-cycles.
#[derive(Debug)]
pub struct EventCollector {
    events: mpsc::Sender<ReportingAgentEvent>,
}

impl EventCollector {
    pub fn new(events: mpsc::Sender<ReportingAgentEvent>) -> Self {
        EventCollector { events }
    }
}

#[derive(Clone, Debug)]
struct EventCollectorHandler(Arc<Mutex<EventCollector>>);

impl EventCollectorHandler {
    /// Push an event into the channel.
    async fn push(&self, event: ReportingAgentEvent) {
        self.0
            .lock()
            .await
            .events
            .send(event)
            .await
            .expect("Event receiver was dropped.")
    }
}

#[agent_lifecycle(agent = "ReportingAgent", on_start = "agent_on_start")]
struct RepoAgent {
    event_handler: EventCollectorHandler,
}

async fn agent_on_start<Context>(inner: &RepoAgent, context: &Context)
where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
    inner
        .event_handler
        .push(ReportingAgentEvent::AgentStart)
        .await;

    let mut count = 0;
    let cmd = context.agent().action.clone();

    context
        .periodically(
            move || {
                let index = count;
                count += 1;

                let key = format!("Name{}", index);
                let mut commander = cmd.commander();

                Box::pin(async move {
                    commander.command(key).await;
                })
            },
            Duration::from_secs(1),
            None,
        )
        .await;
}

#[command_lifecycle(
    agent = "ReportingAgent",
    command_type = "String",
    on_command = "action_on_command"
)]
struct ReportingAction {
    event_handler: EventCollectorHandler,
}

async fn action_on_command<Context>(
    inner: &ReportingAction,
    command: String,
    _model: &ActionLane<String, ()>,
    context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    inner
        .event_handler
        .push(ReportingAgentEvent::Command(command.clone()))
        .await;
    if context
        .agent()
        .data
        .update_direct(command, 1.into())
        .apply(ExactlyOnce)
        .await
        .is_err()
    {
        inner
            .event_handler
            .push(ReportingAgentEvent::TransactionFailed)
            .await;
    }
}

#[map_lifecycle(
    agent = "ReportingAgent",
    key_type = "String",
    value_type = "i32",
    on_start = "data_on_start",
    on_event = "data_on_event"
)]
struct ReportingData {
    event_handler: EventCollectorHandler,
}

async fn data_on_start<Context>(
    _inner: &ReportingData,
    _model: &MapLane<String, i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
}

async fn data_on_event<Context>(
    inner: &ReportingData,
    event: &MapLaneEvent<String, i32>,
    _model: &MapLane<String, i32>,
    context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    inner
        .event_handler
        .push(ReportingAgentEvent::DataEvent(event.clone()))
        .await;
    if let MapLaneEvent::Update(_, v) = event {
        let i = **v;

        let total = &context.agent().total;

        let add = total.get().and_then(move |n| total.set(*n + i));

        if atomically(&add, ExactlyOnce).await.is_err() {
            inner
                .event_handler
                .push(ReportingAgentEvent::TransactionFailed)
                .await;
        }
    }
}

#[value_lifecycle(
    agent = "ReportingAgent",
    event_type = "i32",
    on_start = "total_on_start",
    on_event = "total_on_event"
)]
struct ReportingTotal {
    event_handler: EventCollectorHandler,
}

async fn total_on_start<Context>(
    _inner: &ReportingTotal,
    _model: &ValueLane<i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
}

async fn total_on_event<Context>(
    inner: &ReportingTotal,
    event: &Arc<i32>,
    _model: &ValueLane<i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    let n = **event;
    inner
        .event_handler
        .push(ReportingAgentEvent::TotalEvent(n))
        .await;
}

// impl StatefulLaneLifecycleBase for ReportingDataLifecycle {
//     type WatchStrategy = Queue;
//
//     fn create_strategy(&self) -> Self::WatchStrategy {
//         Queue::default()
//     }
// }
//
// impl StatefulLaneLifecycleBase for ReportingTotalLifecycle {
//     type WatchStrategy = Queue;
//
//     fn create_strategy(&self) -> Self::WatchStrategy {
//         Queue::default()
//     }
// }

/// The event reporter is injected into the agent as ersatz configuration.
#[derive(Debug)]
pub struct TestAgentConfig {
    collector: Arc<Mutex<EventCollector>>,
    command_buffer_size: NonZeroUsize,
}

impl TestAgentConfig {
    pub fn new(sender: mpsc::Sender<ReportingAgentEvent>) -> Self {
        TestAgentConfig {
            collector: Arc::new(Mutex::new(EventCollector::new(sender))),
            command_buffer_size: NonZeroUsize::new(5).unwrap(),
        }
    }

    pub fn agent_lifecycle(&self) -> impl AgentLifecycle<ReportingAgent> {
        RepoAgentLifecycle {
            inner: RepoAgent {
                event_handler: EventCollectorHandler(self.collector.clone()),
            },
        }
    }
}

impl SwimAgent<TestAgentConfig> for ReportingAgent {
    fn instantiate<Context: AgentContext<Self>>(
        configuration: &TestAgentConfig,
    ) -> (Self, Vec<Box<dyn LaneTasks<Self, Context>>>)
    where
        Context: AgentContext<Self> + Send + Sync + 'static,
    {
        let TestAgentConfig {
            collector,
            command_buffer_size,
        } = configuration;

        let event_handler = EventCollectorHandler(collector.clone());

        let buffer_size = NonZeroUsize::new(5).unwrap();
        let (tx, event_stream) = mpsc::channel(buffer_size.get());
        let action = ActionLane::new(tx);

        let action_tasks = ReportingActionLifecycle {
            inner: ReportingAction {
                event_handler: event_handler.clone(),
            },
            name: "action".into(),
            event_stream,
            projection: |agent: &ReportingAgent| &agent.action,
        };

        let init_val = 0;
        let value = Arc::new(init_val);
        let (observer, event_stream) =
            agent::lane::model::value::ValueLaneWatch::make_watch(&Queue::default(), &value);
        let var = TVar::from_arc_with_observer(value, observer);
        let total = ValueLane::from_tvar(var);

        let total_tasks = ReportingTotalLifecycle {
            inner: ReportingTotal {
                event_handler: event_handler.clone(),
            },
            name: "total".into(),
            event_stream,
            projection: |agent: &ReportingAgent| &agent.total,
        };

        let (observer, event_stream) =
            agent::lane::model::map::MapLaneWatch::make_watch(&Queue::default());
        let summary = TVar::new_with_observer(Default::default(), observer);
        let data = MapLane::from_summary(summary);

        let data_tasks = ReportingDataLifecycle {
            inner: ReportingData {
                event_handler: event_handler.clone(),
            },
            name: "data".into(),
            event_stream,
            projection: |agent: &ReportingAgent| &agent.data,
        };

        let agent = ReportingAgent {
            data,
            total,
            action,
        };

        let tasks = vec![
            data_tasks.boxed(),
            total_tasks.boxed(),
            action_tasks.boxed(),
        ];
        (agent, tasks)
    }
}

#[tokio::test]
async fn agent_loop() {
    let (tx, mut rx) = mpsc::channel(5);

    let config = TestAgentConfig::new(tx);

    let url = Url::parse("test://").unwrap();
    let buffer_size = NonZeroUsize::new(10).unwrap();
    let clock = TestClock::default();
    let (stop, stop_sig) = trigger::trigger();

    let agent_lifecycle = config.agent_lifecycle();

    // The ReportingAgent is carefully contrived such that its lifecycle events all trigger in
    // a specific order. We can then safely expect these events in that order to verify the agent
    // loop.
    let agent_proc = super::super::run_agent(
        config,
        agent_lifecycle,
        url,
        buffer_size,
        clock.clone(),
        stop_sig,
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    async fn expect(rx: &mut mpsc::Receiver<ReportingAgentEvent>, expected: ReportingAgentEvent) {
        let result = rx.next().await;
        assert!(result.is_some());
        let event = result.unwrap();
        assert_eq!(event, expected);
    }

    expect(&mut rx, ReportingAgentEvent::AgentStart).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;
    expect(&mut rx, ReportingAgentEvent::Command("Name0".to_string())).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name0".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(1)).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;
    expect(&mut rx, ReportingAgentEvent::Command("Name1".to_string())).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name1".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(2)).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;
    expect(&mut rx, ReportingAgentEvent::Command("Name2".to_string())).await;
    expect(
        &mut rx,
        ReportingAgentEvent::DataEvent(MapLaneEvent::Update("Name2".to_string(), 1.into())),
    )
    .await;
    expect(&mut rx, ReportingAgentEvent::TotalEvent(3)).await;

    assert!(stop.trigger());

    assert!(agent_task.await.is_ok());
}
