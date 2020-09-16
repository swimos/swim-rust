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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::lifecycle::StatefulLaneLifecycleBase;
use crate::agent::lane::model;
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::tests::test_clock::TestClock;
use crate::agent::LaneIo;
use crate::agent::{AgentContext, LaneTasks, SwimAgent};
use crate::agent::{COMMANDED, RESPONSE_IGNORED};
use crate::{agent_lifecycle, command_lifecycle, map_lifecycle, value_lifecycle};
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::stm::Stm;
use stm::transaction::atomically;
use tokio::sync::{mpsc, Mutex};
use url::Url;
use utilities::sync::trigger;

mod swim_server {
    pub use crate::*;
}

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
struct ReportingAgentLifecycle {
    event_handler: EventCollectorHandler,
}

async fn agent_on_start<Context>(lifecycle: &ReportingAgentLifecycle, context: &Context)
where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
    lifecycle
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
struct ActionLifecycle {
    event_handler: EventCollectorHandler,
}

async fn action_on_command<Context>(
    lifecycle: &ActionLifecycle,
    command: String,
    _model: &ActionLane<String, ()>,
    context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    lifecycle
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
        lifecycle
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
struct DataLifecycle {
    event_handler: EventCollectorHandler,
}

impl StatefulLaneLifecycleBase for DataLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

async fn data_on_start<Context>(
    _lifecycle: &DataLifecycle,
    _model: &MapLane<String, i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
}

async fn data_on_event<Context>(
    lifecycle: &DataLifecycle,
    event: &MapLaneEvent<String, i32>,
    _model: &MapLane<String, i32>,
    context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    lifecycle
        .event_handler
        .push(ReportingAgentEvent::DataEvent(event.clone()))
        .await;
    if let MapLaneEvent::Update(_, v) = event {
        let i = **v;

        let total = &context.agent().total;

        let add = total.get().and_then(move |n| total.set(*n + i));

        if atomically(&add, ExactlyOnce).await.is_err() {
            lifecycle
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
struct TotalLifecycle {
    event_handler: EventCollectorHandler,
}

impl StatefulLaneLifecycleBase for TotalLifecycle {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

async fn total_on_start<Context>(
    _lifecycle: &TotalLifecycle,
    _model: &ValueLane<i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync,
{
}

async fn total_on_event<Context>(
    lifecycle: &TotalLifecycle,
    event: &Arc<i32>,
    _model: &ValueLane<i32>,
    _context: &Context,
) where
    Context: AgentContext<ReportingAgent> + Sized + Send + Sync + 'static,
{
    let n = **event;
    lifecycle
        .event_handler
        .push(ReportingAgentEvent::TotalEvent(n))
        .await;
}

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
        ReportingAgentLifecycleTask {
            lifecycle: ReportingAgentLifecycle {
                event_handler: EventCollectorHandler(self.collector.clone()),
            },
        }
    }
}

impl SwimAgent<TestAgentConfig> for ReportingAgent {
    fn instantiate<Context: AgentContext<Self> + AgentExecutionContext>(
        configuration: &TestAgentConfig,
    ) -> (
        Self,
        Vec<Box<dyn LaneTasks<Self, Context>>>,
        HashMap<String, Box<dyn LaneIo<Context>>>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
    {
        let TestAgentConfig {
            collector,
            command_buffer_size,
        } = configuration;

        let event_handler = EventCollectorHandler(collector.clone());

        // Command lifecycle
        let (action, event_stream) = model::action::make_lane_model(*command_buffer_size);
        let action_tasks = ActionLifecycleTask {
            lifecycle: ActionLifecycle {
                event_handler: event_handler.clone(),
            },
            name: "action".into(),
            event_stream,
            projection: |agent: &ReportingAgent| &agent.action,
        };

        // Value lifecycle
        let total_lifecycle = TotalLifecycle {
            event_handler: event_handler.clone(),
        };
        let (total, event_stream) =
            model::value::make_lane_model(0, total_lifecycle.create_strategy());

        let total_tasks = TotalLifecycleTask {
            lifecycle: total_lifecycle,
            name: "total".into(),
            event_stream,
            projection: |agent: &ReportingAgent| &agent.total,
        };

        // Map lifecycle
        let data_lifecycle = DataLifecycle {
            event_handler: event_handler.clone(),
        };
        let (data, event_stream) = model::map::make_lane_model(data_lifecycle.create_strategy());

        let data_tasks = DataLifecycleTask {
            lifecycle: data_lifecycle,
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
        (agent, tasks, HashMap::new())
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
