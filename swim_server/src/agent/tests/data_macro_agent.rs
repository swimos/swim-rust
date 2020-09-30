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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{LaneLifecycle, StatefulLaneLifecycleBase};
use crate::agent::lane::model::action::{ActionLane, CommandLane};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use crate::agent::lane::strategy::Queue;
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::tests::test_clock::TestClock;
use crate::agent::{AgentConfig, AgentContext, LaneTasks};
use crate::{
    action_lifecycle, agent_lifecycle, command_lifecycle, map_lifecycle, value_lifecycle, SwimAgent,
};
use futures::{FutureExt, StreamExt};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stm::stm::Stm;
use stm::transaction::atomically;
use tokio::sync::{mpsc, Mutex};
use url::Url;

mod swim_server {
    pub use crate::*;
}

#[derive(Debug, SwimAgent)]
#[agent(config = "DataAgentConfig")]
struct DataAgent {
    #[lifecycle(public, name = "MapLifecycle1")]
    map_1: MapLane<String, i32>,
    #[lifecycle(public, name = "MapLifecycle2")]
    map_2: MapLane<String, f64>,
    #[lifecycle(name = "ValueLifecycle1")]
    value_1: ValueLane<i32>,
    #[lifecycle(name = "ValueLifecycle2")]
    value_2: ValueLane<f64>,
    #[lifecycle(name = "CommandLifecycle1")]
    command_1: CommandLane<String>,
    #[lifecycle(name = "CommandLifecycle2")]
    command_2: CommandLane<String>,
    #[lifecycle(name = "ActionLifecycle1")]
    action_1: ActionLane<String, i32>,
}

#[derive(Debug)]
pub struct DataAgentConfig {
    collector: Arc<Mutex<EventCollector>>,
    command_buffer_size: NonZeroUsize,
}

impl DataAgentConfig {
    pub fn new(sender: mpsc::Sender<DataAgentEvent>) -> Self {
        DataAgentConfig {
            collector: Arc::new(Mutex::new(EventCollector::new(sender))),
            command_buffer_size: NonZeroUsize::new(5).unwrap(),
        }
    }
}

impl AgentConfig for DataAgentConfig {
    fn get_buffer_size(&self) -> NonZeroUsize {
        self.command_buffer_size.clone()
    }
}

/// Type of the events that will be reported by the agent.
#[derive(Debug, PartialEq)]
pub enum DataAgentEvent {
    AgentStart,
    Command(String),
    MapEvent1(MapLaneEvent<String, i32>),
    MapEvent2(MapLaneEvent<String, f64>),
    ValueEvent1(i32),
    ValueEvent2(f64),
    TransactionFailed,
}

/// Collects the events from the agent life-cycles.
#[derive(Debug)]
pub struct EventCollector {
    events: mpsc::Sender<DataAgentEvent>,
}

impl EventCollector {
    pub fn new(events: mpsc::Sender<DataAgentEvent>) -> Self {
        EventCollector { events }
    }
}

#[derive(Clone, Debug)]
struct EventCollectorHandler(Arc<Mutex<EventCollector>>);

impl EventCollectorHandler {
    /// Push an event into the channel.
    async fn push(&self, event: DataAgentEvent) {
        self.0
            .lock()
            .await
            .events
            .send(event)
            .await
            .expect("Event receiver was dropped.")
    }
}

// ------------------------------ Agent Lifecycle -------------------------------

#[agent_lifecycle(agent = "DataAgent")]
struct DataAgentLifecycle {
    event_handler: EventCollectorHandler,
}

impl DataAgentLifecycle {
    async fn on_start<Context>(&self, context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync,
    {
        self.event_handler.push(DataAgentEvent::AgentStart).await;

        let mut count = 0;
        let cmd_1 = context.agent().command_1.clone();
        let cmd_2 = context.agent().command_2.clone();
        let action_1 = context.agent().action_1.clone();

        context
            .periodically(
                move || {
                    let index = count;
                    count += 1;

                    let cmd_1_key = format!("Command1:{}", index);
                    let cmd_2_key = format!("Command2:{}", index);
                    let action_1_key = format!("Action1:{}", index);

                    let mut commander_1 = cmd_1.commander();
                    let mut commander_2 = cmd_2.commander();
                    let mut commander_3 = action_1.commander();

                    Box::pin(async move {
                        commander_1.command(cmd_1_key).await;
                        commander_2.command(cmd_2_key).await;
                        commander_3.command(action_1_key).await;
                    })
                },
                Duration::from_secs(1),
                None,
            )
            .await;
    }
}

// ------------------------------ Command Lifecycle 1 -------------------------------

#[command_lifecycle(agent = "DataAgent", command_type = "String")]
struct CommandLifecycle1 {
    event_handler: EventCollectorHandler,
}

impl CommandLifecycle1 {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &ActionLane<String, ()>,
        context: &Context,
    ) where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        self.event_handler
            .push(DataAgentEvent::Command(command.clone()))
            .await;
        if context
            .agent()
            .map_1
            .update_direct(command, 1.into())
            .apply(ExactlyOnce)
            .await
            .is_err()
        {
            self.event_handler
                .push(DataAgentEvent::TransactionFailed)
                .await;
        }
    }
}

impl LaneLifecycle<DataAgentConfig> for CommandLifecycle1 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        CommandLifecycle1 { event_handler }
    }
}

// ------------------------------ Command Lifecycle 2 -------------------------------

#[command_lifecycle(agent = "DataAgent", command_type = "String")]
struct CommandLifecycle2 {
    event_handler: EventCollectorHandler,
}

impl CommandLifecycle2 {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &ActionLane<String, ()>,
        context: &Context,
    ) where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        self.event_handler
            .push(DataAgentEvent::Command(command.clone()))
            .await;
        if context
            .agent()
            .map_2
            .update_direct(command, 1.0.into())
            .apply(ExactlyOnce)
            .await
            .is_err()
        {
            self.event_handler
                .push(DataAgentEvent::TransactionFailed)
                .await;
        }
    }
}

impl LaneLifecycle<DataAgentConfig> for CommandLifecycle2 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        CommandLifecycle2 { event_handler }
    }
}

// ------------------------------ Action Lifecycle 1 -------------------------------

#[action_lifecycle(agent = "DataAgent", command_type = "String", response_type = "i32")]
struct ActionLifecycle1 {
    event_handler: EventCollectorHandler,
}

impl ActionLifecycle1 {
    async fn on_command<Context>(
        &self,
        command: String,
        _model: &ActionLane<String, i32>,
        context: &Context,
    ) -> i32
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        self.event_handler
            .push(DataAgentEvent::Command(command.clone()))
            .await;
        if context
            .agent()
            .map_1
            .update_direct(command, 5.into())
            .apply(ExactlyOnce)
            .await
            .is_err()
        {
            self.event_handler
                .push(DataAgentEvent::TransactionFailed)
                .await;
        }
        42
    }
}

impl LaneLifecycle<DataAgentConfig> for ActionLifecycle1 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        ActionLifecycle1 { event_handler }
    }
}

// ------------------------------ Map Lifecycle 1 -------------------------------

#[map_lifecycle(agent = "DataAgent", key_type = "String", value_type = "i32")]
struct MapLifecycle1 {
    event_handler: EventCollectorHandler,
}

impl MapLifecycle1 {
    async fn on_start<Context>(&self, _model: &MapLane<String, i32>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(
        &self,
        event: &MapLaneEvent<String, i32>,
        _model: &MapLane<String, i32>,
        context: &Context,
    ) where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        self.event_handler
            .push(DataAgentEvent::MapEvent1(event.clone()))
            .await;
        if let MapLaneEvent::Update(_, v) = event {
            let i = **v;

            let total = &context.agent().value_1;

            let add = total.get().and_then(move |n| total.set(*n + i));

            if atomically(&add, ExactlyOnce).await.is_err() {
                self.event_handler
                    .push(DataAgentEvent::TransactionFailed)
                    .await;
            }
        }
    }
}

impl LaneLifecycle<DataAgentConfig> for MapLifecycle1 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        MapLifecycle1 { event_handler }
    }
}

impl StatefulLaneLifecycleBase for MapLifecycle1 {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

// ------------------------------ Map Lifecycle 2 -------------------------------

#[map_lifecycle(agent = "DataAgent", key_type = "String", value_type = "f64")]
struct MapLifecycle2 {
    event_handler: EventCollectorHandler,
}

impl MapLifecycle2 {
    async fn on_start<Context>(&self, _model: &MapLane<String, f64>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(
        &self,
        event: &MapLaneEvent<String, f64>,
        _model: &MapLane<String, f64>,
        context: &Context,
    ) where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        self.event_handler
            .push(DataAgentEvent::MapEvent2(event.clone()))
            .await;
        if let MapLaneEvent::Update(_, v) = event {
            let i = **v;

            let total = &context.agent().value_2;

            let add = total.get().and_then(move |n| total.set(*n + i));

            if atomically(&add, ExactlyOnce).await.is_err() {
                self.event_handler
                    .push(DataAgentEvent::TransactionFailed)
                    .await;
            }
        }
    }
}

impl LaneLifecycle<DataAgentConfig> for MapLifecycle2 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        MapLifecycle2 { event_handler }
    }
}

impl StatefulLaneLifecycleBase for MapLifecycle2 {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

// ------------------------------ Value Lifecycle 1 -------------------------------

#[value_lifecycle(agent = "DataAgent", event_type = "i32")]
struct ValueLifecycle1 {
    event_handler: EventCollectorHandler,
}

impl ValueLifecycle1 {
    async fn on_start<Context>(&self, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(&self, event: &Arc<i32>, _model: &ValueLane<i32>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        let n = **event;
        self.event_handler
            .push(DataAgentEvent::ValueEvent1(n))
            .await;
    }
}

impl LaneLifecycle<DataAgentConfig> for ValueLifecycle1 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        ValueLifecycle1 { event_handler }
    }
}

impl StatefulLaneLifecycleBase for ValueLifecycle1 {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

// ------------------------------ Value Lifecycle 2 -------------------------------

#[value_lifecycle(agent = "DataAgent", event_type = "f64")]
struct ValueLifecycle2 {
    event_handler: EventCollectorHandler,
}

impl ValueLifecycle2 {
    async fn on_start<Context>(&self, _model: &ValueLane<f64>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync,
    {
    }

    async fn on_event<Context>(&self, event: &Arc<f64>, _model: &ValueLane<f64>, _context: &Context)
    where
        Context: AgentContext<DataAgent> + Sized + Send + Sync + 'static,
    {
        let n = **event;
        self.event_handler
            .push(DataAgentEvent::ValueEvent2(n))
            .await;
    }
}

impl LaneLifecycle<DataAgentConfig> for ValueLifecycle2 {
    fn create(config: &DataAgentConfig) -> Self {
        let event_handler = EventCollectorHandler(config.collector.clone());
        ValueLifecycle2 { event_handler }
    }
}

impl StatefulLaneLifecycleBase for ValueLifecycle2 {
    type WatchStrategy = Queue;

    fn create_strategy(&self) -> Self::WatchStrategy {
        Queue::default()
    }
}

#[tokio::test]
async fn agent_loop() {
    let (tx, mut rx) = mpsc::channel(5);

    let config = DataAgentConfig::new(tx);

    let url = Url::parse("test://").unwrap();
    let buffer_size = NonZeroUsize::new(10).unwrap();
    let clock = TestClock::default();

    let agent_lifecycle = DataAgentLifecycle {
        event_handler: EventCollectorHandler(config.collector.clone()),
    };

    let exec_config = AgentExecutionConfig::with(buffer_size, 1, 0, Duration::from_secs(1));

    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());

    let agent_proc = super::super::run_agent(
        config,
        agent_lifecycle,
        url,
        exec_config,
        clock.clone(),
        envelope_rx,
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    async fn expect(rx: &mut mpsc::Receiver<DataAgentEvent>, expected: DataAgentEvent) {
        let result = rx.next().await;
        assert!(result.is_some());
        let event = result.unwrap();
        assert_eq!(event, expected);
    }

    expect(&mut rx, DataAgentEvent::AgentStart).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;

    expect(&mut rx, DataAgentEvent::Command("Command1:0".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Command2:0".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Action1:0".to_string())).await;

    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Command1:0".to_string(), 1.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Action1:0".to_string(), 5.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent2(MapLaneEvent::Update("Command2:0".to_string(), 1.0.into())),
    )
    .await;

    expect(&mut rx, DataAgentEvent::ValueEvent1(1)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent1(6)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent2(1.0)).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;

    expect(&mut rx, DataAgentEvent::Command("Command1:1".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Command2:1".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Action1:1".to_string())).await;

    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Command1:1".to_string(), 1.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Action1:1".to_string(), 5.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent2(MapLaneEvent::Update("Command2:1".to_string(), 1.0.into())),
    )
    .await;

    expect(&mut rx, DataAgentEvent::ValueEvent1(7)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent1(12)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent2(2.0)).await;

    clock.advance_when_blocked(Duration::from_secs(1)).await;

    expect(&mut rx, DataAgentEvent::Command("Command1:2".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Command2:2".to_string())).await;
    expect(&mut rx, DataAgentEvent::Command("Action1:2".to_string())).await;

    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Command1:2".to_string(), 1.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent1(MapLaneEvent::Update("Action1:2".to_string(), 5.into())),
    )
    .await;
    expect(
        &mut rx,
        DataAgentEvent::MapEvent2(MapLaneEvent::Update("Command2:2".to_string(), 1.0.into())),
    )
    .await;

    expect(&mut rx, DataAgentEvent::ValueEvent1(13)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent1(18)).await;
    expect(&mut rx, DataAgentEvent::ValueEvent2(3.0)).await;

    drop(envelope_tx);
    assert!(agent_task.await.is_ok());
}
