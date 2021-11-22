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

#![allow(legacy_derive_helpers)]

use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use stm::stm::Stm;
use stm::transaction::atomically;

use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::map::MapLaneEvent;
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::tests::reporting_macro_agent::{
    EventCollector, EventCollectorHandler, ReportingAgentEvent,
};
use crate::agent::tests::run_agent_test;
use crate::agent::AgentContext;
use crate::agent_lifecycle;
use crate::plane::provider::AgentProvider;
use crate::swim_agent;
use swim_utilities::algebra::non_zero_usize;

mod swim_server {
    pub use crate::*;
}

#[derive(Debug, Clone)]
pub struct TestAgentConfig {
    collector: Arc<Mutex<EventCollector>>,
    command_buffer_size: NonZeroUsize,
}

#[agent_lifecycle(agent = "MacroReportingAgent", on_start = "agent_on_start")]
struct MacroReportingAgentLifecycle {
    event_handler: EventCollectorHandler,
}

impl MacroReportingAgentLifecycle {
    async fn agent_on_start<Context>(&self, context: &Context)
    where
        Context: AgentContext<MacroReportingAgent> + Sized + Send + Sync,
    {
        self.event_handler
            .push(ReportingAgentEvent::AgentStart)
            .await;

        let mut count = 0;
        let cmd = context.agent().action.clone();
        let demander: DemandLane<i32> = context.agent().demand.clone();

        context
            .periodically(
                move || {
                    let index = count;
                    count += 1;

                    let key = format!("Name{}", index);
                    let mut commander = cmd.commander();
                    let mut demand_controller = demander.controller();

                    Box::pin(async move {
                        commander.command(key).await;
                        demand_controller.cue().await;
                    })
                },
                Duration::from_secs(1),
                None,
            )
            .await;
    }
}

impl TestAgentConfig {
    pub fn new(sender: mpsc::Sender<ReportingAgentEvent>) -> Self {
        TestAgentConfig {
            collector: Arc::new(Mutex::new(EventCollector::new(sender))),
            command_buffer_size: non_zero_usize!(5),
        }
    }

    pub fn agent_lifecycle(&self) -> impl AgentLifecycle<MacroReportingAgent> + Clone + Debug {
        MacroReportingAgentLifecycle {
            event_handler: EventCollectorHandler(self.collector.clone()),
        }
    }
}

swim_agent! {
    (MacroReportingAgent, TestAgentConfig) => {
        lane(data: MapLane<String, i32>) => {
            lifecycle: DataLifecycle {
                event_handler: EventCollectorHandler,
            }
            on_create(config) {
                let event_handler = EventCollectorHandler(config.collector.clone());
                DataLifecycle { event_handler }
            }
            on_start(self, _model, _context) { }
            on_event(self, event, _model, context) {
                self.event_handler
                .push(ReportingAgentEvent::DataEvent(event.clone()))
                .await;

                if let MapLaneEvent::Update(key, v) = event {
                    let i = **v;

                    let total = &context.agent().total;

                    let add = total.get().and_then(move |n| total.set(*n + i));

                    if atomically(&add, ExactlyOnce).await.is_err() {
                        self.event_handler
                            .push(ReportingAgentEvent::TransactionFailed)
                            .await;
                    } else {
                        let mut controller = context.agent().demand_map.controller();

                        if controller.cue(key.clone()).await.is_err() {
                            self.event_handler
                                .push(ReportingAgentEvent::TransactionFailed)
                                .await;
                        }
                    }
                }
            }
        }
        pub lane(total: ValueLane<i32>) => {
            lifecycle: TotalLifecycle {
                event_handler: EventCollectorHandler,
            }
            on_create(config) {
                let event_handler = EventCollectorHandler(config.collector.clone());
                TotalLifecycle { event_handler }
            }
            on_start(self, _model, _context) {}
            on_event(self, event, _model, _context) {
                let n = *event.current;
                self.event_handler.push(ReportingAgentEvent::TotalEvent(n)).await;
            }
        }
        lane(action: CommandLane<String>) => {
            lifecycle: ActionLifecycle {
                event_handler: EventCollectorHandler,
            }
            on_create(config) {
                let event_handler = EventCollectorHandler(config.collector.clone());
                ActionLifecycle { event_handler }
            }
            on_command(self, command, _model, context) {
                self.event_handler
                .push(ReportingAgentEvent::Command(command.clone()))
                .await;
                if context
                    .agent()
                    .data
                    .update_direct(command.clone(), 1.into())
                    .apply(ExactlyOnce)
                    .await
                    .is_err()
                {
                    self.event_handler
                        .push(ReportingAgentEvent::TransactionFailed)
                        .await;
                }
            }
        }
        lane(demand: DemandLane<i32>) => {
            lifecycle: DemandLifecycle {
                event_handler: EventCollectorHandler,
            }
            on_create(config) {
                let event_handler = EventCollectorHandler(config.collector.clone());
                DemandLifecycle { event_handler }
            }
            on_cue(self, _model, context) -> Option<i32> {
                let total = *context.agent().total.load().await;

                self.event_handler
                    .push(ReportingAgentEvent::DemandLaneEvent(total))
                    .await;

                Some(total)
            }
        }
        lane(demand_map: DemandMapLane<String, i32>) => {
            lifecycle: DemandMapLifecycle {
                event_handler: EventCollectorHandler,
            }
            on_create(config) {
                let event_handler = EventCollectorHandler(config.collector.clone());
                DemandMapLifecycle { event_handler }
            }
            on_cue(self, _model, context, key) -> Option<i32> {
                let result = atomically(&context.agent().data.get(key.clone()), ExactlyOnce).await;
                match result {
                    Ok(Some(value)) => {
                        self.event_handler
                            .push(ReportingAgentEvent::DemandMapLaneEvent(key, *value))
                            .await;
                        Some(*value)
                    }
                    _ => {
                        self.event_handler
                            .push(ReportingAgentEvent::TransactionFailed)
                            .await;
                        None
                    }
                }
            }
            on_sync(self, _model, _context) -> Vec<String> {
                Vec::new()
            }
        }
    }
}

#[tokio::test]
async fn agent_loop() {
    let (tx, rx) = mpsc::channel(5);
    let config = TestAgentConfig::new(tx);
    let agent_lifecycle = config.agent_lifecycle();

    let provider = AgentProvider::new(config.clone(), agent_lifecycle);
    run_agent_test(provider, config, rx).await;
}
