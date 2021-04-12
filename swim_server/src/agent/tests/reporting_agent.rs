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

use crate::agent;
use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::{
    CommandLaneLifecycle, DemandLaneLifecycle, DemandMapLaneLifecycle, StatefulLaneLifecycle,
};
use crate::agent::lane::model::command::CommandLane;
use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::demand_map::DemandMapLane;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent};
use crate::agent::lane::model::value::{ValueLane, ValueLaneEvent};
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::{AgentContext, LaneIo, LaneProperties, LaneTasks, SwimAgent};
use futures::future::{ready, BoxFuture, Ready};
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use store::mem::transaction::atomically;
use store::NodeStore;
use tokio::sync::{mpsc, Mutex};

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
    demand: DemandLane<i32>,
    demand_map: DemandMapLane<String, i32>,
}

/// Type of the events that will be reported by the agent.
#[derive(Debug, PartialEq, Eq)]
pub enum ReportingAgentEvent {
    AgentStart,
    Command(String),
    DemandLaneEvent(i32),
    DemandMapLaneEvent(String, i32),
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
struct ReportingLifecycleInner(Arc<Mutex<EventCollector>>);

impl ReportingLifecycleInner {
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

#[derive(Debug, Clone)]
struct ReportingAgentLifecycle {
    inner: ReportingLifecycleInner,
}
#[derive(Debug)]
struct DataLifecycle {
    inner: ReportingLifecycleInner,
}
#[derive(Debug)]
struct TotalLifecycle {
    inner: ReportingLifecycleInner,
}
#[derive(Debug)]
struct ActionLifecycle {
    inner: ReportingLifecycleInner,
}
#[derive(Debug)]
struct DemandLifecycle {
    inner: ReportingLifecycleInner,
}
#[derive(Debug)]
struct DemandMapLifecycle {
    inner: ReportingLifecycleInner,
}

impl<'a> DemandMapLaneLifecycle<'a, String, i32, ReportingAgent> for DemandMapLifecycle {
    type OnSyncFuture = BoxFuture<'a, Vec<String>>;
    type OnCueFuture = BoxFuture<'a, Option<i32>>;
    type OnRemoveFuture = BoxFuture<'a, ()>;

    fn on_sync<C>(
        &'a self,
        _model: &'a DemandMapLane<String, i32>,
        _context: &'a C,
    ) -> Self::OnSyncFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        Box::pin(ready(Vec::new()))
    }
    fn on_cue<C>(
        &'a self,
        _model: &'a DemandMapLane<String, i32>,
        context: &'a C,
        key: String,
    ) -> Self::OnCueFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let result = atomically(&context.agent().data.get(key.clone()), ExactlyOnce).await;
            match result {
                Ok(Some(value)) => {
                    self.inner
                        .push(ReportingAgentEvent::DemandMapLaneEvent(key, *value))
                        .await;
                    Some(*value)
                }
                _ => {
                    self.inner
                        .push(ReportingAgentEvent::TransactionFailed)
                        .await;
                    None
                }
            }
        })
    }

    fn on_remove<C>(
        &'a mut self,
        _model: &'a DemandMapLane<String, i32>,
        _context: &'a C,
        _key: String,
    ) -> Self::OnRemoveFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        ready(()).boxed()
    }
}

impl<'a> DemandLaneLifecycle<'a, i32, ReportingAgent> for DemandLifecycle {
    type OnCueFuture = BoxFuture<'a, Option<i32>>;

    fn on_cue<C>(&'a self, _model: &'a DemandLane<i32>, context: &'a C) -> Self::OnCueFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let total = *context.agent().total.load().await.unwrap();

            self.inner
                .push(ReportingAgentEvent::DemandLaneEvent(total))
                .await;

            Some(total)
        })
    }
}

impl AgentLifecycle<ReportingAgent> for ReportingAgentLifecycle {
    fn starting<'a, C: AgentContext<ReportingAgent>>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'a,
    {
        Box::pin(async move {
            self.inner.push(ReportingAgentEvent::AgentStart).await;

            let mut count = 0;
            let cmd: CommandLane<String> = context.agent().action.clone();
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
        })
    }
}

impl<'a> CommandLaneLifecycle<'a, String, ReportingAgent> for ActionLifecycle {
    type ResponseFuture = BoxFuture<'a, ()>;

    fn on_command<C>(
        &'a self,
        command: &'a String,
        _model: &'a CommandLane<String>,
        context: &'a C,
    ) -> Self::ResponseFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            self.inner
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
                self.inner
                    .push(ReportingAgentEvent::TransactionFailed)
                    .await;
            }
        })
    }
}

impl<'a> StatefulLaneLifecycle<'a, MapLane<String, i32>, ReportingAgent> for DataLifecycle {
    type StartFuture = Ready<()>;
    type EventFuture = BoxFuture<'a, ()>;

    fn on_start<C>(&'a self, _model: &'a MapLane<String, i32>, _context: &'a C) -> Self::StartFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'a,
    {
        ready(())
    }

    fn on_event<C>(
        &'a mut self,
        event: &'a MapLaneEvent<String, i32>,
        _model: &'a MapLane<String, i32>,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            self.inner
                .push(ReportingAgentEvent::DataEvent(event.clone()))
                .await;
            if let MapLaneEvent::Update(key, v) = event {
                let i = **v;

                let total = &context.agent().total;

                if total.get_for_update(move |n| *n + i).await.is_err() {
                    self.inner
                        .push(ReportingAgentEvent::TransactionFailed)
                        .await;
                } else {
                    let mut controller = context.agent().demand_map.controller();

                    if controller.cue(key.clone()).await.is_err() {
                        self.inner
                            .push(ReportingAgentEvent::TransactionFailed)
                            .await;
                    }
                }
            }
        })
    }
}

impl<'a> StatefulLaneLifecycle<'a, ValueLane<i32>, ReportingAgent> for TotalLifecycle {
    type StartFuture = Ready<()>;
    type EventFuture = BoxFuture<'a, ()>;

    fn on_start<C>(&'a self, _model: &'a ValueLane<i32>, _context: &'a C) -> Self::StartFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'a,
    {
        ready(())
    }

    fn on_event<C>(
        &'a mut self,
        event: &ValueLaneEvent<i32>,
        _model: &'a ValueLane<i32>,
        _context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<ReportingAgent> + Send + Sync + 'static,
    {
        let n = *event.current;
        Box::pin(async move {
            self.inner.push(ReportingAgentEvent::TotalEvent(n)).await;
        })
    }
}

/// The event reporter is injected into the agent as ersatz configuration.
#[derive(Debug, Clone)]
pub struct TestAgentConfig {
    collector: Arc<Mutex<EventCollector>>,
}

impl TestAgentConfig {
    pub fn new(sender: mpsc::Sender<ReportingAgentEvent>) -> Self {
        TestAgentConfig {
            collector: Arc::new(Mutex::new(EventCollector::new(sender))),
        }
    }

    pub fn agent_lifecycle(&self) -> impl AgentLifecycle<ReportingAgent> + Clone + Debug {
        ReportingAgentLifecycle {
            inner: ReportingLifecycleInner(self.collector.clone()),
        }
    }
}

impl SwimAgent<TestAgentConfig> for ReportingAgent {
    fn instantiate<Context, Store>(
        configuration: &TestAgentConfig,
        exec_conf: &AgentExecutionConfig,
        store: &Store,
    ) -> (
        Self,
        Vec<Box<dyn LaneTasks<Self, Context>>>,
        HashMap<String, Box<dyn LaneIo<Context>>>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
        Store: NodeStore,
    {
        let TestAgentConfig { collector } = configuration;

        let inner = ReportingLifecycleInner(collector.clone());

        let (data, data_tasks, _) = agent::make_map_lane(
            "data",
            false,
            exec_conf,
            DataLifecycle {
                inner: inner.clone(),
            },
            |agent: &ReportingAgent| &agent.data,
        );

        let (total, total_tasks, _) = agent::make_value_lane(
            LaneProperties {
                name: "total".to_string(),
                is_public: false,
                is_transient: true,
                init: Default::default(),
            },
            exec_conf,
            TotalLifecycle {
                inner: inner.clone(),
            },
            |agent: &ReportingAgent| &agent.total,
            store,
        );

        let (action, action_tasks, _) = agent::make_command_lane(
            "action",
            false,
            ActionLifecycle {
                inner: inner.clone(),
            },
            |agent: &ReportingAgent| &agent.action,
            exec_conf.action_buffer.clone(),
        );

        let (demand, demand_tasks, _) = agent::make_demand_lane(
            "demand",
            DemandLifecycle {
                inner: inner.clone(),
            },
            |agent: &ReportingAgent| &agent.demand,
            exec_conf.action_buffer.clone(),
        );

        let (demand_map, demand_map_tasks, _) = agent::make_demand_map_lane(
            "demand_map",
            false,
            DemandMapLifecycle {
                inner: inner.clone(),
            },
            |agent: &ReportingAgent| &agent.demand_map,
            exec_conf.action_buffer.clone(),
        );

        let agent = ReportingAgent {
            data,
            total,
            action,
            demand,
            demand_map,
        };

        let tasks = vec![
            data_tasks.boxed(),
            total_tasks.boxed(),
            action_tasks.boxed(),
            demand_tasks.boxed(),
            demand_map_tasks.boxed(),
        ];

        (agent, tasks, HashMap::new())
    }
}
