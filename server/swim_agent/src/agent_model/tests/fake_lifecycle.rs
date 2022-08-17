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

use futures::FutureExt;
use swim_model::Text;
use tokio::sync::mpsc;

use crate::{
    agent_lifecycle::{lane_event::LaneEvent, on_start::OnStart, on_stop::OnStop},
    event_handler::{BoxEventHandler, HandlerAction, SideEffect, Spawner, StepResult},
    meta::AgentMetadata,
};

use super::{fake_agent::TestAgent, CMD_LANE};

#[derive(Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    Start,
    Lane(Text),
    RanSuspended(i32),
    RanSuspendedConsequence,
    Stop,
}

pub struct LifecycleHandler {
    sender: mpsc::UnboundedSender<LifecycleEvent>,
    event: Option<LifecycleEvent>,
}

pub struct TestLifecycle {
    sender: mpsc::UnboundedSender<LifecycleEvent>,
}

impl TestLifecycle {
    pub fn new(tx: mpsc::UnboundedSender<LifecycleEvent>) -> TestLifecycle {
        TestLifecycle { sender: tx }
    }
}

impl TestLifecycle {
    fn make_handler(&self, event: LifecycleEvent) -> LifecycleHandler {
        LifecycleHandler {
            sender: self.sender.clone(),
            event: Some(event),
        }
    }
}

impl<'a> OnStart<'a, TestAgent> for TestLifecycle {
    type OnStartHandler = LifecycleHandler;

    fn on_start(&'a self) -> Self::OnStartHandler {
        self.make_handler(LifecycleEvent::Start)
    }
}

impl<'a> OnStop<'a, TestAgent> for TestLifecycle {
    type OnStopHandler = LifecycleHandler;

    fn on_stop(&'a self) -> Self::OnStopHandler {
        self.make_handler(LifecycleEvent::Stop)
    }
}

impl<'a> LaneEvent<'a, TestAgent> for TestLifecycle {
    type LaneEventHandler = LifecycleHandler;

    fn lane_event(
        &'a self,
        _context: &TestAgent,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        Some(self.make_handler(LifecycleEvent::Lane(Text::new(lane_name))))
    }
}

impl HandlerAction<TestAgent> for LifecycleHandler {
    type Completion = ();

    fn step(
        &mut self,
        spawner: &dyn Spawner<TestAgent>,
        _meta: AgentMetadata,
        context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let LifecycleHandler { sender, event } = self;
        if let Some(event) = event.take() {
            if let LifecycleEvent::Lane(name) = &event {
                if name == CMD_LANE {
                    let n = context.take_cmd();
                    let tx = sender.clone();
                    let fut = async move {
                        tx.send(LifecycleEvent::RanSuspended(n))
                            .expect("Channel closed.");
                        let h = SideEffect::from(move || {
                            tx.send(LifecycleEvent::RanSuspendedConsequence)
                                .expect("Channel closed.");
                        });
                        let boxed: BoxEventHandler<TestAgent> = Box::new(h);
                        boxed
                    };

                    spawner.spawn_suspend(fut.boxed());
                }
            }
            sender.send(event).expect("Report failed.");
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
