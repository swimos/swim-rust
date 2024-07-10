// Copyright 2015-2024 Swim Inc.
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
use swimos_api::address::Address;
use swimos_model::Text;
use tokio::sync::mpsc;

use crate::{
    agent_lifecycle::{item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop},
    event_handler::{
        ActionContext, EventHandler, HandlerAction, LocalBoxEventHandler, SideEffect, Spawner,
        StepResult,
    },
    meta::AgentMetadata,
};

use super::{fake_agent::TestAgent, AD_HOC_HOST, AD_HOC_LANE, AD_HOC_NODE, CMD_LANE, HTTP_LANE};

#[derive(Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    Init,
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

#[derive(Clone)]
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

impl OnInit<TestAgent> for TestLifecycle {
    fn initialize(
        &self,
        _action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        _context: &TestAgent,
    ) {
        assert!(self.sender.send(LifecycleEvent::Init).is_ok());
    }
}

impl OnStart<TestAgent> for TestLifecycle {
    fn on_start(&self) -> impl EventHandler<TestAgent> + '_ {
        self.make_handler(LifecycleEvent::Start)
    }
}

impl OnStop<TestAgent> for TestLifecycle {
    fn on_stop(&self) -> impl EventHandler<TestAgent> + '_ {
        self.make_handler(LifecycleEvent::Stop)
    }
}

impl ItemEvent<TestAgent> for TestLifecycle {
    type ItemEventHandler<'a> = LifecycleHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &TestAgent,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        Some(self.make_handler(LifecycleEvent::Lane(Text::new(item_name))))
    }
}

impl HandlerAction<TestAgent> for LifecycleHandler {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<TestAgent>,
        _meta: AgentMetadata,
        context: &TestAgent,
    ) -> StepResult<Self::Completion> {
        let LifecycleHandler { sender, event } = self;
        if let Some(event) = event.take() {
            if let LifecycleEvent::Lane(name) = &event {
                match name.as_str() {
                    CMD_LANE => {
                        let n = context.take_cmd();
                        if n % 2 == 0 {
                            let tx = sender.clone();
                            let fut = async move {
                                tx.send(LifecycleEvent::RanSuspended(n))
                                    .expect("Channel closed.");
                                let h = SideEffect::from(move || {
                                    tx.send(LifecycleEvent::RanSuspendedConsequence)
                                        .expect("Channel closed.");
                                });
                                let boxed: LocalBoxEventHandler<TestAgent> = Box::new(h);
                                boxed
                            };
                            action_context.spawn_suspend(fut.boxed());
                        } else {
                            let address = Address::new(Some(AD_HOC_HOST), AD_HOC_NODE, AD_HOC_LANE);
                            action_context.send_command(address, "content", true);
                        }
                    }
                    HTTP_LANE => {
                        context.satisfy_http_requests();
                    }
                    _ => {}
                }
            }
            sender.send(event).expect("Report failed.");
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
