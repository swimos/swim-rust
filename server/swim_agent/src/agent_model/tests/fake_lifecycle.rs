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

use swim_model::Text;
use tokio::sync::mpsc;

use crate::{
    event_handler::{HandlerAction, StepResult},
    lifecycle::{lane_event::LaneEvent, on_start::OnStart, on_stop::OnStop},
    meta::AgentMetadata,
};

use super::fake_agent::TestAgent;

#[derive(Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    Start,
    Lane(Text),
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

    fn step(&mut self, _meta: AgentMetadata, _context: &TestAgent) -> StepResult<Self::Completion> {
        let LifecycleHandler { sender, event } = self;
        if let Some(event) = event.take() {
            sender.send(event).expect("Report failed.");
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}
