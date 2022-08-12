use std::cell::RefCell;

use swim_agent::{
    agent_lifecycle::utility::HandlerContext,
    agent_model::AgentModel,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::ValueLane,
    lifecycle, projections, AgentLaneModel,
};
use swim_api::agent::Agent;
use swim_model::Text;

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
fn main() {
    let _ = make_agent();
    println!("Hello, world!");
}

fn make_agent() -> impl Agent + Send {
    let agent_fac = MyAgent::default;
    let lifecycle = MyAgentLifecycle::default().into_lifecycle();

    AgentModel::new(agent_fac, lifecycle)
}

/*
 * This example provides a sketch for how the macros to define an agent will work. It will be remove (and replaced
 * with a real example) after the macro crate is added.
 */

#[derive(Debug, AgentLaneModel)]
#[agent_root(::swim_agent)]
#[projections]
pub struct MyAgent {
    first: ValueLane<i32>,
    second: ValueLane<Text>,
}

#[derive(Clone, Default)]
pub struct MyAgentLifecycle {
    content: RefCell<Text>,
}

#[lifecycle(MyAgent, agent_root(::swim_agent))]
impl MyAgentLifecycle {
    #[on_start]
    pub fn on_start(
        &self, //This could be &mut self at the expense of the event handlers not being able to take the lifetime of the self ref. Possibly the macro could allow both with diffrent semantics (which could be a bit confusing).
        context: HandlerContext<MyAgent>,
    ) -> impl EventHandler<MyAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting: {}", uri)))
    }

    #[on_event(first)]
    pub fn first_on_event(
        &self,
        context: HandlerContext<MyAgent>,
        value: &i32,
    ) -> impl EventHandler<MyAgent> + '_ {
        let n = *value;
        context.get_value(MyAgent::SECOND).and_then(move |text| {
            context.effect(move || {
                println!(
                    "first = {}, second = {}, content = {}",
                    n,
                    text,
                    &*self.content.borrow()
                );
            })
        })
    }
}
