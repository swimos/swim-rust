// Copyright 2015-2023 Swim Inc.
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

use std::collections::HashMap;

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::MapLane,
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    lane: MapLane<i32, String>,
}

#[derive(Clone)]
pub struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Starting agent at: {}", uri);
            })
        })
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ExampleAgent>,
    ) -> impl EventHandler<ExampleAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping agent at: {}", uri);
            })
        })
    }

    #[on_update(lane)]
    pub fn on_update(
        &self,
        context: HandlerContext<ExampleAgent>,
        _map: &HashMap<i32, String>,
        key: i32,
        prev: Option<String>,
        new_value: &str,
    ) -> impl EventHandler<ExampleAgent> + '_ {
        let v = new_value.to_string();
        context.effect(move || {
            if let Some(p) = prev {
                println!("Updating entry for {} from '{}' to '{}'.", key, p, v);
            } else {
                println!("Setting entry for {} to '{}'.", key, v);
            }
        })
    }

    #[on_remove(lane)]
    pub fn on_remove(
        &self,
        context: HandlerContext<ExampleAgent>,
        _map: &HashMap<i32, String>,
        key: i32,
        prev: String,
    ) -> impl EventHandler<ExampleAgent> + '_ {
        context.effect(move || {
            println!("Removing entry for {}. Previous value was '{}'.", key, prev);
        })
    }

    #[on_clear(lane)]
    pub fn on_clear(
        &self,
        context: HandlerContext<ExampleAgent>,
        _prev: HashMap<i32, String>,
    ) -> impl EventHandler<ExampleAgent> + '_ {
        context.effect(|| {
            println!("Map was cleared.");
        })
    }
}
