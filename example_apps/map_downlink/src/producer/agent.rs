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

use swimos::agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt},
    lanes::MapLane,
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ProducerAgent {
    lane: MapLane<String, i32>,
}

#[derive(Clone)]
pub struct ProducerLifecycle;

#[lifecycle(ProducerAgent)]
impl ProducerLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<ProducerAgent>,
    ) -> impl EventHandler<ProducerAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| {
                context.effect(move || {
                    println!("Starting producer agent at: {}", uri);
                })
            })
            .followed_by(context.replace_map(
                ProducerAgent::LANE,
                [
                    ("apple".to_string(), 5),
                    ("pear".to_string(), 10),
                    ("banana".to_string(), 100),
                ],
            ))
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<ProducerAgent>,
    ) -> impl EventHandler<ProducerAgent> {
        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                println!("Stopping producer agent at: {}", uri);
            })
        })
    }

    #[on_update(lane)]
    pub fn on_update(
        &self,
        context: HandlerContext<ProducerAgent>,
        _map: &HashMap<String, i32>,
        key: String,
        prev: Option<i32>,
        new_value: &i32,
    ) -> impl EventHandler<ProducerAgent> + '_ {
        let v = *new_value;
        context.effect(move || {
            if let Some(p) = prev {
                println!(
                    "Updating entry for '{}' from {} to {} on producer.",
                    key, p, v
                );
            } else {
                println!("Setting entry for '{}' to {} on producer.", key, v);
            }
        })
    }

    #[on_remove(lane)]
    pub fn on_remove(
        &self,
        context: HandlerContext<ProducerAgent>,
        _map: &HashMap<String, i32>,
        key: String,
        prev: i32,
    ) -> impl EventHandler<ProducerAgent> + '_ {
        context.effect(move || {
            println!(
                "Removing entry for '{}' on producer. Previous value was {}.",
                key, prev
            );
        })
    }
}
