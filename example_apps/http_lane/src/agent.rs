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

use swimos::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{EventHandler, HandlerAction, HandlerActionExt},
    lanes::{
        http::{HttpRequestContext, Response, UnitResponse},
        SimpleHttpLane, ValueLane,
    },
    lifecycle, projections, AgentLaneModel,
};

#[derive(AgentLaneModel)]
#[projections]
pub struct ExampleAgent {
    value_lane: ValueLane<i32>,
    http_lane: SimpleHttpLane<i32>,
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

    #[on_event(value_lane)]
    pub fn on_event(
        &self,
        context: HandlerContext<ExampleAgent>,
        value: &i32,
    ) -> impl EventHandler<ExampleAgent> {
        let n = *value;
        context.effect(move || {
            println!("Setting value to: {}", n);
        })
    }

    #[on_get(http_lane)]
    fn get_from_value_lane(
        &self,
        context: HandlerContext<ExampleAgent>,
        _http_context: HttpRequestContext,
    ) -> impl HandlerAction<ExampleAgent, Completion = Response<i32>> + '_ {
        context
            .get_value(ExampleAgent::VALUE_LANE)
            .map(Response::from)
    }

    #[on_put(http_lane)]
    fn put_value_to_lane(
        &self,
        context: HandlerContext<ExampleAgent>,
        _http_context: HttpRequestContext,
        value: i32,
    ) -> impl HandlerAction<ExampleAgent, Completion = UnitResponse> + '_ {
        context
            .set_value(ExampleAgent::VALUE_LANE, value)
            .followed_by(context.value(UnitResponse::default()))
    }
}
