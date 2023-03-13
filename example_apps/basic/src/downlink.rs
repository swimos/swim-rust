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

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    config::SimpleDownlinkConfig,
    event_handler::{EventHandler, HandlerActionExt, OpenValueDownlink},
};

use crate::agent::ExampleAgent;

pub fn make_downlink(
    context: HandlerContext<ExampleAgent>,
    lane: &str,
) -> impl OpenValueDownlink<ExampleAgent, i32> {
    let builder = context.value_downlink_builder::<i32>(
        Some("swim://example.remote:8080"),
        "/node",
        lane,
        SimpleDownlinkConfig::default(),
    );

    builder
        .on_linked(my_downlink_linked)
        .on_event(my_downlink_event)
        .done()
}

fn my_downlink_linked(context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Downlink linked."))
}

fn my_downlink_event(
    context: HandlerContext<ExampleAgent>,
    n: &i32,
) -> impl EventHandler<ExampleAgent> {
    let value = *n;
    context
        .effect(move || {
            println!("Received {} on downlink.", value);
        })
        .followed_by(context.set_value(ExampleAgent::FROM_REMOTE, value))
}
