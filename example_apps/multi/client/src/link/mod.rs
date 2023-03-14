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

use swim::{
    agent::{
        agent_lifecycle::utility::HandlerContext,
        event_handler::{EventHandler},
        lanes::ValueLane,
        lifecycle, projections, AgentLaneModel,
    },
    route::RouteUri,
};

use crate::LANE_NAME;

#[derive(AgentLaneModel)]
#[projections]
pub struct LinkAgent {
    from_remote: ValueLane<i32>,
}

#[derive(Clone)]
pub struct LinkLifecycle {
    remote: String,
    node: RouteUri,
}

impl LinkLifecycle {
    pub fn new(remote: &str, node: RouteUri) -> Self {
        LinkLifecycle { remote: remote.to_string(), node }
    }
}

#[lifecycle(LinkAgent)]
impl LinkLifecycle {
    
    #[on_start]
    pub fn open_link(&self, context: HandlerContext<LinkAgent>) -> impl EventHandler<LinkAgent> {
        let LinkLifecycle { remote, node } = self;
        context.event_downlink_builder::<i32>(Some(&remote), node.as_str(), LANE_NAME, Default::default())
            .on_linked(|context| context.effect(|| println!("Downlink linked!")))
            .on_unlinked(|context| context.effect(|| println!("Downlink unlinked!")))
            .on_failed(|context| context.effect(|| println!("Downlink Failed!")))
            .on_event(|context, value| context.set_value(LinkAgent::FROM_REMOTE, value))
            .done()
    }

    #[on_event(from_remote)]
    fn received_value(
        &self,
        context: HandlerContext<LinkAgent>,
        value: &i32,
    ) -> impl EventHandler<LinkAgent> {
        let n = *value;
        context.effect(move || println!("Lane set to: {}", n))
    }
}
