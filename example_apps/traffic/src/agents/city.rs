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

use swim::agent::agent_model::downlink::hosted::MapDownlinkHandle;
use swim::agent::config::MapDownlinkConfig;
use swim::agent::state::State;
use swim::agent::{
    agent_lifecycle::utility::{HandlerContext, JoinValueContext},
    event_handler::{EventHandler, HandlerActionExt},
    lanes::{join_value::lifecycle::JoinValueLaneLifecycle, CommandLane, JoinValueLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};
use swim::model::Value;
use tracing::{debug, info};

const TRAFFIC_HOST: &str =
    "warps://trafficware.swim.services?key=ab21cfe05ba-7d43-69b2-0aef-94d9d54b6f65";
const INTERSECTION_INFO: &str = "intersection/info";
const LANE_URI: &str = "intersections";

#[derive(AgentLaneModel)]
#[projections]
#[agent(transient, convention = "camel")]
pub struct CityAgent {
    //Todo Cant use Uri as it does not implement Structure Writeable
    intersections: JoinValueLane<String, Value>,
}

#[derive(Debug, Default)]
pub struct CityLifecycle {
    handle: State<CityAgent, Option<MapDownlinkHandle<String, Value>>>,
}

impl CityLifecycle {
    pub fn new() -> Self {
        CityLifecycle {
            handle: State::default(),
        }
    }
}

#[lifecycle(CityAgent, no_clone)]
impl CityLifecycle {
    #[on_start]
    fn on_start<'a>(&'a self, context: HandlerContext<CityAgent>) -> impl EventHandler<CityAgent> + 'a {
        let CityLifecycle { handle } = self;

        context.get_agent_uri().and_then(move |uri| {
            context.effect(move || {
                info!(uri = %uri, "Starting city agent.");

                // Open downlink
                context
                    .map_downlink_builder::<String, Value>(
                        Some(TRAFFIC_HOST),
                        INTERSECTION_INFO,
                        LANE_URI,
                        Default::default(),
                    )
                    .done().and_then(move |dl_handle| handle.set(Some(dl_handle)));
            })
        })
    }

    #[on_stop]
    fn stopping(&self, context: HandlerContext<CityAgent>) -> impl EventHandler<CityAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || info!(uri = %uri, "Stopping city agent.")))
    }
}
