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

use std::time::Duration;

use futures::{future::BoxFuture, FutureExt};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult},
    error::{AgentInitError, AgentTaskError},
    meta::{lane::LaneKind, uplink::LanePulse},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::route_uri::RouteUri,
    trigger,
};

use crate::{
    config::IntrospectionConfig,
    model::LaneView,
    route::{lane_pattern, LANE_PARAM, NODE_PARAM},
    task::IntrospectionResolver,
};

use super::{run_pulse_lane, PULSE_LANE};

#[cfg(test)]
mod tests;

pub struct LaneMetaAgent {
    config: IntrospectionConfig,
    resolver: IntrospectionResolver,
}

impl LaneMetaAgent {
    pub fn new(config: IntrospectionConfig, resolver: IntrospectionResolver) -> Self {
        LaneMetaAgent { config, resolver }
    }
}

impl Agent for LaneMetaAgent {
    fn run(
        &self,
        route: RouteUri,
        agent_config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let LaneMetaAgent { config, resolver } = self;
        run_init(
            config.node_pulse_interval,
            resolver.clone(),
            route,
            agent_config,
            context,
        )
        .boxed()
    }
}

async fn run_init(
    pulse_interval: Duration,
    resolver: IntrospectionResolver,
    route: RouteUri,
    config: AgentConfig,
    context: Box<dyn AgentContext + Send>,
) -> AgentInitResult {
    let pattern = lane_pattern();
    let params = match pattern.unapply_route_uri(&route) {
        Ok(params) => params,
        Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
    };
    let node_uri = &params[NODE_PARAM];
    let lane_name = &params[LANE_PARAM];
    let view = match resolver
        .resolve_lane(Text::new(node_uri), Text::new(lane_name))
        .await
    {
        Ok(view) => view,
        Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
    };
    let mut lane_config = config.default_lane_config.unwrap_or_default();
    lane_config.transient = true;
    let pulse_io = context
        .add_lane(PULSE_LANE, LaneKind::Supply, lane_config)
        .await?;

    Ok(run_task(pulse_interval, view, pulse_io).boxed())
}

type Io = (ByteWriter, ByteReader);

async fn run_task(
    pulse_interval: Duration,
    view: LaneView,
    pulse_io: Io,
) -> Result<(), AgentTaskError> {
    let (_shutdown_tx, shutdown_rx) = trigger::trigger();
    run_pulse_lane(
        shutdown_rx,
        pulse_interval,
        view.report_reader,
        pulse_io,
        |uplink_pulse| LanePulse { uplink_pulse },
    )
    .await
    .map_err(|error| AgentTaskError::BadFrame {
        lane: Text::new(PULSE_LANE),
        error,
    })
}
