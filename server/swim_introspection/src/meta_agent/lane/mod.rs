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

use std::{collections::HashMap, time::Duration};

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
    route::{LANE_PARAM, NODE_PARAM},
    task::IntrospectionResolver,
};

use super::{run_pulse_lane, MetaRouteError, PULSE_LANE};

#[cfg(test)]
mod tests;

/// A meta agent providing statistics on the uplinks for a single lane. The meta agent extracts
/// the target node URI and lane from its own node URI and then attempts to resolve the
/// introspection view during it's initialization phase. If the lane cannot be resolved, the
/// meta-agent will fail to start with an appropriate error.
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
        route_params: HashMap<String, String>,
        agent_config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let LaneMetaAgent { config, resolver } = self;
        run_init(
            config.node_pulse_interval,
            resolver.clone(),
            route,
            route_params,
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
    route_params: HashMap<String, String>,
    config: AgentConfig,
    context: Box<dyn AgentContext + Send>,
) -> AgentInitResult {
    let node_uri = if let Some(node_uri) = route_params.get(NODE_PARAM) {
        Text::new(node_uri)
    } else {
        return Err(AgentInitError::UserCodeError(Box::new(
            MetaRouteError::new(route, NODE_PARAM),
        )));
    };
    let lane_name = if let Some(lane_name) = route_params.get(LANE_PARAM) {
        Text::new(lane_name)
    } else {
        return Err(AgentInitError::UserCodeError(Box::new(
            MetaRouteError::new(route, LANE_PARAM),
        )));
    };

    let view = match resolver.resolve_lane(node_uri, lane_name).await {
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
