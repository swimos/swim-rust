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

use std::{collections::HashMap, pin::pin, time::Duration};

use crate::{config::IntrospectionConfig, meta_agent::run_pulse_lane, route::NODE_PARAM};
use futures::{
    future::{BoxFuture, Either},
    FutureExt, SinkExt, StreamExt,
};
use swimos_agent_protocol::{
    encoding::lane::{MapLaneResponseEncoder, RawValueLaneRequestDecoder},
    LaneRequest, LaneResponse, MapOperation,
};
use swimos_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, WarpLaneKind},
    error::{AgentInitError, AgentTaskError, FrameIoError},
};
use swimos_meta::{LaneInfo, NodePulse};
use swimos_model::Text;
use swimos_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::route_uri::RouteUri,
    trigger,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{model::AgentIntrospectionHandle, task::IntrospectionResolver};
use futures::future::select;

use super::{MetaRouteError, PULSE_LANE};

#[cfg(test)]
mod tests;

const LANES_LANE: &str = "lanes";

/// A meta agent providing information on the lanes of an agent and aggregate statistics on
/// the uplinks for all of its lanes. The meta agent extracts the target node URI from its own
/// node URI and then attempts to resolve the introspection handle during it's initialization
/// phase. If the node cannot be resolved, the meta-agent will fail to start with an appropriate
/// error.
pub struct NodeMetaAgent {
    config: IntrospectionConfig,
    resolver: IntrospectionResolver,
}

impl NodeMetaAgent {
    pub fn new(config: IntrospectionConfig, resolver: IntrospectionResolver) -> NodeMetaAgent {
        NodeMetaAgent { config, resolver }
    }
}

impl Agent for NodeMetaAgent {
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        agent_config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let NodeMetaAgent { config, resolver } = self;
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

    let handle = match resolver.resolve_agent(node_uri).await {
        Ok(handle) => handle,
        Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
    };
    let mut lane_config = config.default_lane_config.unwrap_or_default();
    lane_config.transient = true;
    let pulse_io = context
        .add_lane(PULSE_LANE, WarpLaneKind::Supply, lane_config)
        .await?;
    let lanes_io = context
        .add_lane(LANES_LANE, WarpLaneKind::DemandMap, lane_config)
        .await?;
    Ok(run_task(context, pulse_interval, handle, pulse_io, lanes_io).boxed())
}

type Io = (ByteWriter, ByteReader);

async fn run_task(
    context: Box<dyn AgentContext + Send>,
    pulse_interval: Duration,
    handle: AgentIntrospectionHandle,
    pulse_io: Io,
    lanes_io: Io,
) -> Result<(), AgentTaskError> {
    // deferred drop so the agent doesn't terminate early.
    let _context = context;

    let report_reader = handle.aggregate_reader();
    let (shutdown_tx, shutdown_rx) = trigger::trigger();
    let pulse_lane = pin!(run_pulse_lane(
        shutdown_rx.clone(),
        pulse_interval,
        report_reader,
        pulse_io,
        |uplinks| NodePulse { uplinks },
    ));
    let lanes_lane = pin!(run_lanes_descriptor_lane(shutdown_rx, handle, lanes_io));

    match select(pulse_lane, lanes_lane).await {
        Either::Left((result, fut)) => {
            shutdown_tx.trigger();
            let _ = fut.await;
            result.map_err(|error| AgentTaskError::BadFrame {
                lane: Text::new(PULSE_LANE),
                error,
            })
        }
        Either::Right((result, fut)) => {
            shutdown_tx.trigger();
            let _ = fut.await;
            result.map_err(|error| AgentTaskError::BadFrame {
                lane: Text::new(LANES_LANE),
                error,
            })
        }
    }
}

/// A lane that will return information on all of the lanes of an agent, as a map, when a Sync
/// request is sent to the lane.
///
/// #Arguments
/// * `shutdown_rx` - Shutdown signal for when the agent is stopping.
/// * `handle` - Introspection handle used to refresh the view of the lanes.
/// * lanes_io` - The input and output channels for the lane.
async fn run_lanes_descriptor_lane(
    shutdown_rx: trigger::Receiver,
    mut handle: AgentIntrospectionHandle,
    lanes_io: Io,
) -> Result<(), FrameIoError> {
    let (tx, rx) = lanes_io;

    let mut input =
        FramedRead::new(rx, RawValueLaneRequestDecoder::default()).take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, MapLaneResponseEncoder::default());

    let mut snapshot = if let Some(s) = handle.new_snapshot() {
        s
    } else {
        return Ok(());
    };

    while let Some(request) = input.next().await.transpose()? {
        if let LaneRequest::Sync(id) = request {
            if handle.changed() {
                snapshot = if let Some(s) = handle.new_snapshot() {
                    s
                } else {
                    return Ok(());
                };
            }

            for lane_info in snapshot.lane_info() {
                let op = MapOperation::Update {
                    key: lane_info.lane_uri.as_str(),
                    value: &lane_info,
                };
                output.send(LaneResponse::SyncEvent(id, op)).await?;
            }
            let synced: LaneResponse<MapOperation<&str, &LaneInfo>> = LaneResponse::Synced(id);
            output.send(synced).await?;
        }
    }
    Ok(())
}
