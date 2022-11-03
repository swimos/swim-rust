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

use crate::{
    config::IntrospectionConfig,
    route::{node_pattern, NODE_PARAM},
};
use futures::{
    future::{BoxFuture, Either},
    pin_mut, FutureExt, SinkExt, StreamExt,
};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult},
    error::{AgentInitError, AgentTaskError, FrameIoError},
    meta::{
        lane::{LaneInfo, LaneKind},
        uplink::NodePulse,
    },
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        map::{MapOperation, MapOperationEncoder},
        WithLenReconEncoder, WithLengthBytesCodec,
    },
};
use swim_model::Text;
use swim_runtime::agent::reporting::UplinkReportReader;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::route_uri::RouteUri,
    trigger,
};
use tokio::{select, time::Instant};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{model::AgentIntrospectionHandle, task::IntrospectionResolver};
use futures::future::select;

const PULSE_LANE: &str = "pulse";
const LANES_LANE: &str = "lanes";

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
        agent_config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        let NodeMetaAgent { config, resolver } = self;
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
    let pattern = node_pattern();
    let params = match pattern.unapply_route_uri(&route) {
        Ok(params) => params,
        Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
    };
    let node_uri = &params[NODE_PARAM];
    let handle = match resolver.resolve_agent(Text::from(node_uri)).await {
        Ok(handle) => handle,
        Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
    };
    let mut lane_config = config.default_lane_config.unwrap_or_default();
    lane_config.transient = true;
    let pulse_io = context
        .add_lane(PULSE_LANE, LaneKind::Supply, lane_config)
        .await?;
    let lanes_io = context
        .add_lane(LANES_LANE, LaneKind::DemandMap, lane_config)
        .await?;
    Ok(run_task(pulse_interval, handle, pulse_io, lanes_io).boxed())
}

type Io = (ByteWriter, ByteReader);

async fn run_task(
    pulse_interval: Duration,
    handle: AgentIntrospectionHandle,
    pulse_io: Io,
    lanes_io: Io,
) -> Result<(), AgentTaskError> {
    let report_reader = handle.aggregate_reader();
    let (shutdown_tx, shutdown_rx) = trigger::trigger();
    let pulse_lane = run_pulse_lane(shutdown_rx.clone(), pulse_interval, report_reader, pulse_io);
    let lanes_lane = run_lanes_descriptor_lane(shutdown_rx, handle, lanes_io);

    pin_mut!(pulse_lane);
    pin_mut!(lanes_lane);

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

async fn run_pulse_lane(
    shutdown_rx: trigger::Receiver,
    pulse_interval: Duration,
    report_reader: UplinkReportReader,
    pulse_io: Io,
) -> Result<(), FrameIoError> {
    let (tx, rx) = pulse_io;

    let mut input = FramedRead::new(rx, LaneRequestDecoder::new(WithLengthBytesCodec::default()))
        .take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, LaneResponseEncoder::new(WithLenReconEncoder::default()));

    let sleep = tokio::time::sleep(pulse_interval);
    pin_mut!(sleep);

    let mut previous = Instant::now();
    if report_reader.snapshot().is_none() {
        return Ok(());
    }

    loop {
        let result = select! {
            biased;
            maybe_request = input.next() => {
                if maybe_request.is_some() {
                    maybe_request
                } else {
                    break Ok(());
                }
            }
            _ = sleep.as_mut() => None,
        };

        match result.transpose()? {
            Some(LaneRequest::Sync(id)) => {
                let synced: LaneResponse<NodePulse> = LaneResponse::Synced(id);
                output.send(synced).await?;
            }
            None => {
                let new_timeout = Instant::now()
                    .checked_add(pulse_interval)
                    .expect("Timer overflow.");
                sleep.as_mut().reset(new_timeout);
                if let Some(report) = report_reader.snapshot() {
                    let now = Instant::now();
                    let diff = now.duration_since(previous);
                    previous = now;
                    let pulse = report.make_pulse(diff);
                    let node_pulse = NodePulse { uplinks: pulse };
                    output.send(LaneResponse::StandardEvent(node_pulse)).await?;
                } else {
                    break Ok(());
                }
            }
            _ => {}
        }
    }
}

async fn run_lanes_descriptor_lane(
    shutdown_rx: trigger::Receiver,
    mut handle: AgentIntrospectionHandle,
    lanes_io: Io,
) -> Result<(), FrameIoError> {
    let (tx, rx) = lanes_io;

    let mut input = FramedRead::new(rx, LaneRequestDecoder::new(WithLengthBytesCodec::default()))
        .take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, LaneResponseEncoder::new(MapOperationEncoder::default()));

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
