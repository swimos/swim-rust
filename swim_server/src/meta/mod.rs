// Copyright 2015-2021 SWIM.AI inc.
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

pub mod info;
pub mod log;
pub mod metric;
pub mod pulse;
pub mod uri;

#[cfg(test)]
mod tests;

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AgentContext, DynamicLaneTasks, Eff, LaneIo, SwimAgent};
use crate::meta::info::{open_info_lane, LaneInfo, LaneInformation};
use crate::meta::log::{open_log_lanes, LogLevel, NodeLogger};
use crate::meta::metric::NodeMetricAggregator;
use crate::meta::pulse::open_pulse_lanes;
use crate::meta::uri::{parse, MetaParseErr};
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use lazy_static::lazy_static;
use percent_encoding::percent_decode_str;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use swim_common::model::text::Text;
use swim_common::warp::path::RelativePath;

use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use tracing::{span, Level};
use tracing_futures::{Instrument, Instrumented};

lazy_static! {
    static ref META_PATTERN: Regex =
        Regex::new(r"(/?)(swim:meta:)(edge|mesh|part|host|node)(?P<node>/[^/]+/)")
            .expect("Failed to compile meta pattern");
}

const NODE_CAPTURE_GROUP: &str = "node";

const AGGREGATOR_TASK: &str = "Aggregator task";
pub const LANE_URI: &str = "lane";
pub const LANES_URI: &str = "lanes";
pub const PULSE_URI: &str = "pulse";
pub const UPLINK_URI: &str = "uplink";

pub const META_NODE: &str = "swim:meta:node";

pub type IdentifiedAgentIo<Context> = HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>;

/// Attempts to decode a meta-encoded node URI. Returns a decoded node URI if `uri` matches or
/// returns `uri`.
pub fn get_route(uri: RelativeUri) -> RelativeUri {
    let captures = META_PATTERN.captures(uri.path());
    match captures {
        Some(captures) => match captures.name(NODE_CAPTURE_GROUP) {
            Some(node) => match percent_decode_str(node.as_str()).decode_utf8() {
                Ok(decoded) => RelativeUri::from_str(decoded.as_ref()).unwrap_or(uri),
                Err(_) => uri,
            },
            None => uri,
        },
        None => uri,
    }
}

#[derive(Debug)]
pub struct MetaContext {
    lane_information: LaneInformation,
    node_logger: NodeLogger,
    metric_aggregator: NodeMetricAggregator,
}

impl MetaContext {
    fn new(
        lane_information: LaneInformation,
        node_logger: NodeLogger,
        metric_aggregator: NodeMetricAggregator,
    ) -> MetaContext {
        MetaContext {
            lane_information,
            node_logger,
            metric_aggregator,
        }
    }

    pub fn node_logger(&self) -> NodeLogger {
        self.node_logger.clone()
    }

    #[allow(dead_code)]
    // todo
    pub fn lane_information(&self) -> &LaneInformation {
        &self.lane_information
    }

    pub fn metrics(&self) -> NodeMetricAggregator {
        self.metric_aggregator.clone()
    }
}

/// Node-addressed metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaNodeAddressed {
    /// A node's pulse.
    ///
    /// swim:meta:node/percent-encoded-nodeuri/pulse
    /// Eg: swim:meta:node/unit%2Ffoo/pulse
    NodeProfile,
    /// Uplink pulse.
    ///
    /// swim:meta:node/percent-encoded-nodeuri/lane/lane-uri/uplink
    /// Eg: swim:meta:node/unit%2Ffoo/lane/bar/uplink
    // todo: replace as 'uplinks' to return all uplinks for the lane
    UplinkProfile { lane_uri: Text },
    /// Lane addressed routes: pulse/logs.
    ///
    /// swim:meta:node/percent-encoded-nodeuri/lane/node-lane-uri/lane-uri
    /// Eg: swim:meta:node/unit%2Ffoo/lane/bar/traceLog
    LaneAddressed {
        lane_uri: Text,
        kind: LaneAddressedKind,
    },
    /// A node's lanes.
    ///
    /// swim:meta:node/percent-encoded-nodeuri/lanes
    /// Eg: swim:meta:node/unit%2Ffoo/lanes/
    Lanes,
    /// Node-level logs.
    ///
    /// swim:meta:node/percent-encoded-nodeuri/lane-uri/traceLog
    /// Eg: swim:meta:node/unit%2Ffoo/bar/traceLog
    NodeLog(LogLevel),
}

impl Display for MetaNodeAddressed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaNodeAddressed::NodeProfile => {
                write!(f, "NodePulse")
            }
            MetaNodeAddressed::UplinkProfile { lane_uri } => {
                write!(f, "UplinkProfile(lane_uri: \"{}\")", lane_uri)
            }
            MetaNodeAddressed::LaneAddressed { lane_uri, kind } => {
                write!(f, "Lane(lane_uri: \"{}\", kind: {})", lane_uri, kind)
            }
            MetaNodeAddressed::Lanes => {
                write!(f, "Lanes")
            }
            MetaNodeAddressed::NodeLog(level) => {
                write!(f, "Log(level: {})", level)
            }
        }
    }
}

/// Lane-addressed metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LaneAddressedKind {
    /// A lane's pulse.
    Pulse,
    /// Lane-level logs.
    Log(LogLevel),
}

impl Display for LaneAddressedKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneAddressedKind::Pulse => write!(f, "Pulse"),
            LaneAddressedKind::Log(level) => write!(f, "Log(level: {})", level),
        }
    }
}

impl MetaNodeAddressed {
    /// Attempts to parse `path` into a metadata route.
    pub fn try_from_relative(path: &RelativePath) -> Result<MetaNodeAddressed, MetaParseErr> {
        let RelativePath { node, lane } = path;
        let node_uri = RelativeUri::from_str(node.as_str())?;

        parse(node_uri, lane.as_str())
    }
}

pub fn open_meta_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    exec_conf: &AgentExecutionConfig,
    lanes_summary: HashMap<String, LaneInfo>,
    stop_rx: trigger::Receiver,
    task_manager: &FuturesUnordered<Instrumented<Eff>>,
) -> (
    MetaContext,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let mut tasks = Vec::new();
    let mut ios = HashMap::new();

    let (node_logger, log_tasks, log_ios) = open_log_lanes(
        node_uri.clone(),
        exec_conf.node_log,
        stop_rx.clone(),
        exec_conf.yield_after,
        task_manager,
    );

    tasks.extend(log_tasks);
    ios.extend(log_ios);

    let lanes = lanes_summary.keys().into_iter().collect::<Vec<_>>();

    let (pulse_lanes, pulse_tasks, pulse_io) =
        open_pulse_lanes(node_uri.clone(), lanes.as_slice(), exec_conf.lane_buffer);

    let (lane_information, info_tasks, info_ios) =
        open_info_lane(exec_conf.lane_buffer, lanes_summary);

    let (aggregator, aggregator_task) = NodeMetricAggregator::new(
        node_uri.clone(),
        stop_rx,
        exec_conf.metrics.clone(),
        pulse_lanes,
        node_logger.clone(),
    );

    tasks.extend(info_tasks);
    tasks.extend(pulse_tasks);
    ios.extend(info_ios);
    ios.extend(pulse_io);

    let aggregator_task = async move {
        let _ = aggregator_task.await;
    };

    task_manager.push(aggregator_task.boxed().instrument(span!(
        Level::DEBUG,
        AGGREGATOR_TASK,
        ?node_uri
    )));

    let meta_context = MetaContext::new(lane_information, node_logger, aggregator);

    (meta_context, tasks, ios)
}

/// Sinking meta context that will do nothing
#[cfg(test)]
pub(crate) fn meta_context_sink() -> MetaContext {
    use crate::agent::lane::model::demand_map::DemandMapLane;
    use crate::agent::model::supply::SupplyLane;
    use crate::meta::log::make_node_logger;
    use crate::meta::metric::config::MetricAggregatorConfig;
    use crate::meta::pulse::PulseLanes;
    use tokio::sync::mpsc;

    let (_tx, rx) = trigger::trigger();
    let node_logger = make_node_logger(RelativeUri::default());
    let pulse_lanes = PulseLanes {
        uplinks: Default::default(),
        lanes: Default::default(),
        node: SupplyLane::new(mpsc::channel(1).0),
    };

    let (metric_aggregator, _) = NodeMetricAggregator::new(
        RelativeUri::default(),
        rx,
        MetricAggregatorConfig::default(),
        pulse_lanes,
        node_logger.clone(),
    );

    MetaContext {
        lane_information: LaneInformation::new(DemandMapLane::new(
            mpsc::channel(1).0,
            mpsc::channel(1).0,
        )),
        node_logger: make_node_logger(RelativeUri::default()),
        metric_aggregator,
    }
}

#[cfg(test)]
pub async fn accumulate_metrics<E>(receiver: tokio::sync::mpsc::Receiver<E>) -> E
where
    E: std::ops::Add<E, Output = E> + Default + PartialEq + Debug,
{
    use futures::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    let mut accumulated = E::default();
    let mut stream = ReceiverStream::new(receiver);

    while let Some(item) = stream.next().await {
        accumulated = accumulated.add(item);
    }

    accumulated
}
