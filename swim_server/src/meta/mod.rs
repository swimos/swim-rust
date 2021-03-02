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
pub mod uri;

#[cfg(test)]
mod tests;

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AgentContext, DynamicLaneTasks, LaneIo, SwimAgent};
use crate::meta::info::{open_info_lane, LaneInfo, LaneInformation};
use crate::meta::log::{open_log_lanes, LogLevel, NodeLogger};
use crate::meta::uri::{parse, MetaParseErr};
use lazy_static::lazy_static;
use percent_encoding::percent_decode_str;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use swim_common::model::text::Text;
use swim_common::warp::path::RelativePath;
use utilities::uri::RelativeUri;

lazy_static! {
    static ref META_PATTERN: Regex =
        Regex::new(r"(/?)(swim:meta:)(edge|mesh|part|host|node)(?P<node>/[^/]+/)")
            .expect("Failed to compile meta pattern");
}

const NODE_CAPTURE_GROUP: &str = "node";

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
}

impl MetaContext {
    fn new(lane_information: LaneInformation, node_logger: NodeLogger) -> MetaContext {
        MetaContext {
            lane_information,
            node_logger,
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

    let (node_logger, log_tasks, log_ios) = open_log_lanes(node_uri, exec_conf.node_log);

    tasks.extend(log_tasks);
    ios.extend(log_ios);

    let (lane_information, info_tasks, info_ios) =
        open_info_lane(exec_conf.lane_buffer, lanes_summary);

    tasks.extend(info_tasks);
    ios.extend(info_ios);

    let meta_context = MetaContext::new(lane_information, node_logger);

    (meta_context, tasks, ios)
}

#[cfg(test)]
pub(crate) fn make_test_meta_context() -> MetaContext {
    use crate::agent::lane::model::demand_map::DemandMapLane;
    use crate::meta::log::make_node_logger;
    use tokio::sync::mpsc;

    MetaContext {
        lane_information: LaneInformation::new(DemandMapLane::new(
            mpsc::channel(1).0,
            mpsc::channel(1).0,
        )),
        node_logger: make_node_logger(RelativeUri::default()),
    }
}
