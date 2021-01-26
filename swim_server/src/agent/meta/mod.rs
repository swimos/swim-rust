// Copyright 2015-2020 SWIM.AI inc.
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

#[cfg(test)]
mod tests;

mod info;
mod uri;

pub(crate) mod lane;
pub(crate) mod log;

pub use info::LaneInfo;
pub use log::LogLevel;

use self::uri::parse;
use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::meta::info::{open_info_lane, InfoHandler};
use crate::agent::meta::log::{open_log_lanes, LogHandler};
use crate::agent::LaneIo;
use crate::agent::{AgentContext, DynamicLaneTasks, SwimAgent};
use crate::routing::LaneIdentifier;
use std::collections::HashMap;
use std::fmt::Debug;
use swim_common::model::text::Text;
use swim_common::warp::path::RelativePath;
use utilities::uri::RelativeUri;

pub const META_EDGE: &str = "swim:meta:edge";
pub const META_MESH: &str = "swim:meta:mesh";
pub const META_PART: &str = "swim:meta:part";
pub const META_HOST: &str = "swim:meta:host";
pub const META_NODE: &str = "swim:meta:node";

pub const LANES_URI: &str = "lanes";

pub type IdentifiedAgentIo<Context> = HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>;

#[derive(Clone, Debug)]
pub struct MetaContext {
    log_handler: LogHandler,
    info_handler: InfoHandler,
}

impl MetaContext {
    fn new(log_handler: LogHandler, info_handler: InfoHandler) -> MetaContext {
        MetaContext {
            log_handler,
            info_handler,
        }
    }

    pub fn log_handler(&self) -> &LogHandler {
        &self.log_handler
    }

    pub fn info_handler(&self) -> &InfoHandler {
        &self.info_handler
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaAddressed {
    /// swim:meta:edge
    Edge,
    /// swim:meta:mesh
    Mesh,
    /// swim:meta:part
    Part,
    /// swim:meta:host
    Host,
    /// swim:meta:node
    Node(MetaNodeAddressed),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetaNodeAddressed {
    /// swim:meta:node/percent-encoded-nodeuri/pulse
    /// Eg: swim:meta:node/unit%2Ffoo/pulse
    NodeProfile { node_uri: Text },
    /// swim:meta:node/percent-encoded-nodeuri/lane/lane-uri/uplink
    /// Eg: swim:meta:node/unit%2Ffoo/lane/bar/uplink
    UplinkProfile { node_uri: Text, lane_uri: Text },
    /// swim:meta:node/percent-encoded-nodeuri/lanes
    /// Eg: swim:meta:node/unit%2Ffoo/lanes/
    Lanes { node_uri: Text },
    /// swim:meta:node/percent-encoded-nodeuri/lane-uri/traceLog
    /// Eg: swim:meta:node/unit%2Ffoo/traceLog
    Log { node_uri: Text, level: LogLevel },
}

impl MetaNodeAddressed {
    pub fn decoded_relative_path(&self) -> RelativePath {
        match self {
            MetaNodeAddressed::NodeProfile { node_uri } => {
                RelativePath::new(format!("/{}", node_uri), "/pulse".to_string())
            }
            MetaNodeAddressed::UplinkProfile { node_uri, lane_uri } => RelativePath::new(
                format!("/{}", node_uri),
                format!("/lane/{}/uplink", lane_uri),
            ),
            MetaNodeAddressed::Lanes { node_uri } => {
                RelativePath::new(format!("/{}", node_uri), "/lanes".to_string())
            }
            MetaNodeAddressed::Log { node_uri, level } => {
                RelativePath::new(format!("/{}", node_uri), format!("/{}", level.uri_ref()))
            }
        }
    }

    pub fn node_uri_ref(&self) -> &Text {
        match self {
            MetaNodeAddressed::NodeProfile { node_uri, .. } => &node_uri,
            MetaNodeAddressed::UplinkProfile { node_uri, .. } => &node_uri,
            MetaNodeAddressed::Lanes { node_uri, .. } => &node_uri,
            MetaNodeAddressed::Log { node_uri, .. } => &node_uri,
        }
    }
}

pub(crate) trait MetaPath {
    fn try_into_meta(&self) -> Result<MetaAddressed, RelativePath>;
}

impl MetaPath for RelativePath {
    fn try_into_meta(&self) -> Result<MetaAddressed, RelativePath> {
        let RelativePath { node, lane } = self;
        parse(node.as_str(), lane.as_str()).map_err(|_| RelativePath::new(node, lane))
    }
}

pub fn open_meta_lanes<Config, Agent, Context>(
    uri: RelativeUri,
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
    let mut tasks = Vec::with_capacity(6);
    let mut ios = HashMap::with_capacity(6);

    let (log_handler, log_tasks, log_ios) = open_log_lanes(uri, exec_conf);
    log_tasks.into_iter().for_each(|t| {
        tasks.push(t);
    });
    log_ios.into_iter().for_each(|(k, v)| {
        ios.insert(k, v);
    });

    let (info_handler, info_tasks, info_ios) = open_info_lane(exec_conf, lanes_summary);
    info_tasks.into_iter().for_each(|t| {
        tasks.push(t);
    });
    info_ios.into_iter().for_each(|(k, v)| {
        ios.insert(k, v);
    });

    let meta_context = MetaContext::new(log_handler, info_handler);

    (meta_context, tasks, ios)
}

#[cfg(test)]
pub(crate) fn make_test_meta_context(uri: RelativeUri) -> MetaContext {
    use self::info::make_info_handler;
    use self::log::make_log_handler;

    MetaContext {
        log_handler: make_log_handler(uri),
        info_handler: make_info_handler(),
    }
}
