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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::meta::log::{open_log_lanes, LogHandler};
use crate::agent::{AgentContext, DynamicLaneTasks, LaneIo, SwimAgent};
use crate::routing::LaneIdentifier;
use std::collections::HashMap;
use utilities::uri::RelativeUri;

pub mod info;
pub mod log;

pub const META_EDGE: &str = "swim:meta:edge";
pub const META_MESH: &str = "swim:meta:mesh";
pub const META_PART: &str = "swim:meta:part";
pub const META_HOST: &str = "swim:meta:host";
pub const META_NODE: &str = "swim:meta:node";
pub const META_LANE: &str = "swim:meta:lane";

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum MetaKind {
    Edge,
    Mesh,
    Part,
    Host,
    Node,
    Lane,
}

pub fn open_meta_lanes<Config, Agent, Context>(
    uri: RelativeUri,
    // lanes_summary: LanesSummary,
    exec_conf: &AgentExecutionConfig,
    meta_kind: MetaKind,
) -> (
    LogHandler,
    DynamicLaneTasks<Agent, Context>,
    HashMap<LaneIdentifier, Option<impl LaneIo<Context>>>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    open_log_lanes(uri.clone(), exec_conf, meta_kind)
}
