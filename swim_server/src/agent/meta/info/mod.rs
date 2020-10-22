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
use crate::agent::lane::lifecycle::DemandMapLaneLifecycle;
use crate::agent::lane::model::demand_map::DemandMapLane;
use crate::agent::lane::LaneKind;
use crate::agent::meta::IdentifiedAgentIo;
use crate::agent::LaneTasks;
use crate::agent::{make_demand_map_lane, AgentContext, DynamicLaneTasks, LaneIo, SwimAgent};
use crate::routing::LaneIdentifier;
use futures::future::BoxFuture;
use std::collections::HashMap;
use swim_common::form::Form;
use utilities::uri::RelativeUri;

#[derive(Form, Debug, Clone)]
pub struct LaneInfo {
    lane_uri: String,
    lane_type: LaneKind,
}

impl LaneInfo {
    pub fn new(lane_uri: String, lane_type: LaneKind) -> LaneInfo {
        LaneInfo {
            lane_uri,
            lane_type,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InfoHandler {
    info_lane: DemandMapLane<String, LaneInfo>,
}

#[cfg(test)]
pub fn make_info_handler() -> InfoHandler {
    use crate::agent::lane::model::demand_map::make_lane_model;
    use std::num::NonZeroUsize;
    use tokio::sync::mpsc;

    InfoHandler {
        info_lane: make_lane_model(NonZeroUsize::new(5).unwrap(), mpsc::channel(1).0).0,
    }
}

struct LaneInfoLifecycle {
    lanes: HashMap<String, LaneInfo>,
}

impl<'a, Agent> DemandMapLaneLifecycle<'a, String, LaneInfo, Agent> for LaneInfoLifecycle {
    type OnSyncFuture = BoxFuture<'a, Vec<String>>;
    type OnCueFuture = BoxFuture<'a, Option<LaneInfo>>;

    fn on_sync<C>(&'a self, _context: &'a C) -> Self::OnSyncFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.keys().map(Clone::clone).collect::<Vec<_>>() })
    }

    fn on_cue<C>(&'a self, _context: &'a C, key: String) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.get(&key).map(Clone::clone) })
    }
}

pub fn open_info_lanes<Config, Agent, Context>(
    uri: RelativeUri,
    exec_conf: &AgentExecutionConfig,
    lanes_summary: HashMap<String, LaneInfo>,
) -> (
    InfoHandler,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let (info_lane, lane_info_task, lane_info_io) =
        make_demand_map_lane::<Agent, Context, String, LaneInfo, LaneInfoLifecycle>(
            uri.to_string(),
            true,
            LaneInfoLifecycle {
                lanes: lanes_summary,
            },
            exec_conf.lane_buffer,
        );

    let info_handler = InfoHandler { info_lane };

    let lane_info_io = lane_info_io.unwrap().boxed();
    let mut lane_hashmap = HashMap::new();
    lane_hashmap.insert(LaneIdentifier::meta(uri.to_string()), lane_info_io);

    (info_handler, vec![lane_info_task.boxed()], lane_hashmap)
}
