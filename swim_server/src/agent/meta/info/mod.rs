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
use crate::agent::meta::lane::make_meta_demand_map_lane;
use crate::agent::meta::{IdentifiedAgentIo, LANES_URI};
use crate::agent::LaneTasks;
use crate::agent::{AgentContext, DynamicLaneTasks, LaneIo, SwimAgent};
use crate::routing::LaneIdentifier;
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use std::collections::HashMap;
use swim_common::form::Form;

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
    use tokio::sync::mpsc;

    InfoHandler {
        info_lane: DemandMapLane::new(mpsc::channel(1).0, mpsc::channel(1).0),
    }
}

struct LaneInfoLifecycle {
    lanes: HashMap<String, LaneInfo>,
}

impl<'a, Agent> DemandMapLaneLifecycle<'a, String, LaneInfo, Agent> for LaneInfoLifecycle {
    type OnSyncFuture = BoxFuture<'a, Vec<String>>;
    type OnCueFuture = BoxFuture<'a, Option<LaneInfo>>;
    type OnRemoveFuture = BoxFuture<'a, ()>;

    fn on_sync<C>(
        &'a self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
    ) -> Self::OnSyncFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.keys().map(Clone::clone).collect::<Vec<_>>() })
    }

    fn on_cue<C>(
        &'a self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
        key: String,
    ) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.get(&key).map(Clone::clone) })
    }

    fn on_remove<C>(
        &'a self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
        _key: String,
    ) -> Self::OnRemoveFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        ready(()).boxed()
    }
}

pub fn open_info_lane<Config, Agent, Context>(
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
    let (info_lane, lane_info_task, lane_info_io) = make_meta_demand_map_lane(
        LANES_URI.to_string(),
        true,
        exec_conf.lane_buffer,
        lanes_summary,
    );

    let info_handler = InfoHandler { info_lane };

    let lane_info_io = lane_info_io.unwrap().boxed();
    let mut lane_hashmap = HashMap::new();
    lane_hashmap.insert(LaneIdentifier::meta(LANES_URI.to_string()), lane_info_io);

    (info_handler, vec![lane_info_task.boxed()], lane_hashmap)
}
