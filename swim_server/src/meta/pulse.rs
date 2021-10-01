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

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::{
    make_supply_lane, AgentContext, DynamicLaneTasks, LaneIo, LaneParts, LaneTasks, SwimAgent,
};
use crate::meta::metric::lane::LanePulse;
use crate::meta::metric::node::NodePulse;
use crate::meta::metric::uplink::WarpUplinkPulse;
use crate::meta::{IdentifiedAgentIo, LaneAddressedKind, MetaNodeAddressed};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use swim_form::Form;
use swim_common::warp::path::RelativePath;
use swim_utilities::routing::uri::RelativeUri;

pub type PulseLaneOpenResult<Agent, Context> = (
    PulseLanes,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
);

pub struct PulseLanes {
    pub uplinks: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
    pub lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
    pub node: SupplyLane<NodePulse>,
}

pub type MakePulseLaneResult<V, Agent, Context> = (
    SupplyLane<V>,
    Box<dyn LaneTasks<Agent, Context>>,
    Box<dyn LaneIo<Context>>,
);

/// Creates a supply lane for `lane_uri`.
pub fn make_pulse_lane<Config, Agent, Context, V>(
    lane_uri: String,
    buffer_size: NonZeroUsize,
) -> MakePulseLaneResult<V, Agent, Context>
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    V: Any + Clone + Send + Sync + Form + Debug + Unpin,
{
    let LaneParts { lane, tasks, io } = make_supply_lane(lane_uri, true, buffer_size);
    (
        lane,
        tasks.boxed(),
        io.routing.expect("Lane returned private IO"),
    )
}

/// Opens uplink and lane supply lanes for all `agent_lanes`.
pub fn open_pulse_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    agent_lanes: &[&String],
    buffer_size: NonZeroUsize,
) -> PulseLaneOpenResult<Agent, Context>
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let len = agent_lanes.len() * 2;
    let mut tasks = Vec::with_capacity(len);
    let mut ios = HashMap::with_capacity(len);

    let mut uplinks = HashMap::new();
    let mut lanes = HashMap::new();

    // open uplink pulse lanes
    agent_lanes.iter().for_each(|lane_uri| {
        let (lane, task, io) = make_pulse_lane::<Config, Agent, Context, WarpUplinkPulse>(
            lane_uri.to_string(),
            buffer_size,
        );

        uplinks.insert(
            RelativePath::new(node_uri.to_string(), lane_uri.to_string()),
            lane,
        );
        tasks.push(task);
        ios.insert(
            LaneIdentifier::Meta(MetaNodeAddressed::UplinkProfile {
                lane_uri: lane_uri.to_string().into(),
            }),
            io,
        );
    });

    // open lane pulse lanes
    agent_lanes.iter().for_each(|lane_uri| {
        let (lane, task, io) =
            make_pulse_lane::<Config, Agent, Context, LanePulse>(lane_uri.to_string(), buffer_size);

        lanes.insert(
            RelativePath::new(node_uri.to_string(), lane_uri.to_string()),
            lane,
        );
        tasks.push(task);
        ios.insert(
            LaneIdentifier::Meta(MetaNodeAddressed::LaneAddressed {
                lane_uri: lane_uri.to_string().into(),
                kind: LaneAddressedKind::Pulse,
            }),
            io,
        );
    });

    // open node pulse lane
    let (node_lane, task, io) =
        make_pulse_lane::<Config, Agent, Context, NodePulse>("".to_string(), buffer_size);

    tasks.push(task);
    ios.insert(LaneIdentifier::Meta(MetaNodeAddressed::NodeProfile), io);

    let pulse_lanes = PulseLanes {
        uplinks,
        lanes,
        node: node_lane,
    };

    (pulse_lanes, tasks, ios)
}
