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

mod aggregator;
pub mod config;
mod lane;
mod node;
mod uplink;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use swim_common::warp::path::RelativePath;
use utilities::sync::trigger;

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::LaneIo;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::{Debug, Display};
use utilities::uri::RelativeUri;

use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::model::supply::supplier::Dropping;
use crate::meta::metric::aggregator::AggregatorTask;
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::lane::{LaneAggregatorTask, LanePulse};
use crate::meta::metric::node::{NodeAggregatorTask, NodePulse};
use crate::meta::metric::uplink::{
    uplink_observer, TaggedWarpUplinkProfile, UplinkAggregatorTask, UplinkProfileSender,
    WarpUplinkPulse,
};
use crate::meta::{IdentifiedAgentIo, LaneAddressedKind, MetaNodeAddressed};
use futures::future::try_join3;
pub use lane::LaneProfile;
pub use node::NodeProfile;
use std::any::Any;
use swim_common::form::Form;
pub use uplink::{UplinkActionObserver, UplinkEventObserver, WarpUplinkProfile};

const REMOVING_LANE: &str = "Lane closed, removing";
const LANE_NOT_FOUND: &str = "Lane not found";
const STOP_OK: &str = "Aggregator stopped normally";
const STOP_CLOSED: &str = "Aggregator event stream unexpectedly closed";

/// An observer for node, lane and uplinks which generates profiles based on the events for the
/// part. These events are aggregated and forwarded to their corresponding lanes as pulses.
pub struct MetricAggregator {
    observer: MetricObserver,
    _aggregator_task: JoinHandle<Result<(), AggregatorError>>,
}

impl Debug for MetricAggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricAggregator")
            .field("observer", &self.observer)
            .finish()
    }
}

pub struct AggregatorError {
    aggregator: AggregatorKind,
    error: AggregatorErrorKind,
}

impl Display for AggregatorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AggregatorKind {
    Node,
    Lane,
    Uplink,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AggregatorErrorKind {
    ForwardChannelClosed,
    AbnormalStop,
}

impl Display for AggregatorErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregatorErrorKind::AbnormalStop => write!(f, "Collected stopped abnormally"),
            AggregatorErrorKind::ForwardChannelClosed => {
                write!(f, "Aggregator's forward channel closed")
            }
        }
    }
}

impl MetricAggregator {
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        config: MetricAggregatorConfig,
        uplink_pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
        lane_pulse_lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
        agent_pulse: SupplyLane<NodePulse>,
    ) -> MetricAggregator {
        let (node_tx, node_rx) = mpsc::channel(config.buffer_size.get());
        let node_aggregator = NodeAggregatorTask::new(stop_rx.clone(), node_rx, agent_pulse);

        let (lane_tx, lane_rx) = mpsc::channel(config.buffer_size.get());
        let lane_aggregator = AggregatorTask::new(
            node_id.clone(),
            stop_rx.clone(),
            LaneAggregatorTask::new(lane_pulse_lanes),
            lane_rx,
            Some(node_tx),
        );

        let (uplink_tx, uplink_rx) = mpsc::channel(config.buffer_size.get());

        let uplink_aggregator = AggregatorTask::new(
            node_id,
            stop_rx,
            UplinkAggregatorTask::new(uplink_pulse_lanes),
            uplink_rx,
            Some(lane_tx),
        );
        let observer = MetricObserver::new(config.clone(), uplink_tx);

        let jh = async move {
            try_join3(
                node_aggregator.run(config.yield_after),
                lane_aggregator.run(config.yield_after),
                uplink_aggregator.run(config.yield_after),
            )
            .await
            .map(|_| ())
        };

        MetricAggregator {
            observer,
            _aggregator_task: tokio::spawn(jh),
        }
    }

    /// Returns a handle which can be used to create uplink, lane, or node observers.
    pub fn observer(&self) -> MetricObserver {
        self.observer.clone()
    }
}

#[derive(Clone, Debug)]
pub struct MetricObserver {
    config: MetricAggregatorConfig,
    metric_tx: Sender<TaggedWarpUplinkProfile>,
}

impl MetricObserver {
    pub fn new(
        config: MetricAggregatorConfig,
        metric_tx: Sender<TaggedWarpUplinkProfile>,
    ) -> MetricObserver {
        MetricObserver { config, metric_tx }
    }

    /// Returns a new `UplinkObserver` for the provided `address`.
    pub fn uplink_observer(
        &self,
        address: RelativePath,
    ) -> (UplinkEventObserver, UplinkActionObserver) {
        let MetricObserver { config, metric_tx } = self;
        let profile_sender = UplinkProfileSender::new(address, metric_tx.clone());

        uplink_observer(config.sample_rate, profile_sender)
    }
}

type PulseLaneOpenResult<Agent, Context> = (
    PulseLanes,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
);

pub struct PulseLanes {
    pub uplinks: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
    pub lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
    pub node: SupplyLane<NodePulse>,
}

pub fn make_pulse_lane<Config, Agent, Context, V>(
    lane_uri: String,
) -> (
    SupplyLane<V>,
    Box<dyn LaneTasks<Agent, Context>>,
    Box<dyn LaneIo<Context>>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    V: Any + Clone + Send + Sync + Form + Debug + Unpin,
{
    let (lane, task, io) = make_supply_lane(lane_uri, true, Dropping);
    (
        lane,
        task.boxed(),
        io.expect("Lane returned private IO").boxed(),
    )
}

pub fn open_pulse_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    agent_lanes: &[&String],
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
        let (lane, task, io) =
            make_pulse_lane::<Config, Agent, Context, WarpUplinkPulse>(lane_uri.to_string());

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
            make_pulse_lane::<Config, Agent, Context, LanePulse>(lane_uri.to_string());

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
        make_pulse_lane::<Config, Agent, Context, NodePulse>("".to_string());

    tasks.push(task);
    ios.insert(LaneIdentifier::Meta(MetaNodeAddressed::NodeProfile), io);

    let pulse_lanes = PulseLanes {
        uplinks,
        lanes,
        node: node_lane,
    };

    (pulse_lanes, tasks, ios)
}
