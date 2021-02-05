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

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use swim_common::model::Value;
use swim_common::warp::path::RelativePath;
use utilities::sync::trigger;

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::supply::{Dropping, SupplyLane};
use crate::agent::meta::metric::config::MetricCollectorConfig;
use crate::agent::meta::metric::lane::LaneProfile;
use crate::agent::meta::metric::node::NodeProfile;
use crate::agent::meta::metric::sender::TransformedSender;
use crate::agent::meta::metric::task::{CollectorStopResult, CollectorTask};
use crate::agent::meta::metric::uplink::{UplinkObserver, UplinkProfile, UplinkSurjection};
use crate::agent::meta::{IdentifiedAgentIo, MetaNodeAddressed};
use crate::agent::LaneIo;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::routing::LaneIdentifier;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::Debug;
use utilities::uri::RelativeUri;

pub mod channel;
pub mod config;
pub mod lane;
pub mod node;
pub mod sender;
pub mod task;
pub mod uplink;

#[cfg(test)]
mod tests;

#[derive(PartialEq, Debug)]
pub enum ObserverEvent {
    Node(NodeProfile),
    Lane(RelativePath, LaneProfile),
    Uplink(RelativePath, UplinkProfile),
}

pub struct MetricCollector {
    observer: MetricObserver,
    _collector_task: JoinHandle<CollectorStopResult>,
}

impl Debug for MetricCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricCollector")
            .field("observer", &self.observer)
            .finish()
    }
}

impl MetricCollector {
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        config: MetricCollectorConfig,
        lanes: HashMap<LaneIdentifier, SupplyLane<Value>>,
    ) -> MetricCollector {
        let (metric_tx, metric_rx) = mpsc::channel(config.buffer_size.get());
        let collector =
            CollectorTask::new(node_id, stop_rx, metric_rx, config.prune_frequency, lanes);
        let jh = tokio::spawn(collector.run(config.yield_after));
        let observer = MetricObserver::new(config, metric_tx);

        MetricCollector {
            observer,
            _collector_task: jh,
        }
    }

    pub fn observer(&self) -> MetricObserver {
        self.observer.clone()
    }
}

#[derive(Clone, Debug)]
pub struct MetricObserver {
    config: MetricCollectorConfig,
    metric_tx: Sender<ObserverEvent>,
}

impl MetricObserver {
    pub fn new(config: MetricCollectorConfig, metric_tx: Sender<ObserverEvent>) -> MetricObserver {
        MetricObserver { config, metric_tx }
    }

    pub fn uplink_observer(&self, address: RelativePath) -> UplinkObserver {
        let MetricObserver { config, metric_tx } = self;
        let sender = TransformedSender::new(UplinkSurjection(address), metric_tx.clone());

        UplinkObserver::new(sender, config.sample_rate)
    }
}

pub fn open_pulse_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    lanes: &[&String],
    config: &AgentExecutionConfig,
) -> (
    HashMap<LaneIdentifier, SupplyLane<Value>>,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let len = lanes.len() * 2 + 1;
    let mut tasks = Vec::with_capacity(len);
    let mut ios = HashMap::with_capacity(len);

    let mut make_lane = |map: &mut HashMap<LaneIdentifier, SupplyLane<Value>>,
                         lane_uri: &String,
                         kind: MetaNodeAddressed| {
        let (lane, task, io) = make_supply_lane(lane_uri.clone(), true, Dropping, &config);
        let identifier = LaneIdentifier::meta(kind);

        map.insert(identifier.clone(), lane);
        tasks.push(task.boxed());
        ios.insert(identifier, io.expect("Lane returned private IO").boxed());
    };

    let mut supply_lanes =
        lanes
            .into_iter()
            .fold(HashMap::with_capacity(len), |mut map, lane_uri| {
                make_lane(
                    &mut map,
                    lane_uri,
                    MetaNodeAddressed::UplinkProfile {
                        node_uri: node_uri.to_string().into(),
                        lane_uri: lane_uri.to_string().into(),
                    },
                );

                map
            });

    make_lane(
        &mut supply_lanes,
        &node_uri.to_string(),
        MetaNodeAddressed::NodeProfile {
            node_uri: node_uri.to_string().into(),
        },
    );

    (supply_lanes, tasks, ios)
}
