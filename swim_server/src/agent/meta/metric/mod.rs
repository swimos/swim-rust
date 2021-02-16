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
use crate::agent::lane::model::supply::{Dropping, SupplyLane};
use crate::agent::meta::metric::config::{MetricCollectorConfig, MetricCollectorTaskConfig};
use crate::agent::meta::metric::lane::LaneProfile;
use crate::agent::meta::metric::node::NodeProfile;
use crate::agent::meta::metric::sender::TransformedSender;
use crate::agent::meta::metric::task::{CollectorStopResult, CollectorTask};
use crate::agent::meta::metric::uplink::{UplinkObserver, UplinkSurjection, WarpUplinkProfile};
use crate::agent::meta::{IdentifiedAgentIo, MetaNodeAddressed};
use crate::agent::LaneIo;
use crate::agent::LaneTasks;
use crate::agent::{make_supply_lane, AgentContext, DynamicLaneTasks, SwimAgent};
use crate::routing::LaneIdentifier;
use futures::Stream;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::Debug;
use swim_common::sink::item::{for_mpsc_sender, ItemSender};
use swim_warp::backpressure::keyed::{release_pressure, Keyed};
use swim_warp::backpressure::KeyedBackpressureConfig;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;
use utilities::uri::RelativeUri;

pub mod config;
pub mod lane;
pub mod node;
pub mod sender;
pub mod task;
pub mod uplink;

#[cfg(test)]
mod tests;

pub struct MetricCollectorError;

impl From<SendError<ObserverEvent>> for MetricCollectorError {
    fn from(_: SendError<ObserverEvent>) -> Self {
        MetricCollectorError
    }
}

#[derive(PartialEq, Debug)]
pub enum ObserverEvent {
    Node(NodeProfile),
    Lane(RelativePath, LaneProfile),
    Uplink(RelativePath, WarpUplinkProfile),
}

impl Keyed for ObserverEvent {
    type Key = String;

    fn key(&self) -> Self::Key {
        match self {
            ObserverEvent::Node(_) => {
                unimplemented!()
            }
            ObserverEvent::Lane(lane, _) => lane.to_string(),
            ObserverEvent::Uplink(lane, _) => lane.to_string(),
        }
    }
}

/// An observer for node, lane and uplinks which generates profiles based on the events for the
/// part. These events are aggregated and forwarded to their corresponding lanes as pulses.
pub struct MetricCollector {
    observer: MetricObserver,
    _collector_task: JoinHandle<CollectorStopResult>,
    _backpressure_task: JoinHandle<Result<(), MetricCollectorError>>,
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
        let (task_config, bp_config) = config.split();

        let (metric_tx, metric_rx) = mpsc::channel(task_config.buffer_size.get());
        let (messages, stream) = mpsc::channel(task_config.buffer_size.get());
        let stream = ReceiverStream::new(stream);
        let sink = for_mpsc_sender(messages).map_err_into();

        let release_task = metrics_release_backpressure(stream, sink, bp_config);
        let release_jh = tokio::spawn(release_task);

        let collector = CollectorTask::new(node_id, stop_rx, metric_rx, lanes);
        let collector_jh = tokio::spawn(collector.run(task_config.yield_after));
        let observer = MetricObserver::new(task_config, metric_tx);

        MetricCollector {
            observer,
            _collector_task: collector_jh,
            _backpressure_task: release_jh,
        }
    }

    /// Returns a handle which can be used to create uplink, lane, or node observers.
    pub fn observer(&self) -> MetricObserver {
        self.observer.clone()
    }
}

async fn metrics_release_backpressure<E, Sink>(
    messages: impl Stream<Item = ObserverEvent>,
    sink: Sink,
    config: KeyedBackpressureConfig,
) -> Result<(), E>
where
    Sink: ItemSender<ObserverEvent, E>,
{
    release_pressure(
        messages,
        sink,
        config.yield_after,
        config.bridge_buffer_size,
        config.cache_size,
        config.buffer_size,
    )
    .await
}

#[derive(Clone, Debug)]
pub struct MetricObserver {
    config: MetricCollectorTaskConfig,
    metric_tx: Sender<ObserverEvent>,
}

impl MetricObserver {
    pub fn new(
        config: MetricCollectorTaskConfig,
        metric_tx: Sender<ObserverEvent>,
    ) -> MetricObserver {
        MetricObserver { config, metric_tx }
    }

    /// Returns a new `UplinkObserver` for the provided `address`.
    pub fn uplink_observer(&self, address: RelativePath) -> UplinkObserver {
        let MetricObserver { config, metric_tx } = self;
        let sender = TransformedSender::new(UplinkSurjection(address), metric_tx.clone());

        UplinkObserver::new(sender, config.sample_rate)
    }
}

type PulseLaneOpenResult<Agent, Context> = (
    HashMap<LaneIdentifier, SupplyLane<Value>>,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
);

pub fn open_pulse_lanes<Config, Agent, Context>(
    node_uri: RelativeUri,
    lanes: &[&String],
) -> PulseLaneOpenResult<Agent, Context>
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
        let (lane, task, io) = make_supply_lane(lane_uri.clone(), true, Dropping);
        let identifier = LaneIdentifier::meta(kind);

        map.insert(identifier.clone(), lane);
        tasks.push(task.boxed());
        ios.insert(identifier, io.expect("Lane returned private IO").boxed());
    };

    let mut supply_lanes = lanes
        .iter()
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
