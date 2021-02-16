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
use std::fmt::{Debug, Display, Formatter};

use futures::future::join;
use futures::{Future, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;

use swim_common::model::Value;
use swim_common::sink::item::{for_mpsc_sender, ItemSender};
use swim_common::warp::path::RelativePath;
use swim_warp::backpressure::keyed::{release_pressure, Keyed};
use swim_warp::backpressure::KeyedBackpressureConfig;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

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

pub mod config;
pub mod lane;
pub mod node;
pub mod sender;
pub mod task;
pub mod uplink;

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq)]
pub enum MetricObserverError {
    CollectorFailed,
    ChannelClosed,
}

impl Display for MetricObserverError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricObserverError::CollectorFailed => {
                write!(f, "The collector's input stream closed unexpectedly")
            }
            MetricObserverError::ChannelClosed => write!(f, "Input stream closed unexpectedly"),
        }
    }
}

impl From<SendError<ObserverEvent>> for MetricObserverError {
    fn from(_: SendError<ObserverEvent>) -> Self {
        MetricObserverError::ChannelClosed
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

/// Creates a new metric observer factory for the provided `node_id`. The factory can produce
/// observers which may be used to register events such as receiving a command, link/unlink requests
/// and other events for uplinks, nodes, and lanes. Events produced are *not* guaranteed to be
/// delivered as this will add additional overhead. Instead, observers communicate with an inner
/// collector task over an MPSC channel that has a backpressure relief system which drops the oldest
/// message first. Messages are taken from this channel and forwarded to the corresponding supply
/// lanes for the metric - these supply lanes also have a dropping strategy.
///
/// # Arguments:
/// `node_id`: the node that this observer is operating on.
/// `stop_rx`: a stop trigger which will cause this observer to stop.
/// `config`: a configuration for the collector task and backpressure relief system.
/// `lanes`: a map containing the meta node addressed supply lanes for the observers.
pub fn make_metric_observer(
    node_id: String,
    stop_rx: trigger::Receiver,
    config: MetricCollectorConfig,
    lanes: HashMap<LaneIdentifier, SupplyLane<Value>>,
) -> (
    MetricObserverFactory,
    impl Future<Output = Result<(), MetricObserverError>>,
) {
    let (task_config, bp_config) = config.split();
    let (metric_tx, metric_rx) = mpsc::channel(task_config.buffer_size.get());
    let observer = MetricObserverFactory::new(task_config, metric_tx);

    let task = async move {
        let metric_stream = ReceiverStream::new(metric_rx);
        let (sink, bp_stream) = mpsc::channel(task_config.buffer_size.get());
        let sink = for_mpsc_sender(sink).map_err_into::<MetricObserverError>();

        let release_task = metrics_release_backpressure(
            metric_stream.take_until(stop_rx.clone()),
            sink,
            bp_config,
        );

        let collector = CollectorTask::new(node_id, stop_rx, bp_stream, lanes);
        let (release_result, collector_result) =
            join(release_task, collector.run(task_config.yield_after)).await;

        match (release_result, collector_result) {
            (Err(e), _) => Err(e),
            (_, CollectorStopResult::Abnormal) => Err(MetricObserverError::CollectorFailed),
            _ => Ok(()),
        }
    };

    (observer, task)
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
pub struct MetricObserverFactory {
    config: MetricCollectorTaskConfig,
    metric_tx: Sender<ObserverEvent>,
}

impl MetricObserverFactory {
    fn new(
        config: MetricCollectorTaskConfig,
        metric_tx: Sender<ObserverEvent>,
    ) -> MetricObserverFactory {
        MetricObserverFactory { config, metric_tx }
    }

    /// Returns a new `UplinkObserver` for the provided `address`.
    pub fn uplink_observer(&self, address: RelativePath) -> UplinkObserver {
        let MetricObserverFactory { config, metric_tx } = self;
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
