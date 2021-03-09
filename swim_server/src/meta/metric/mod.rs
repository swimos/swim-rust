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

mod aggregator;
pub mod config;
mod lane;
mod node;
pub mod pulse;
mod uplink;

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use swim_common::warp::path::RelativePath;
use utilities::sync::trigger;

use crate::agent::lane::model::supply::SupplyLane;
use pin_utils::core_reexport::fmt::Formatter;
use std::fmt::{Debug, Display};
use utilities::uri::RelativeUri;

use crate::meta::log::{LogEntry, LogLevel, NodeLogger};
use crate::meta::metric::aggregator::{AddressedMetric, AggregatorTask, ProfileItem};
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::lane::{LanePulse, TaggedLaneProfile};
use crate::meta::metric::node::{NodeAggregatorTask, NodePulse};
use crate::meta::metric::uplink::{
    uplink_aggregator, uplink_observer, TaggedWarpUplinkProfile, UplinkProfileSender,
    WarpUplinkPulse,
};
use futures::future::try_join3;
use futures::Future;
pub use lane::WarpLaneProfile;
pub use node::NodeProfile;
use std::time::Duration;
use tokio::sync::mpsc::error::SendError as TokioSendError;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{span, Level};
use tracing_futures::Instrument;
pub use uplink::{UplinkActionObserver, UplinkEventObserver, WarpUplinkProfile};

const AGGREGATOR_TASK: &str = "Metric aggregator task";
const STOP_OK: &str = "Aggregator stopped normally";
const STOP_CLOSED: &str = "Aggregator event stream unexpectedly closed";
const LOG_ERROR_MSG: &str = "Node aggregator failed";
const LOG_TASK_FINISHED_MSG: &str = "Node aggregator task completed";

/// An observer for node, lane and uplinks which generates profiles based on the events for the
/// part. These events are aggregated and forwarded to their corresponding lanes as pulses.
pub struct NodeMetricAggregator {
    observer: MetricObserver,
}

impl Debug for NodeMetricAggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricAggregator")
            .field("observer", &self.observer)
            .finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregatorError {
    aggregator: MetricKind,
    error: AggregatorErrorKind,
}

impl From<TokioSendError<TaggedWarpUplinkProfile>> for AggregatorError {
    fn from(_: TokioSendError<TaggedWarpUplinkProfile>) -> Self {
        AggregatorError {
            aggregator: MetricKind::Uplink,
            error: AggregatorErrorKind::ForwardChannelClosed,
        }
    }
}

impl Display for AggregatorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let AggregatorError { aggregator, error } = self;
        write!(f, "{} aggregator errored with: {}", aggregator, error)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MetricKind {
    Node,
    Lane,
    Uplink,
}

impl Display for MetricKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AggregatorErrorKind {
    ForwardChannelClosed,
    AbnormalStop,
}

impl Display for AggregatorErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregatorErrorKind::ForwardChannelClosed => {
                write!(f, "Aggregator's forward channel closed")
            }
            AggregatorErrorKind::AbnormalStop => {
                write!(f, "Aggregator's input stream closed unexpectedly")
            }
        }
    }
}

impl NodeMetricAggregator {
    pub fn new(
        node_uri: RelativeUri,
        stop_rx: trigger::Receiver,
        config: MetricAggregatorConfig,
        uplink_pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
        lane_pulse_lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
        agent_pulse: SupplyLane<NodePulse>,
        log_context: NodeLogger,
    ) -> (
        NodeMetricAggregator,
        impl Future<Output = Result<(), AggregatorError>>,
    ) {
        let MetricAggregatorConfig {
            sample_rate,
            buffer_size,
            yield_after,
            backpressure_config,
        } = config;

        let (node_tx, node_rx) = mpsc::channel(buffer_size.get());
        let node_aggregator = NodeAggregatorTask::new(
            stop_rx.clone(),
            sample_rate,
            agent_pulse,
            ReceiverStream::new(node_rx),
        );

        let (lane_tx, lane_rx) = mpsc::channel(buffer_size.get());
        let lane_pulse_lanes = lane_pulse_lanes
            .into_iter()
            .map(|(k, v)| {
                let inner = ProfileItem::new(
                    TaggedLaneProfile::pack(WarpLaneProfile::default(), k.clone()),
                    v,
                );
                (k, inner)
            })
            .collect();

        let lane_aggregator = AggregatorTask::new(
            lane_pulse_lanes,
            sample_rate,
            stop_rx.clone(),
            ReceiverStream::new(lane_rx),
            node_tx,
        );

        let (uplink_task, uplink_tx) = uplink_aggregator(
            stop_rx,
            sample_rate,
            buffer_size,
            yield_after,
            backpressure_config,
            uplink_pulse_lanes,
            lane_tx,
        );

        let task_node_uri = node_uri.clone();

        let task = async move {
            let result = try_join3(
                node_aggregator.run(yield_after),
                lane_aggregator.run(yield_after),
                uplink_task,
            )
            .instrument(span!(Level::DEBUG, AGGREGATOR_TASK, ?task_node_uri))
            .await
            .map(|_| ());

            let entry = match &result {
                Ok(()) => LogEntry::make(
                    LOG_TASK_FINISHED_MSG.to_string(),
                    LogLevel::Debug,
                    task_node_uri,
                    None,
                ),
                Err(e) => {
                    let message = format!("{}: {}", LOG_ERROR_MSG, e);
                    LogEntry::make(message, LogLevel::Error, task_node_uri, None)
                }
            };

            log_context.log_entry(entry);

            result
        };

        let metrics = NodeMetricAggregator {
            observer: MetricObserver::new(config.sample_rate, node_uri, uplink_tx),
        };
        (metrics, task)
    }

    pub fn observer(&self) -> MetricObserver {
        self.observer.clone()
    }
}

#[derive(Clone, Debug)]
pub struct MetricObserver {
    sample_rate: Duration,
    node_uri: String,
    metric_tx: Sender<TaggedWarpUplinkProfile>,
}

impl MetricObserver {
    pub fn new(
        sample_rate: Duration,
        node_uri: RelativeUri,
        metric_tx: Sender<TaggedWarpUplinkProfile>,
    ) -> MetricObserver {
        MetricObserver {
            sample_rate,
            node_uri: node_uri.to_string(),
            metric_tx,
        }
    }

    /// Returns a new `UplinkObserver` for the provided `address`.
    pub fn uplink_observer(&self, lane_uri: String) -> (UplinkEventObserver, UplinkActionObserver) {
        let MetricObserver {
            sample_rate,
            node_uri,
            metric_tx,
        } = self;
        let profile_sender =
            UplinkProfileSender::new(RelativePath::new(node_uri, lane_uri), metric_tx.clone());

        uplink_observer(*sample_rate, profile_sender)
    }
}
