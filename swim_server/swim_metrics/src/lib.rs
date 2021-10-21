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

use crate::aggregator::{AggregatorTask, MetricState};
use crate::config::MetricAggregatorConfig;
use crate::lane::{LaneMetricReporter, LanePulse};
use crate::node::{NodeAggregatorTask, NodePulse};
use crate::uplink::{
    uplink_aggregator, uplink_observer, AggregatorConfig, TaggedWarpUplinkProfile, UplinkObserver,
    UplinkProfileSender, WarpUplinkPulse,
};
use futures::future::try_join3;
use futures::Future;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Add;
use swim_common::sink::item::try_send::TrySend;
use swim_common::warp::path::RelativePath;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError as TokioSendError, TrySendError};
use tokio::sync::mpsc::Sender;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{span, Level};
use tracing_futures::Instrument;

pub mod aggregator;
pub mod config;
pub mod lane;
pub mod node;
pub mod uplink;

#[cfg(test)]
mod tests;

const AGGREGATOR_TASK: &str = "Metric aggregator task";
pub(crate) const STOP_OK: &str = "Aggregator stopped normally";
pub(crate) const STOP_CLOSED: &str = "Aggregator event stream unexpectedly closed";

type SupplyLane<I> = Box<dyn TrySend<I, Error = TrySendError<I>> + Send>;

pub struct MetaPulseLanes {
    pub uplinks: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
    pub lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
    pub node: SupplyLane<NodePulse>,
}

/// A metric aggregator kind or stage in the pipeline.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MetricStage {
    /// A node aggregator which accepts lane profiles and produces node pulses/profiles.
    Node,
    /// A lane aggregator which accepts uplink profiles and produces lane pulses/profiles.
    Lane,
    /// An uplink aggregator which aggregates events and actions which occur in the uplink and then
    /// produces uplink profiles and a pulse.
    Uplink,
}

impl Display for MetricStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AggregatorError {
    /// The type of aggregator that errored.
    pub aggregator: MetricStage,
    /// The underlying error.
    pub error: AggregatorErrorKind,
}

impl Display for AggregatorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let AggregatorError { aggregator, error } = self;
        write!(f, "{} aggregator errored with: {}", aggregator, error)
    }
}

impl From<TokioSendError<TaggedWarpUplinkProfile>> for AggregatorError {
    fn from(_: TokioSendError<TaggedWarpUplinkProfile>) -> Self {
        AggregatorError {
            aggregator: MetricStage::Uplink,
            error: AggregatorErrorKind::ForwardChannelClosed,
        }
    }
}

/// An error produced by a metric aggregator.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AggregatorErrorKind {
    /// The aggregator's forward/output channel closed.
    ForwardChannelClosed,
    /// The input stream to the aggregator closed unexpectedly.
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

/// A metric reporter which will take its input and produce a node and profile for the metric.
pub trait MetricReporter {
    /// The stage in the pipeline that this metric is.
    const METRIC_STAGE: MetricStage;

    /// The type of pulse that is generated by this metric.
    type Pulse: Send + Sync + 'static;
    /// The type of pulse that is generated by this metric.
    type Profile: Send + Sync + 'static;
    /// An aggregated metric that this reporter will use to produce a pulse and profile from.
    type Input: Add<Self::Input, Output = Self::Input> + Copy + Default;

    /// Produce a pulse and profile from an accumulated metric.
    fn report(&mut self, part: Self::Input) -> (Self::Pulse, Self::Profile);
}

/// A node metric aggregator.
///
/// The aggregator has an input channel which is fed WARP uplink profiles which are produced by
/// uplink observers. The reporting interval of these profiles is configurable and backpressure
/// relief is also applied. Profile reporting is event driven and a profile will only be reported
/// if a metric is reported *and* the sample period has elapsed; no periodic flushing of stale
/// profiles is applied. These profiles are then aggregated by an uplink aggregator and a WARP
/// uplink pulse is produced at the corresponding supply lane as well as a WARP uplink profile being
/// forwarded the the next stage in the pipeline. The same process is repeated for lanes and finally
/// a node pulse and profile is produced.
///
/// This aggregator is structured in a fan-in fashion and profiles and pulses are debounced by the
/// sample rate which is provided at creation. In addition to this, no guarantees are made as to
/// whether the the pulse or profiles will be delivered; if the channel is full, then the message is
/// dropped.
#[derive(Debug, Clone)]
pub struct NodeMetricAggregator {
    /// The sample rate at which profiles will be reported.
    sample_rate: Duration,
    /// The URI of the metric aggregator.
    node_uri: String,
    /// A reporting channel for the accumulated profile.
    metric_tx: Sender<TaggedWarpUplinkProfile>,
}

impl NodeMetricAggregator {
    /// Creates a new node metric aggregator for the `node_uri`.
    ///
    /// # Arguments:
    ///
    /// * `node_uri` - The URI that this aggregator corresponds to.
    /// * `stop_rx` - A stop signal for shutting down the aggregator. When this is triggered, it
    /// will cause all of the pending profiles and pulses to be flushed. Regardless of the last
    /// flush time.
    /// * `config` - A configuration for the aggregator and backpressure.
    /// * `lanes`- A collection of lanes that the metrics will be sent to for: uplink, lane and node
    /// pulses.
    pub fn new(
        node_uri: RelativeUri,
        stop_rx: trigger::Receiver,
        config: MetricAggregatorConfig,
        lanes: MetaPulseLanes,
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

        let MetaPulseLanes {
            uplinks,
            lanes,
            node,
        } = lanes;

        let (uplink_to_lane_tx, uplink_to_lane_rx) = trigger::trigger();
        let (lane_to_node_tx, lane_to_node_rx) = trigger::trigger();

        let (node_tx, node_rx) = mpsc::channel(buffer_size.get());
        let node_aggregator = NodeAggregatorTask::new(
            lane_to_node_rx,
            sample_rate,
            node,
            ReceiverStream::new(node_rx),
        );

        let (lane_tx, lane_rx) = mpsc::channel(buffer_size.get());
        let lane_pulse_lanes = lanes
            .into_iter()
            .map(|(k, v)| {
                let inner = MetricState::new(LaneMetricReporter::default(), v);
                (k, inner)
            })
            .collect();

        let lane_aggregator = AggregatorTask::new(
            lane_pulse_lanes,
            sample_rate,
            uplink_to_lane_rx,
            ReceiverStream::new(lane_rx),
            node_tx,
        );

        let uplink_config = AggregatorConfig {
            backpressure_config,
            sample_rate,
            buffer_size,
            yield_after,
        };

        let (uplink_task, uplink_tx) =
            uplink_aggregator(uplink_config, stop_rx, uplinks, lane_tx, uplink_to_lane_tx);

        let task_node_uri = node_uri.clone();

        let task = async move {
            let task = try_join3(
                node_aggregator.run(yield_after),
                lane_aggregator.run(yield_after, lane_to_node_tx),
                uplink_task,
            )
            .instrument(span!(Level::DEBUG, AGGREGATOR_TASK, ?task_node_uri));

            task.await.map(|_| ())
        };

        let metrics = NodeMetricAggregator {
            sample_rate: config.sample_rate,
            node_uri: node_uri.to_string(),
            metric_tx: uplink_tx,
        };
        (metrics, task)
    }

    /// Returns a new event and action observer pair for the provided `lane_uri`.
    pub fn uplink_observer(&self, lane_uri: String) -> UplinkObserver {
        let NodeMetricAggregator {
            sample_rate,
            node_uri,
            metric_tx,
        } = self;
        let profile_sender =
            UplinkProfileSender::new(RelativePath::new(node_uri, lane_uri), metric_tx.clone());

        uplink_observer(*sample_rate, profile_sender)
    }

    pub fn uplink_observer_for_path(&self, uri: RelativePath) -> UplinkObserver {
        let NodeMetricAggregator {
            sample_rate,
            metric_tx,
            ..
        } = self;
        let profile_sender = UplinkProfileSender::new(uri, metric_tx.clone());

        uplink_observer(*sample_rate, profile_sender)
    }
}
