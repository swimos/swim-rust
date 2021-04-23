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

use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::log::{LogEntry, LogLevel, NodeLogger};
use crate::meta::metric::aggregator::{AggregatorTask, MetricState};
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::lane::{LaneMetricReporter, LanePulse};
use crate::meta::metric::node::{NodeAggregatorTask, NodePulse};
use crate::meta::metric::uplink::{
    uplink_aggregator, uplink_observer, TaggedWarpUplinkProfile, UplinkActionObserver,
    UplinkEventObserver, UplinkProfileSender, WarpUplinkPulse,
};
use futures::future::try_join3;
use futures::Future;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Add;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError as TokioSendError;
use tokio::sync::mpsc::Sender;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{span, Level};
use tracing_futures::Instrument;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

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
const LOG_ERROR_MSG: &str = "Node aggregator failed";
const LOG_TASK_FINISHED_MSG: &str = "Node aggregator task completed";

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
    pub(crate) aggregator: MetricStage,
    /// The underlying error.
    error: AggregatorErrorKind,
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

pub trait MetricReporter {
    const METRIC_STAGE: MetricStage;

    type Pulse: Send + Sync + 'static;
    type Profile: Send + Sync + 'static;
    type Input: Add<Self::Input, Output = Self::Input> + Copy + Default;

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
pub struct NodeMetricAggregator {
    /// An inner observer factory for creating uplink observer pairs.
    observer: UplinkMetricObserver,
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
    /// * `uplink_pulse_lanes` - A map keyed by lane paths and that contains supply lanes for
    /// WARP uplink pulses.
    /// * `lane_pulse_lanes` - A map keyed by lane paths and that contains supply lanes for lane
    /// pulses.
    /// * `agent_pulse` - A supply lane for producing a node's pulse.
    /// * `log_context` - Logging context for reporting errors that occur.
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
                let inner = MetricState::new(LaneMetricReporter::default(), v);
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
            .await;

            match &result {
                Ok((node, lane, uplink)) => {
                    let stages = vec![node, lane, uplink];
                    for state in stages {
                        let entry = LogEntry::make(
                            LOG_TASK_FINISHED_MSG.to_string(),
                            LogLevel::Debug,
                            task_node_uri.clone(),
                            state.to_string().to_lowercase(),
                        );
                        let _res = log_context.log_entry(entry).await;
                    }
                }
                Err(e) => {
                    let message = format!("{}: {}", LOG_ERROR_MSG, e);
                    let entry = LogEntry::make(
                        message,
                        LogLevel::Error,
                        task_node_uri,
                        e.aggregator.to_string().to_lowercase(),
                    );

                    let _res = log_context.log_entry(entry).await;
                }
            };

            result.map(|_| ())
        };

        let metrics = NodeMetricAggregator {
            observer: UplinkMetricObserver::new(config.sample_rate, node_uri, uplink_tx),
        };
        (metrics, task)
    }

    pub fn observer(&self) -> UplinkMetricObserver {
        self.observer.clone()
    }
}

/// An uplink metric observer factory for creating new ingress channels to the metric aggregator.
#[derive(Clone, Debug)]
pub struct UplinkMetricObserver {
    /// The same rate at which profiles will be reported.
    sample_rate: Duration,
    /// The URI of the metric aggregator.
    node_uri: String,
    /// A reporting channel for the accumulated profile.
    metric_tx: Sender<TaggedWarpUplinkProfile>,
}

impl UplinkMetricObserver {
    /// Creates a new uplink metric observer factory for `node_uri`.
    ///
    /// # Arguments:
    /// `sample_rate`: The rate at which to report the accumulated profile. A profile will only be
    /// reported if this period has elapsed *and* a metric is reported.
    /// `node_uri`: The node URI that this uplink is attached to.
    /// `metric_tx`: The channel to forward the profile to.
    pub fn new(
        sample_rate: Duration,
        node_uri: RelativeUri,
        metric_tx: Sender<TaggedWarpUplinkProfile>,
    ) -> UplinkMetricObserver {
        UplinkMetricObserver {
            sample_rate,
            node_uri: node_uri.to_string(),
            metric_tx,
        }
    }

    /// Returns a new event and action observer pair for the provided `lane_uri`.
    pub fn uplink_observer(&self, lane_uri: String) -> (UplinkEventObserver, UplinkActionObserver) {
        let UplinkMetricObserver {
            sample_rate,
            node_uri,
            metric_tx,
        } = self;
        let profile_sender =
            UplinkProfileSender::new(RelativePath::new(node_uri, lane_uri), metric_tx.clone());

        uplink_observer(*sample_rate, profile_sender)
    }
}
