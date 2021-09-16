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

use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::lane::WarpLaneProfile;
use crate::meta::metric::uplink::WarpUplinkPulse;
use crate::meta::metric::{AggregatorError, AggregatorErrorKind, MetricReporter, MetricStage};
use crate::meta::metric::{STOP_CLOSED, STOP_OK};
use futures::FutureExt;
use futures::StreamExt;
use futures::{select, Stream};
use std::num::NonZeroUsize;
use std::ops::Add;
use std::time::{Duration, Instant};
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use utilities::sync::trigger;

#[derive(Default)]
pub struct ModeMetricReporter {
    event_count: u64,
    command_count: u64,
    close_count: u32,
    open_count: u32,
}

impl MetricReporter for ModeMetricReporter {
    const METRIC_STAGE: MetricStage = MetricStage::Node;

    type Pulse = NodePulse;
    // stub until node profile aggregation is implemented in the host table
    type Profile = ();
    type Input = WarpLaneProfile;

    fn report(&mut self, part: Self::Input) -> (Self::Pulse, Self::Profile) {
        let ModeMetricReporter {
            event_count,
            command_count,
            close_count,
            open_count,
        } = self;

        let WarpLaneProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_command_delta,
            uplink_command_rate,
            uplink_open_delta,
            uplink_close_delta,
        } = part;

        *open_count = open_count.saturating_add(uplink_open_delta);
        *close_count = close_count.saturating_add(uplink_close_delta);
        *event_count = event_count.saturating_add(uplink_event_delta as u64);
        *command_count = command_count.saturating_add(uplink_command_delta as u64);
        let link_count = open_count.saturating_sub(*close_count);

        let pulse = NodePulse {
            uplinks: WarpUplinkPulse {
                link_count,
                event_rate: uplink_event_rate,
                event_count: *event_count,
                command_rate: uplink_command_rate,
                command_count: *command_count,
            },
        };

        (pulse, ())
    }
}

/// A node pulse detailing accumulated metrics.
#[derive(Default, Form, Copy, Clone, PartialEq, Debug)]
pub struct NodePulse {
    /// Accumulated WARP uplink pulse.
    pub uplinks: WarpUplinkPulse,
}

impl Add<NodePulse> for NodePulse {
    type Output = NodePulse;

    fn add(self, rhs: NodePulse) -> Self::Output {
        NodePulse {
            uplinks: self.uplinks.add(rhs.uplinks),
        }
    }
}

/// An aggregator for node metrics.
pub struct NodeAggregatorTask {
    /// A stop signal for consuming and producing metrics.
    stop_rx: trigger::Receiver,
    /// The rate at which profiles and pulses will be reported.
    sample_rate: Duration,
    /// The time at which the task last reported a profile or pulse.
    last_report: Instant,
    /// A supply lane for sending pulses to.
    lane: SupplyLane<NodePulse>,
    /// The input channel for lane profiles.
    input: ReceiverStream<(RelativePath, WarpLaneProfile)>,
    /// The accumulated lane profile.
    profile: WarpLaneProfile,
    /// Metric reporter
    reporter: ModeMetricReporter,
}

impl NodeAggregatorTask {
    pub fn new(
        stop_rx: trigger::Receiver,
        sample_rate: Duration,
        lane: SupplyLane<NodePulse>,
        input: ReceiverStream<(RelativePath, WarpLaneProfile)>,
    ) -> Self {
        NodeAggregatorTask {
            stop_rx,
            sample_rate,
            last_report: Instant::now(),
            lane,
            input,
            profile: WarpLaneProfile::default(),
            reporter: ModeMetricReporter::default(),
        }
    }

    /// Runs the aggregator and yields executing back to the runtime every `yield_after`.
    pub async fn run(self, yield_after: NonZeroUsize) -> Result<MetricStage, AggregatorError> {
        let NodeAggregatorTask {
            stop_rx,
            sample_rate,
            mut last_report,
            lane,
            input,
            mut profile,
            mut reporter,
        } = self;

        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let error = loop {
            let event: Option<(RelativePath, WarpLaneProfile)> = select! {
                _ = fused_trigger => {
                    event!(Level::WARN, STOP_OK);
                    drain(&mut fused_metric_rx, lane, profile, reporter)?;

                    return Ok(MetricStage::Node);
                },
                metric = fused_metric_rx.next() => metric,
            };
            match event {
                None => {
                    event!(Level::WARN, STOP_CLOSED);
                    drain(&mut fused_metric_rx, lane, profile, reporter)?;

                    break AggregatorErrorKind::AbnormalStop;
                }
                Some((_path, new_profile)) => {
                    profile = profile.add(new_profile);

                    if last_report.elapsed() > sample_rate {
                        let (pulse, _) = reporter.report(profile);

                        if let Err(TrySendError::Closed(_)) = lane.try_send(pulse) {
                            break AggregatorErrorKind::ForwardChannelClosed;
                        } else {
                            profile = Default::default();
                            last_report = Instant::now();
                        }
                    }
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        event!(Level::ERROR, %error, STOP_CLOSED);

        Err(AggregatorError {
            aggregator: MetricStage::Node,
            error,
        })
    }
}

/// Drains all the pending messages from `stream` and flushes any pending pulses and profiles.
fn drain<S>(
    stream: &mut S,
    lane: SupplyLane<NodePulse>,
    mut profile: WarpLaneProfile,
    mut reporter: ModeMetricReporter,
) -> Result<(), AggregatorError>
where
    S: Stream<Item = (RelativePath, WarpLaneProfile)> + Unpin,
{
    while let Some((_, part)) = stream.next().now_or_never().flatten() {
        profile = profile.add(part);
    }

    let (pulse, _) = reporter.report(profile);
    match lane.try_send(pulse) {
        Err(TrySendError::Closed(_)) => Err(AggregatorError {
            aggregator: MetricStage::Node,
            error: AggregatorErrorKind::ForwardChannelClosed,
        }),
        _ => Ok(()),
    }
}
