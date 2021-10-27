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

use crate::{AggregatorError, AggregatorErrorKind, MetricReporter, SupplyLane};
use crate::{STOP_CLOSED, STOP_OK};
use futures::FutureExt;
use futures::StreamExt;
use futures::{select_biased, Stream};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Add;
use std::time::{Duration, Instant};
use swim_common::warp::path::RelativePath;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{event, Level};

const LANE_NOT_FOUND: &str = "Lane not found";
const REMOVING_LANE: &str = "Lane closed, removing";

/// Represents the state of a metric and its pulse lane.
pub struct MetricState<M>
where
    M: MetricReporter,
{
    /// Whether the metric has had data reported since its last report time.
    requires_flush: bool,
    /// The time that this metric was last reported.
    last_report: Instant,
    /// Accumulated metrics.
    accumulated: M::Input,
    /// Metric reporter.
    reporter: M,
    /// The metric's pulse lane.
    lane: SupplyLane<M::Pulse>,
}

impl<M: MetricReporter> MetricState<M> {
    pub fn new(reporter: M, lane: SupplyLane<M::Pulse>) -> MetricState<M> {
        MetricState {
            requires_flush: true,
            last_report: Instant::now(),
            accumulated: Default::default(),
            reporter,
            lane,
        }
    }

    /// Accumulate `profile` and then flush the profile and its pulse
    fn flush(&mut self, new: M::Input) -> M::Profile {
        let MetricState {
            accumulated,
            reporter,
            lane,
            requires_flush,
            ..
        } = self;

        *requires_flush = false;
        let input = accumulated.add(new);
        let (pulse, profile) = reporter.report(input);

        *accumulated = Default::default();

        let _ = lane.try_send_item(pulse);

        profile
    }

    /// Accumulate `profile`, flush the pulse if the last report time is greater than `sample_rate`.
    /// Returns a result with an optional profile if the report time is greater than `sample_rate`,
    /// or an error if the pulse lane is closed.
    fn report(
        &mut self,
        profile: M::Input,
        sample_rate: Duration,
    ) -> Result<Option<M::Profile>, ()> {
        let MetricState {
            last_report,
            accumulated,
            lane,
            reporter,
            requires_flush,
        } = self;

        *accumulated = accumulated.add(profile);

        if last_report.elapsed() > sample_rate {
            let (pulse, profile) = reporter.report(*accumulated);

            match lane.try_send_item(pulse) {
                Ok(_) | Err(TrySendError::Full(_)) => {
                    *last_report = Instant::now();
                    *requires_flush = false;
                    *accumulated = Default::default();

                    Ok(Some(profile))
                }
                Err(TrySendError::Closed(_)) => Err(()),
            }
        } else {
            *requires_flush = true;
            Ok(None)
        }
    }
}

/// An aggregator for an addressed metric.
pub struct AggregatorTask<M, S>
where
    S: Stream<Item = (RelativePath, M::Input)> + Unpin,
    M: MetricReporter,
{
    /// A stop signal for consuming and producing metrics. When this is triggered, all pending
    /// messages are flushed regardless of their last report time.
    stop_rx: trigger::Receiver,
    /// The rate at which profiles and pulses will be reported.
    sample_rate: Duration,
    /// A map keyed by lane URIs and has a corresponding state for its metrics.
    metrics: HashMap<RelativePath, MetricState<M>>,
    /// A stream of profiles which this aggregator will accumulate and generate pulses and profiles
    /// from.
    input: S,
    /// A channel for sending accumulated profiles to.
    output: mpsc::Sender<(RelativePath, M::Profile)>,
}

impl<M, S> AggregatorTask<M, S>
where
    S: Stream<Item = (RelativePath, M::Input)> + Unpin,
    M: MetricReporter,
{
    pub fn new(
        metrics: HashMap<RelativePath, MetricState<M>>,
        sample_rate: Duration,
        stop_rx: trigger::Receiver,
        input: S,
        output: mpsc::Sender<(RelativePath, M::Profile)>,
    ) -> AggregatorTask<M, S> {
        AggregatorTask {
            stop_rx,
            sample_rate,
            metrics,
            input,
            output,
        }
    }

    /// Runs the aggregator and yields executing back to the runtime every `yield_after`.
    pub async fn run(
        self,
        yield_after: NonZeroUsize,
        stop_notify: trigger::Sender,
    ) -> Result<(), AggregatorError> {
        let AggregatorTask {
            stop_rx,
            sample_rate,
            mut metrics,
            input,
            output,
            ..
        } = self;

        let mut removed_lanes = HashSet::new();
        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;
        let yield_mod = yield_after.get();
        let stage = M::METRIC_STAGE;

        let stop_result = loop {
            let event: Option<(RelativePath, M::Input)> = select_biased! {
                _ = fused_trigger => {
                    drain(&mut fused_metric_rx, &mut metrics, &output);
                    event!(Level::DEBUG, %stage, STOP_OK);

                    break Ok(());
                },
                metric = fused_metric_rx.next() => metric,
            };
            match event {
                None => {
                    drain(&mut fused_metric_rx, &mut metrics, &output);
                    event!(Level::WARN, %stage, STOP_CLOSED);

                    break Err(AggregatorError {
                        aggregator: M::METRIC_STAGE,
                        error: AggregatorErrorKind::AbnormalStop,
                    });
                }
                Some((path, payload)) => {
                    let did_error = match metrics.get_mut(&path) {
                        Some(state) => match state.report(payload, sample_rate) {
                            Ok(Some(profile)) => {
                                if let Err(TrySendError::Closed(_)) =
                                    output.try_send((path.clone(), profile))
                                {
                                    let error = AggregatorError {
                                        aggregator: M::METRIC_STAGE,
                                        error: AggregatorErrorKind::ForwardChannelClosed,
                                    };
                                    event!(Level::ERROR, %stage, %error, STOP_CLOSED);
                                    break Err(error);
                                }
                                false
                            }
                            Ok(None) => false,
                            Err(_) => true,
                        },
                        None => {
                            if removed_lanes.get(&path).is_none() {
                                event!(Level::ERROR, ?path, LANE_NOT_FOUND);
                            }

                            false
                        }
                    };

                    if did_error {
                        event!(Level::DEBUG, ?path, REMOVING_LANE);
                        let _ = metrics.remove(&path);
                        removed_lanes.insert(path);
                    }
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        stop_notify.trigger();
        stop_result
    }
}

/// Drains all the pending messages from `stream` and flushes any pending pulses and profiles.
fn drain<M, S>(
    stream: &mut S,
    metrics: &mut HashMap<RelativePath, MetricState<M>>,
    output: &mpsc::Sender<(RelativePath, M::Profile)>,
) where
    S: Stream<Item = (RelativePath, M::Input)> + Unpin,
    M: MetricReporter,
{
    while let Some((path, part)) = stream.next().now_or_never().flatten() {
        if let Some(item) = metrics.get_mut(&path) {
            let profile = item.flush(part);
            let _ = output.try_send((path, profile));
        }
    }

    metrics
        .iter_mut()
        .filter(|(_k, v)| v.requires_flush)
        .for_each(|(k, item)| {
            let MetricState {
                accumulated,
                reporter,
                lane,
                ..
            } = item;
            let (pulse, profile) = reporter.report(*accumulated);
            let _ = output.try_send((k.clone(), profile));
            let _ = lane.try_send_item(pulse);
        });
}
