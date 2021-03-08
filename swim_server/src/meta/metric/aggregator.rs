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

use crate::agent::lane::model::supply::supplier::TrySupplyError;
use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::{AggregatorError, AggregatorErrorKind, MetricKind};
use crate::meta::metric::{STOP_CLOSED, STOP_OK};
use futures::FutureExt;
use futures::StreamExt;
use futures::{select, Stream};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{event, Level};
use utilities::sync::trigger;

const LANE_NOT_FOUND: &str = "Lane not found";
const REMOVING_LANE: &str = "Lane closed, removing";

pub trait AddressedMetric {
    type Metric: Send + Sync + Clone + Default;

    fn unpack(self) -> (RelativePath, Self::Metric);

    fn path(&self) -> RelativePath;

    fn pack(payload: Self::Metric, path: RelativePath) -> Self;
}

pub trait Metric<In>: Clone {
    const METRIC_KIND: MetricKind;

    type Pulse: Send + Sync + 'static;

    fn accumulate(&mut self, new: In);

    fn collect(&mut self) -> Self;

    fn as_pulse(&self) -> Self::Pulse;
}

pub struct ProfileItem<M, In>
where
    In: AddressedMetric,
    M: Metric<In::Metric>,
{
    requires_flush: bool,
    last_report: Instant,
    inner: M,
    lane: SupplyLane<M::Pulse>,
}

impl<M, In> ProfileItem<M, In>
where
    In: AddressedMetric,
    M: Metric<In::Metric>,
{
    pub fn new(profile: M, lane: SupplyLane<M::Pulse>) -> ProfileItem<M, In> {
        ProfileItem {
            requires_flush: false,
            last_report: Instant::now(),
            inner: profile,
            lane,
        }
    }

    fn flush_profile(&mut self, profile: In::Metric) -> M {
        let ProfileItem {
            inner,
            lane,
            requires_flush,
            ..
        } = self;

        *requires_flush = false;
        inner.accumulate(profile);

        let pulse = inner.as_pulse();
        let _ = lane.try_send(pulse);

        inner.collect()
    }

    fn flush_pulse(&self) {
        let ProfileItem { inner, lane, .. } = self;

        let pulse = inner.as_pulse();
        let _ = lane.try_send(pulse);
    }

    fn report(&mut self, profile: In::Metric, sample_rate: Duration) -> Result<Option<M>, ()> {
        let ProfileItem {
            last_report,
            inner,
            lane,
            requires_flush,
        } = self;

        inner.accumulate(profile);

        if last_report.elapsed() > sample_rate {
            let pulse = inner.as_pulse();

            match lane.try_send(pulse) {
                Ok(_) | Err(TrySupplyError::Capacity) => {
                    *last_report = Instant::now();

                    let ret = inner.collect();
                    *requires_flush = false;

                    Ok(Some(ret))
                }
                Err(TrySupplyError::Closed) => Err(()),
            }
        } else {
            *requires_flush = true;
            Ok(None)
        }
    }
}

pub struct AggregatorTask<In, M, S>
where
    In: AddressedMetric,
    S: Stream<Item = In> + Unpin,
    M: Metric<In::Metric>,
{
    stop_rx: trigger::Receiver,
    sample_rate: Duration,
    pulse_lanes: HashMap<RelativePath, ProfileItem<M, In>>,
    input: S,
    output: mpsc::Sender<M>,
}

impl<In, M, S> AggregatorTask<In, M, S>
where
    In: AddressedMetric,
    S: Stream<Item = In> + Unpin,
    M: Metric<In::Metric>,
{
    pub fn new(
        pulse_lanes: HashMap<RelativePath, ProfileItem<M, In>>,
        sample_rate: Duration,
        stop_rx: trigger::Receiver,
        input: S,
        output: mpsc::Sender<M>,
    ) -> AggregatorTask<In, M, S> {
        AggregatorTask {
            stop_rx,
            sample_rate,
            pulse_lanes,
            input,
            output,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        let AggregatorTask {
            stop_rx,
            sample_rate,
            mut pulse_lanes,
            input,
            output,
        } = self;

        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let error = loop {
            let event: Option<In> = select! {
                _ = fused_trigger => {
                    event!(Level::WARN, STOP_OK);

                    drain(&mut fused_metric_rx, &mut pulse_lanes, &output);

                    return Ok(());
                },
                metric = fused_metric_rx.next() => metric,
            };
            match event {
                None => {
                    event!(Level::WARN, STOP_CLOSED);
                    break AggregatorErrorKind::AbnormalStop;
                }
                Some(profile) => {
                    let (path, payload) = profile.unpack();

                    let did_error = match pulse_lanes.get_mut(&path) {
                        Some(profile) => match profile.report(payload, sample_rate) {
                            Ok(Some(pulse)) => {
                                if let Err(TrySendError::Closed(_)) = output.try_send(pulse) {
                                    break AggregatorErrorKind::ForwardChannelClosed;
                                }
                                false
                            }
                            Ok(None) => false,
                            Err(_) => true,
                        },
                        None => {
                            event!(Level::DEBUG, ?path, LANE_NOT_FOUND);
                            false
                        }
                    };

                    if did_error {
                        event!(Level::DEBUG, ?path, REMOVING_LANE);
                        let _ = pulse_lanes.remove(&path);
                    }
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        event!(Level::ERROR, %error, STOP_CLOSED);

        return Err(AggregatorError {
            aggregator: M::METRIC_KIND,
            error,
        });
    }
}

fn drain<In, M, S>(
    stream: &mut S,
    pulse_lanes: &mut HashMap<RelativePath, ProfileItem<M, In>>,
    output: &mpsc::Sender<M>,
) where
    In: AddressedMetric,
    S: Stream<Item = In> + Unpin,
    M: Metric<In::Metric>,
{
    while let Some(profile) = stream.next().now_or_never().flatten() {
        let (path, payload) = profile.unpack();

        if let Some(item) = pulse_lanes.get_mut(&path) {
            let pulse = item.flush_profile(payload);
            let _ = output.try_send(pulse);
        }
    }

    pulse_lanes
        .iter()
        .filter(|(_k, v)| v.requires_flush)
        .for_each(|(_k, item)| {
            item.flush_pulse();
        });
}
