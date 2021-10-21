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

#[cfg(test)]
mod tests;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tracing::{event, Level};

use crate::aggregator::{AggregatorTask, MetricState};
use crate::{AggregatorError, MetricReporter, MetricStage, SupplyLane};
use futures::{Future, Stream, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Add;
use swim_common::sink::item::{for_mpsc_sender, ItemSender};
use swim_utilities::trigger;
use swim_warp::backpressure::keyed::{release_pressure, Keyed};
use tokio_stream::wrappers::ReceiverStream;

impl Add<WarpUplinkProfile> for WarpUplinkProfile {
    type Output = WarpUplinkProfile;

    fn add(self, rhs: WarpUplinkProfile) -> Self::Output {
        WarpUplinkProfile {
            event_delta: self.event_delta.saturating_add(rhs.event_delta),
            event_rate: self.event_rate.saturating_add(rhs.event_rate),
            command_delta: self.command_delta.saturating_add(rhs.command_delta),
            command_rate: self.command_rate.saturating_add(rhs.command_rate),
            open_delta: self.open_delta.saturating_add(rhs.open_delta),
            close_delta: self.close_delta.saturating_add(rhs.close_delta),
        }
    }
}

#[derive(Default)]
pub struct UplinkMetricReporter {
    event_count: u64,
    command_count: u64,
    close_count: u32,
    open_count: u32,
}

impl MetricReporter for UplinkMetricReporter {
    const METRIC_STAGE: MetricStage = MetricStage::Uplink;

    type Pulse = WarpUplinkPulse;
    type Profile = WarpUplinkProfile;
    type Input = WarpUplinkProfile;

    fn report(&mut self, accumulated: Self::Input) -> (Self::Pulse, Self::Profile) {
        let UplinkMetricReporter {
            event_count,
            command_count,
            close_count,
            open_count,
        } = self;

        let WarpUplinkProfile {
            event_delta,
            event_rate,
            command_delta,
            command_rate,
            open_delta,
            close_delta,
        } = accumulated;

        *open_count = open_count.saturating_add(open_delta);
        *close_count = close_count.saturating_add(close_delta);
        *event_count = event_count.saturating_add(event_delta as u64);
        *command_count = command_count.saturating_add(command_delta as u64);

        let link_count = open_count.saturating_sub(*close_count);

        let pulse = WarpUplinkPulse {
            link_count,
            event_rate,
            event_count: *event_count,
            command_rate,
            command_count: *command_count,
        };

        let profile = WarpUplinkProfile {
            event_delta,
            event_rate,
            command_delta,
            command_rate,
            open_delta,
            close_delta,
        };

        (pulse, profile)
    }
}

const SEND_PROFILE_FAIL: &str = "Failed to send uplink profile";

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct TrySendError;

/// A sender that tags and maps profiles to `TaggedWarpUplinkProfile`
#[derive(Clone, Debug)]
pub struct UplinkProfileSender {
    lane_id: RelativePath,
    sender: mpsc::Sender<TaggedWarpUplinkProfile>,
}

impl UplinkProfileSender {
    pub fn new(
        lane_id: RelativePath,
        sender: mpsc::Sender<TaggedWarpUplinkProfile>,
    ) -> UplinkProfileSender {
        UplinkProfileSender { lane_id, sender }
    }

    pub fn try_send(&self, profile: WarpUplinkProfile) -> Result<(), TrySendError> {
        let UplinkProfileSender { lane_id, sender } = self;
        let tagged = TaggedWarpUplinkProfile {
            path: lane_id.clone(),
            profile,
        };

        sender.try_send(tagged).map_err(|_| TrySendError)
    }
}

/// A lane pulse detailing accumulated metrics for all uplinks on a lane.
#[derive(Default, Form, Clone, Copy, PartialEq, Debug)]
pub struct WarpUplinkPulse {
    /// Uplink open count - close count.
    pub link_count: u32,
    /// The rate at which events are being produced.
    pub event_rate: u64,
    /// The total number of events that have occurred.
    pub event_count: u64,
    /// The rate at which command messages are being produced.
    pub command_rate: u64,
    /// The total number of command messages that have occurred.
    pub command_count: u64,
}

impl Add<WarpUplinkPulse> for WarpUplinkPulse {
    type Output = WarpUplinkPulse;

    fn add(self, rhs: WarpUplinkPulse) -> Self::Output {
        WarpUplinkPulse {
            link_count: self.link_count.wrapping_add(rhs.link_count),
            event_rate: self.event_rate.wrapping_add(rhs.event_rate),
            event_count: self.event_count.wrapping_add(rhs.event_count),
            command_rate: self.command_rate.wrapping_add(rhs.command_rate),
            command_count: self.command_count.wrapping_add(rhs.command_count),
        }
    }
}

/// An accumulated WARP uplink profile generated by the accumulator task.
#[derive(Default, Form, Copy, Clone, PartialEq, Debug)]
pub struct WarpUplinkProfile {
    /// The number of events that have been produced since the last report time.
    pub event_delta: u32,
    /// The rate at which events were produced since the last report time.
    pub event_rate: u64,
    /// The number of command messages that have been produced since the last report time.
    pub command_delta: u32,
    /// The rate at which command messages were produced since the last report time.
    pub command_rate: u64,
    /// The number of uplinks that were opened since the last report time.
    pub open_delta: u32,
    /// The number of uplinks that were closed since the last report time.
    pub close_delta: u32,
}

/// An inner observer for event and action observers to allow for interior mutability and for
/// channels observers to be cloned.
pub struct InnerObserver {
    sending: AtomicBool,
    event_delta: AtomicU32,
    command_delta: AtomicU32,
    open_delta: AtomicU32,
    close_delta: AtomicU32,
    last_report: UnsafeCell<Instant>,
    report_interval: Duration,
    sender: UplinkProfileSender,
    count: AtomicUsize,
}

impl InnerObserver {
    /// Flush the latest profile iff the report interval has elapsed.
    fn flush(&self) {
        let InnerObserver {
            sending,
            last_report,
            report_interval,
            ..
        } = self;

        if let Ok(false) =
            sending.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        {
            let last_reported = unsafe { *last_report.get() };
            if last_reported.elapsed() < *report_interval {
                sending.store(false, Ordering::Release);
                return;
            }

            self.accumulate_and_send();

            unsafe {
                *last_report.get() = Instant::now();
            }

            sending.store(false, Ordering::Release);
        }
    }

    /// Accumulate and send an updated profile regardless of the last send time.
    fn accumulate_and_send(&self) {
        let InnerObserver {
            event_delta,
            command_delta,
            open_delta,
            close_delta,
            last_report,
            sender,
            ..
        } = self;

        let last_reported = unsafe { *last_report.get() };

        let calculate_rate = |delta: u32, dt: f64| {
            if delta == 0 || dt == 0.0 {
                0
            } else {
                ((delta as f64 * 1000.0) / dt).ceil() as u64
            }
        };

        let now = Instant::now();
        let dt = now.duration_since(last_reported).as_secs_f64();

        let event_delta = event_delta.swap(0, Ordering::Relaxed);
        let event_rate = calculate_rate(event_delta, dt);

        let command_delta = command_delta.swap(0, Ordering::Acquire);
        let command_rate = calculate_rate(command_delta, dt);

        let profile_open_delta = open_delta.swap(0, Ordering::Relaxed);
        let profile_close_delta = close_delta.swap(0, Ordering::Relaxed);

        let profile = WarpUplinkProfile {
            event_delta,
            event_rate,
            command_delta,
            command_rate,
            open_delta: profile_open_delta,
            close_delta: profile_close_delta,
        };

        if sender.try_send(profile).is_err() {
            let lane = &sender.lane_id;
            event!(Level::DEBUG, ?lane, SEND_PROFILE_FAIL);
        }
    }
}

unsafe impl Send for InnerObserver {}
unsafe impl Sync for InnerObserver {}

/// Creates a new uplink observer pair that will send profiles when new events occur at
/// `report_interval` to `sender`.
pub fn uplink_observer(report_interval: Duration, sender: UplinkProfileSender) -> UplinkObserver {
    let inner = InnerObserver {
        sending: AtomicBool::new(false),
        event_delta: AtomicU32::new(0),
        command_delta: AtomicU32::new(0),
        open_delta: AtomicU32::new(0),
        close_delta: AtomicU32::new(0),
        last_report: UnsafeCell::new(Instant::now()),
        report_interval,
        sender,
        count: AtomicUsize::new(1),
    };

    UplinkObserver::from_inner(Arc::new(inner))
}

pub struct UplinkObserver {
    inner: Arc<InnerObserver>,
}

impl UplinkObserver {
    fn from_inner(inner: Arc<InnerObserver>) -> UplinkObserver {
        UplinkObserver { inner }
    }
}

impl UplinkObserver {
    /// Flush any pending metrics if the report interval has elapsed.
    pub fn flush(&self) {
        self.inner.flush();
    }

    /// Report that a new event has been dispatched and send a new profile if the report interval
    /// has elapsed.
    pub fn on_event(&self, notify: bool) {
        let _old = self.inner.event_delta.fetch_add(1, Ordering::Acquire);

        if notify {
            self.inner.flush();
        }
    }

    /// Report that a new command message has been dispatched and send a new profile if the
    /// report interval has elapsed.
    pub fn on_command(&self, notify: bool) {
        let _old = self.inner.command_delta.fetch_add(1, Ordering::Acquire);
        if notify {
            self.inner.flush();
        }
    }

    /// Report that a new uplink opened and send a new profile if the report interval has elapsed.
    pub fn did_open(&self, notify: bool) {
        let _old = self.inner.open_delta.fetch_add(1, Ordering::Acquire);
        if notify {
            self.inner.flush();
        }
    }

    /// Report that an uplink closed and send a new profile if the report interval has elapsed.
    pub fn did_close(&self, notify: bool) {
        let _old = self.inner.close_delta.fetch_add(1, Ordering::Acquire);
        if notify {
            self.inner.flush();
        }
    }

    /// Test utility that will set the deltas and counts from `to` to the atomics in the inner
    /// observer.
    #[cfg(test)]
    pub(crate) fn set_inner_values(&self, to: WarpUplinkProfile) {
        let InnerObserver {
            event_delta,
            command_delta,
            open_delta,
            close_delta,
            ..
        } = &*self.inner;

        let WarpUplinkProfile {
            event_delta: profile_event_delta,
            command_delta: profile_command_delta,
            open_delta: profile_open_delta,
            close_delta: profile_close_delta,
            ..
        } = to;

        event_delta.store(profile_event_delta, Ordering::Relaxed);
        command_delta.store(profile_command_delta, Ordering::Relaxed);

        open_delta.store(profile_open_delta, Ordering::Relaxed);
        close_delta.store(profile_close_delta, Ordering::Relaxed);
    }

    pub fn force_flush(&self) {
        self.inner.accumulate_and_send();
    }
}

impl Clone for UplinkObserver {
    fn clone(&self) -> Self {
        self.inner.count.fetch_add(1, Ordering::Relaxed);
        UplinkObserver::from_inner(self.inner.clone())
    }
}

impl Drop for UplinkObserver {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }

        // Force flush any remaining metrics
        self.inner.accumulate_and_send();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricBackpressureConfig {
    /// Buffer size for the channels connecting the input and output tasks.
    pub buffer_size: NonZeroUsize,
    /// Number of loop iterations after which the input and output tasks will yield.
    pub yield_after: NonZeroUsize,
    /// Buffer size for the communication side channel between the input and output tasks.
    pub bridge_buffer_size: NonZeroUsize,
    /// Number of keys for maintain a channel for at any one time.
    pub cache_size: NonZeroUsize,
}

pub struct AggregatorConfig {
    pub sample_rate: Duration,
    pub buffer_size: NonZeroUsize,
    pub yield_after: NonZeroUsize,
    pub backpressure_config: MetricBackpressureConfig,
}

pub fn uplink_aggregator(
    config: AggregatorConfig,
    stop_rx: trigger::Receiver,
    uplink_pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
    lane_tx: mpsc::Sender<(RelativePath, WarpUplinkProfile)>,
    uplink_to_lane_tx: trigger::Sender,
) -> (
    impl Future<Output = Result<(), AggregatorError>>,
    mpsc::Sender<TaggedWarpUplinkProfile>,
) {
    let AggregatorConfig {
        sample_rate,
        buffer_size,
        yield_after,
        backpressure_config,
    } = config;

    let (uplink_tx, uplink_rx) = mpsc::channel(buffer_size.get());
    let metric_stream = ReceiverStream::new(uplink_rx);
    let (sink, bp_stream) = mpsc::channel(buffer_size.get());
    let sink = for_mpsc_sender(sink).map_err_into::<AggregatorError>();
    let (flush_tx, flush_rx) = trigger::trigger();
    let release_task = metrics_release_backpressure(
        metric_stream.take_until(flush_rx),
        sink,
        backpressure_config,
    );

    let uplink_pulse_lanes = uplink_pulse_lanes
        .into_iter()
        .map(|(k, v)| {
            let inner = MetricState::new(UplinkMetricReporter::default(), v);
            (k, inner)
        })
        .collect();

    let bp_stream = ReceiverStream::new(bp_stream).map(|e| {
        let TaggedWarpUplinkProfile { path, profile } = e;
        (path, profile)
    });

    let uplink_aggregator =
        AggregatorTask::new(uplink_pulse_lanes, sample_rate, stop_rx, bp_stream, lane_tx);

    let task = async move {
        // Once `stop_rx` has been triggered, the uplink aggregator will initiate a shutdown process
        // that will drain the stream of incoming metrics. This stream is driven by uplink envelope
        // events that will also terminate as the stop trigger has been triggered. Once the
        // aggregator has finished draining the stream, the aggregator will produce one final metric
        // and feed it forward to the lane aggregator where the same process will happen and the
        // staged shutdown of the metrics system will finish.
        let _jh = tokio::spawn(release_task);
        let aggregator_result = uplink_aggregator.run(yield_after, uplink_to_lane_tx).await;
        flush_tx.trigger();
        aggregator_result
    };
    (task, uplink_tx)
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedWarpUplinkProfile {
    pub path: RelativePath,
    pub profile: WarpUplinkProfile,
}

impl TaggedWarpUplinkProfile {
    pub fn new(path: RelativePath, profile: WarpUplinkProfile) -> Self {
        TaggedWarpUplinkProfile { path, profile }
    }
}

impl Keyed for TaggedWarpUplinkProfile {
    type Key = RelativePath;

    fn key(&self) -> Self::Key {
        self.path.clone()
    }
}

async fn metrics_release_backpressure<E, Sink>(
    messages: impl Stream<Item = TaggedWarpUplinkProfile>,
    sink: Sink,
    config: MetricBackpressureConfig,
) -> Result<(), E>
where
    Sink: ItemSender<TaggedWarpUplinkProfile, E>,
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
