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

use futures::future::try_join;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tracing::{event, Level};

use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::aggregator::{AggregatorTask, MetricState};
use crate::meta::metric::MetricReporter;
use crate::meta::metric::{AggregatorError, MetricStage};
use futures::{Future, Stream, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Add;
use swim_common::sink::item::{for_mpsc_sender, ItemSender};
use swim_warp::backpressure::keyed::{release_pressure, Keyed};
use tokio_stream::wrappers::ReceiverStream;
use utilities::sync::trigger;

impl Add<WarpUplinkProfile> for WarpUplinkProfile {
    type Output = WarpUplinkProfile;

    fn add(self, rhs: WarpUplinkProfile) -> Self::Output {
        WarpUplinkProfile {
            event_delta: self.event_delta + rhs.event_delta,
            event_rate: self.event_rate + rhs.event_rate,
            command_delta: self.command_delta + rhs.command_delta,
            command_rate: self.command_rate + rhs.command_rate,
            open_delta: self.open_delta + rhs.open_delta,
            close_delta: self.close_delta + rhs.close_delta,
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

        *open_count += open_delta;
        *close_count += close_delta;
        *event_count += event_delta as u64;
        *command_count += command_delta as u64;

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

unsafe impl Send for InnerObserver {}
unsafe impl Sync for InnerObserver {}

/// An inner observer for event and action observers to allow for interior mutability and for
/// channels observers to be cloned.
#[derive(Clone, Debug)]
pub struct InnerObserver {
    sending: Arc<AtomicBool>,
    event_delta: Arc<AtomicU32>,
    command_delta: Arc<AtomicU32>,
    open_delta: Arc<AtomicU32>,
    close_delta: Arc<AtomicU32>,
    last_report: Arc<UnsafeCell<Instant>>,
    report_interval: Duration,
    sender: UplinkProfileSender,
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

/// Creates a new event and action observer pair that will send profiles when new events occur at
/// `report_interval` to `sender`.
pub fn uplink_observer(
    report_interval: Duration,
    sender: UplinkProfileSender,
) -> (UplinkEventObserver, UplinkActionObserver) {
    let inner = InnerObserver {
        sending: Arc::new(AtomicBool::new(false)),
        event_delta: Arc::new(AtomicU32::new(0)),
        command_delta: Arc::new(AtomicU32::new(0)),
        open_delta: Arc::new(AtomicU32::new(0)),
        close_delta: Arc::new(AtomicU32::new(0)),
        last_report: Arc::new(UnsafeCell::new(Instant::now())),
        report_interval,
        sender,
    };

    let arc = Arc::new(inner);
    (
        UplinkEventObserver::new(arc.clone()),
        UplinkActionObserver::new(arc),
    )
}

#[derive(Clone)]
pub struct UplinkEventObserver {
    inner: Arc<InnerObserver>,
}

impl UplinkEventObserver {
    fn new(inner: Arc<InnerObserver>) -> UplinkEventObserver {
        UplinkEventObserver { inner }
    }
}

impl UplinkEventObserver {
    pub fn on_event(&self) {
        let _old = self.inner.event_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }
}

#[derive(Clone)]
pub struct UplinkActionObserver {
    inner: Arc<InnerObserver>,
}

impl UplinkActionObserver {
    pub fn new(inner: Arc<InnerObserver>) -> Self {
        UplinkActionObserver { inner }
    }
}

impl UplinkActionObserver {
    /// Report that a new command message has been dispatched and send a new profile if the
    /// report interval has elapsed.
    pub fn on_command(&self) {
        let _old = self.inner.command_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }

    /// Report that a new uplink opened and send a new profile if the report interval has elapsed.
    pub fn did_open(&self) {
        let _old = self.inner.open_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }

    /// Report that an uplink closed and send a new profile if the report interval has elapsed.
    pub fn did_close(&self) {
        let _old = self.inner.close_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
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

    #[cfg(test)]
    pub(crate) fn force_flush(&self) {
        self.inner.accumulate_and_send();
    }
}

pub fn uplink_aggregator(
    stop_rx: trigger::Receiver,
    sample_rate: Duration,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
    backpressure_config: KeyedBackpressureConfig,
    uplink_pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
    lane_tx: mpsc::Sender<(RelativePath, WarpUplinkProfile)>,
) -> (
    impl Future<Output = Result<MetricStage, AggregatorError>>,
    mpsc::Sender<TaggedWarpUplinkProfile>,
) {
    let (uplink_tx, uplink_rx) = mpsc::channel(buffer_size.get());

    let metric_stream = ReceiverStream::new(uplink_rx);
    let (sink, bp_stream) = mpsc::channel(buffer_size.get());
    let sink = for_mpsc_sender(sink).map_err_into::<AggregatorError>();

    let release_task = metrics_release_backpressure(
        metric_stream.take_until(stop_rx.clone()),
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
        try_join(uplink_aggregator.run(yield_after), release_task)
            .await
            .map(|(stage, _)| stage)
    };
    (task, uplink_tx)
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedWarpUplinkProfile {
    pub path: RelativePath,
    pub profile: WarpUplinkProfile,
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
    config: KeyedBackpressureConfig,
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
