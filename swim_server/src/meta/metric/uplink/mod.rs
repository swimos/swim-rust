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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tracing::{event, Level};

use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::aggregator::{AddressedMetric, AggregatorTask, Metric, ProfileItem};
use crate::meta::metric::{AggregatorError, MetricKind};
use futures::{Future, Stream, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use swim_common::sink::item::{for_mpsc_sender, ItemSender};
use swim_warp::backpressure::keyed::{release_pressure, Keyed};
use tokio_stream::wrappers::ReceiverStream;
use utilities::sync::trigger;

const SEND_PROFILE_FAIL: &str = "Failed to send uplink profile";

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct TrySendError;

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
        let tagged = TaggedWarpUplinkProfile::tag(lane_id.clone(), profile);

        sender.try_send(tagged).map_err(|_| TrySendError)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaggedWarpUplinkProfile {
    pub path: RelativePath,
    pub profile: WarpUplinkProfile,
}

impl AddressedMetric for TaggedWarpUplinkProfile {
    type Metric = WarpUplinkProfile;

    fn unpack(self) -> (RelativePath, Self::Metric) {
        let TaggedWarpUplinkProfile { path, profile } = self;
        (path, profile)
    }

    fn path(&self) -> RelativePath {
        self.path.clone()
    }

    fn pack(payload: Self::Metric, path: RelativePath) -> Self {
        TaggedWarpUplinkProfile {
            path,
            profile: payload,
        }
    }
}

impl Keyed for TaggedWarpUplinkProfile {
    type Key = RelativePath;

    fn key(&self) -> Self::Key {
        self.path.clone()
    }
}

impl TaggedWarpUplinkProfile {
    pub fn tag(lane_id: RelativePath, profile: WarpUplinkProfile) -> Self {
        TaggedWarpUplinkProfile {
            path: lane_id,
            profile,
        }
    }

    pub fn split(self) -> (RelativePath, WarpUplinkProfile) {
        let TaggedWarpUplinkProfile { path, profile } = self;
        (path, profile)
    }
}

#[derive(Debug, Clone)]
pub struct TaggedWarpUplinkPulse {
    pub path: RelativePath,
    pub pulse: WarpUplinkPulse,
}

impl AddressedMetric for TaggedWarpUplinkPulse {
    type Metric = WarpUplinkPulse;

    fn unpack(self) -> (RelativePath, Self::Metric) {
        let TaggedWarpUplinkPulse { path, pulse } = self;
        (path, pulse)
    }

    fn path(&self) -> RelativePath {
        self.path.clone()
    }

    fn pack(payload: Self::Metric, path: RelativePath) -> Self {
        TaggedWarpUplinkPulse {
            path,
            pulse: payload,
        }
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct WarpUplinkPulse {
    pub link_count: u32,
    pub event_rate: u64,
    pub event_count: u64,
    pub command_rate: u64,
    pub command_count: u64,
}

impl WarpUplinkPulse {
    pub fn new(
        link_count: u32,
        event_rate: u64,
        event_count: u64,
        command_rate: u64,
        command_count: u64,
    ) -> Self {
        WarpUplinkPulse {
            link_count,
            event_rate,
            event_count,
            command_rate,
            command_count,
        }
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct WarpUplinkProfile {
    pub event_delta: u32,
    pub event_rate: u64,
    pub event_count: u64,
    pub command_delta: u32,
    pub command_rate: u64,
    pub command_count: u64,
    pub open_delta: u32,
    pub open_count: u32,
    pub close_delta: u32,
    pub close_count: u32,
}

unsafe impl Send for InnerObserver {}
unsafe impl Sync for InnerObserver {}

#[derive(Clone)]
pub struct InnerObserver {
    sending: Arc<AtomicBool>,
    event_delta: Arc<AtomicU32>,
    event_count: Arc<AtomicU64>,
    command_delta: Arc<AtomicU32>,
    command_count: Arc<AtomicU64>,
    open_delta: Arc<AtomicU32>,
    open_count: Arc<AtomicU32>,
    close_delta: Arc<AtomicU32>,
    close_count: Arc<AtomicU32>,
    last_report: Arc<UnsafeCell<Instant>>,
    report_interval: Duration,
    sender: UplinkProfileSender,
}

impl InnerObserver {
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

    fn accumulate_and_send(&self) {
        let InnerObserver {
            event_delta,
            event_count,
            command_delta,
            command_count,
            open_delta,
            close_delta,
            open_count,
            close_count,
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
        let event_count = event_count.fetch_add(event_delta as u64, Ordering::Acquire);

        let command_delta = command_delta.swap(0, Ordering::Acquire);
        let command_rate = calculate_rate(command_delta, dt);
        let command_count = command_count.fetch_add(command_delta as u64, Ordering::Acquire);

        let profile_open_delta = open_delta.swap(0, Ordering::Relaxed);
        let profile_open_count = open_count.fetch_add(profile_open_delta, Ordering::Acquire);
        let profile_close_delta = close_delta.swap(0, Ordering::Relaxed);
        let profile_close_count = close_count.fetch_add(profile_close_delta, Ordering::Acquire);

        let profile = WarpUplinkProfile {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
            open_delta: profile_open_delta,
            open_count: profile_open_count,
            close_delta: profile_close_delta,
            close_count: profile_close_count,
        };

        if sender.try_send(profile).is_err() {
            let lane = &sender.lane_id;
            event!(Level::DEBUG, ?lane, SEND_PROFILE_FAIL);
        }
    }
}

pub fn uplink_observer(
    report_interval: Duration,
    sender: UplinkProfileSender,
) -> (UplinkEventObserver, UplinkActionObserver) {
    let inner = InnerObserver {
        sending: Arc::new(AtomicBool::new(false)),
        event_delta: Arc::new(AtomicU32::new(0)),
        event_count: Arc::new(AtomicU64::new(0)),
        command_delta: Arc::new(AtomicU32::new(0)),
        command_count: Arc::new(AtomicU64::new(0)),
        open_delta: Arc::new(AtomicU32::new(0)),
        open_count: Arc::new(AtomicU32::new(0)),
        close_delta: Arc::new(AtomicU32::new(0)),
        close_count: Arc::new(AtomicU32::new(0)),
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
    pub fn on_command(&self) {
        let _old = self.inner.command_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }

    pub fn did_open(&self) {
        let _old = self.inner.open_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }

    pub fn did_close(&self) {
        let _old = self.inner.close_delta.fetch_add(1, Ordering::Acquire);
        self.inner.flush();
    }

    #[cfg(test)]
    pub(crate) fn set_inner_values(&self, to: u32) {
        let InnerObserver {
            event_delta,
            command_delta,
            open_delta,
            close_delta,
            ..
        } = &*self.inner;

        event_delta.store(to, Ordering::Relaxed);
        command_delta.store(to, Ordering::Relaxed);
        open_delta.store(to, Ordering::Relaxed);
        close_delta.store(to, Ordering::Relaxed);
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
    lane_tx: mpsc::Sender<TaggedWarpUplinkProfile>,
) -> (
    impl Future<Output = Result<(), AggregatorError>>,
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
            let inner = ProfileItem::new(
                TaggedWarpUplinkProfile::pack(WarpUplinkProfile::default(), k.clone()),
                v,
            );
            (k, inner)
        })
        .collect();

    let uplink_aggregator = AggregatorTask::new(
        uplink_pulse_lanes,
        sample_rate,
        stop_rx,
        ReceiverStream::new(bp_stream),
        lane_tx,
    );

    let task = async move {
        try_join(uplink_aggregator.run(yield_after), release_task)
            .await
            .map(|_| ())
    };

    (task, uplink_tx)
}

impl Metric<WarpUplinkProfile> for TaggedWarpUplinkProfile {
    const METRIC_KIND: MetricKind = MetricKind::Uplink;

    type Pulse = WarpUplinkPulse;

    fn accumulate(&mut self, profile: WarpUplinkProfile) {
        let WarpUplinkProfile {
            event_delta: profile_event_delta,
            event_rate: profile_event_rate,
            command_delta: profile_command_delta,
            command_rate: profile_command_rate,
            open_delta: profile_open_delta,
            open_count: profile_open_count,
            close_delta: profile_close_delta,
            close_count: profile_close_count,
            ..
        } = profile;

        let WarpUplinkProfile {
            event_delta,
            event_rate,
            command_delta,
            command_rate,
            open_delta,
            open_count,
            close_delta,
            close_count,
            ..
        } = &mut self.profile;

        *event_delta += event_delta.wrapping_add(profile_event_delta);
        *event_rate += event_rate.wrapping_add(profile_event_rate);

        *command_delta += command_delta.wrapping_add(profile_command_delta);
        *command_rate += command_rate.wrapping_add(profile_command_rate);

        *open_delta += open_delta.wrapping_add(profile_open_delta);
        *open_count += open_count.wrapping_add(profile_open_count);

        *close_delta += close_delta.wrapping_add(profile_close_delta);
        *close_count += close_count.wrapping_add(profile_close_count);
    }

    fn collect(&mut self) -> Self {
        self.clone()
    }

    fn as_pulse(&self) -> Self::Pulse {
        let WarpUplinkProfile {
            event_delta,
            event_rate,
            command_delta,
            command_rate,
            open_count,
            close_count,
            ..
        } = self.profile;

        let link_count = open_count.saturating_sub(close_count);

        WarpUplinkPulse {
            link_count,
            event_rate,
            event_count: event_delta as u64,
            command_rate,
            command_count: command_delta as u64,
        }
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
