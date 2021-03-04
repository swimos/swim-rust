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

use futures::future::try_join;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
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
pub struct SendError;

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

    pub fn try_send(&self, profile: WarpUplinkProfile) -> Result<(), SendError> {
        let UplinkProfileSender { lane_id, sender } = self;
        let tagged = TaggedWarpUplinkProfile::tag(lane_id.clone(), profile);

        sender.try_send(tagged).map_err(|_| SendError)
    }
}

#[derive(Debug, Clone)]
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
    pub event_delta: i32,
    pub event_rate: u64,
    pub event_count: u64,
    pub command_delta: i32,
    pub command_rate: u64,
    pub command_count: u64,
}

impl WarpUplinkPulse {
    pub fn new(
        event_delta: i32,
        event_rate: u64,
        event_count: u64,
        command_delta: i32,
        command_rate: u64,
        command_count: u64,
    ) -> Self {
        WarpUplinkPulse {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
        }
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct WarpUplinkProfile {
    pub event_delta: i32,
    pub event_rate: u64,
    pub event_count: u64,
    pub command_delta: i32,
    pub command_rate: u64,
    pub command_count: u64,
    pub open_delta: i32,
    pub open_count: u64,
    pub close_delta: i32,
    pub close_count: u64,
}

impl WarpUplinkProfile {
    pub fn new(
        event_delta: i32,
        event_rate: u64,
        event_count: u64,
        command_delta: i32,
        command_rate: u64,
        command_count: u64,
        open_delta: i32,
        open_count: u64,
        close_delta: i32,
        close_count: u64,
    ) -> Self {
        WarpUplinkProfile {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
            open_delta,
            open_count,
            close_delta,
            close_count,
        }
    }
}

#[derive(Clone)]
pub struct InnerObserver {
    sending: Arc<AtomicBool>,
    event_delta: Arc<AtomicI32>,
    event_count: Arc<AtomicU64>,
    command_delta: Arc<AtomicI32>,
    command_count: Arc<AtomicU64>,
    open_delta: Arc<AtomicI32>,
    close_delta: Arc<AtomicI32>,
    last_report: Arc<UnsafeCell<Instant>>,
    report_interval: Duration,
    sender: UplinkProfileSender,
}

impl InnerObserver {
    fn flush(&self) {
        let InnerObserver {
            sending,
            event_delta,
            event_count,
            command_delta,
            command_count,
            open_delta,
            close_delta,
            last_report,
            report_interval,
            sender,
        } = self;

        if let Ok(false) =
            sending.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        {
            let last_reported = unsafe { *last_report.get() };
            if last_reported.elapsed() < *report_interval {
                sending.store(false, Ordering::Release);
                return;
            }

            let now = Instant::now();
            let dt = now.duration_since(last_reported).as_secs_f64();

            let event_delta = event_delta.swap(0, Ordering::Acquire);
            let event_rate = ((event_delta * 1000) as f64 / dt).ceil() as u64;
            let event_count = event_count.fetch_add(event_rate, Ordering::Acquire);

            let command_delta = command_delta.swap(0, Ordering::Acquire);
            let command_rate = ((command_delta * 1000) as f64 / dt).ceil() as u64;
            let command_count = command_count.fetch_add(command_rate, Ordering::Acquire);

            let open_delta = open_delta.load(Ordering::Relaxed);
            let close_delta = close_delta.load(Ordering::Relaxed);

            let profile = WarpUplinkProfile {
                event_delta,
                event_rate,
                event_count,
                command_delta,
                command_rate,
                command_count,
                open_delta,
                open_count: 0, //todo
                close_delta,
                close_count: 0,
            };

            if sender.try_send(profile).is_err() {
                let lane = &sender.lane_id;
                event!(Level::WARN, ?lane, SEND_PROFILE_FAIL);
            }

            unsafe {
                *last_report.get() = Instant::now();
            }

            sending.store(false, Ordering::Release);
        }
    }
}

pub fn uplink_observer(
    report_interval: Duration,
    sender: UplinkProfileSender,
) -> (UplinkEventObserver, UplinkActionObserver) {
    let inner = InnerObserver {
        sending: Arc::new(AtomicBool::new(false)),
        event_delta: Arc::new(AtomicI32::new(0)),
        event_count: Arc::new(AtomicU64::new(0)),
        command_delta: Arc::new(AtomicI32::new(0)),
        command_count: Arc::new(AtomicU64::new(0)),
        open_delta: Arc::new(AtomicI32::new(0)),
        close_delta: Arc::new(AtomicI32::new(0)),
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
        let _old = self.inner.event_count.fetch_add(1, Ordering::Acquire);
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
        let _old = self.inner.command_count.fetch_add(1, Ordering::Acquire);
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
        Some(lane_tx),
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

    fn accumulate(&mut self, _new: WarpUplinkProfile) {
        unimplemented!()
    }

    fn reset(&mut self) {
        unimplemented!()
    }

    fn as_pulse(&self) -> Self::Pulse {
        unimplemented!()
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

#[cfg(test)]
mod tests {
    // use super::*;
    // use futures::FutureExt;
    // use tokio::sync::mpsc;
    //
    // #[tokio::test]
    // async fn test_uplink_surjection() {
    //     let path = RelativePath::new("/node", "/lane");
    //     let (tx, mut rx) = mpsc::channel(1);
    //     let sender = ProfileSender::new(UplinkSurjection(path.clone()), tx);
    //     let profile = WarpUplinkProfile::default();
    //
    //     assert!(sender.try_send(profile.clone()).is_ok());
    //     assert_eq!(
    //         rx.recv().now_or_never().unwrap().unwrap(),
    //         ObserverEvent::Uplink(path, profile)
    //     );
    // }
    //
    // #[tokio::test]
    // async fn test_receive() {
    //     let path = RelativePath::new("/node", "/lane");
    //     let (tx, mut rx) = mpsc::channel(1);
    //     let sender = ProfileSender::new(UplinkSurjection(path.clone()), tx);
    //
    //     let (event_observer, action_observer) = uplink_observer(Duration::from_nanos(1), sender);
    //
    //     event_observer
    //         .inner
    //         .command_count
    //         .fetch_add(2, Ordering::Acquire);
    //     event_observer
    //         .inner
    //         .event_count
    //         .fetch_add(2, Ordering::Acquire);
    //
    //     event_observer.inner.flush();
    //
    //     let received = rx.recv().await.unwrap();
    //
    //     match received {
    //         ObserverEvent::Uplink(_, profile) => {
    //             assert_eq!(profile.event_count, 2);
    //             assert_eq!(profile.command_count, 2);
    //         }
    //         _ => {
    //             panic!("Unexpected event kind")
    //         }
    //     }
    // }
}
