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

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tracing::{event, Level};

use crate::agent::lane::model::supply::supplier::TrySupplyError;
use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::aggregator::{Addressed, MetricAggregator};
use crate::meta::metric::REMOVING_LANE;
use crate::meta::metric::{try_send, AggregatorKind};
use swim_warp::backpressure::keyed::Keyed;

const SEND_PROFILE_FAIL: &str = "Failed to send uplink profile";
const SEND_PULSE_FAIL: &str = "Failed to send uplink pulse";
pub const MISSING_LANE: &str = "Lane does not exist";

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

impl Addressed for TaggedWarpUplinkProfile {
    type Tag = RelativePath;

    fn address(&self) -> &Self::Tag {
        &self.path
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

pub struct TaggedWarpUplinkPulse {
    pub lane_id: RelativePath,
    pub profile: WarpUplinkPulse,
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

impl From<WarpUplinkProfile> for WarpUplinkPulse {
    fn from(profile: WarpUplinkProfile) -> Self {
        let WarpUplinkProfile {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
            ..
        } = profile;

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
    pub close_delta: i32,
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
        close_delta: i32,
    ) -> Self {
        WarpUplinkProfile {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
            open_delta,
            close_delta,
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
                close_delta,
            };

            if sender.try_send(profile).is_err() {
                let lane = &sender.lane_id;
                event!(Level::WARN, ?lane, SEND_PROFILE_FAIL);
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

pub struct UplinkAggregatorTask {
    pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>,
}

impl UplinkAggregatorTask {
    pub fn new(pulse_lanes: HashMap<RelativePath, SupplyLane<WarpUplinkPulse>>) -> Self {
        UplinkAggregatorTask { pulse_lanes }
    }
}

impl MetricAggregator for UplinkAggregatorTask {
    const AGGREGATOR_KIND: AggregatorKind = AggregatorKind::Uplink;

    type Input = TaggedWarpUplinkProfile;
    type Output = TaggedWarpUplinkProfile;

    fn on_receive(&mut self, tagged_profile: Self::Input) -> Result<Option<Self::Input>, ()> {
        let TaggedWarpUplinkProfile { path, profile } = tagged_profile;
        let lane_uri = &path.lane;

        match self.pulse_lanes.get(&path) {
            Some(lane) => {
                let pulse = profile.clone().into();
                try_send(lane, pulse, &path, &mut self.pulse_lanes);
            }
            None => {
                event!(Level::WARN, ?lane_uri, MISSING_LANE);
            }
        }
        Ok(Some(TaggedWarpUplinkProfile { path, profile }))
    }
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
