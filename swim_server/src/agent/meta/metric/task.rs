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

use crate::agent::meta::metric::{MetricKind, ObserverEvent, Profile};
use futures::select;
use futures::FutureExt;
use futures::StreamExt;
use pin_utils::core_reexport::fmt::Formatter;
use std::collections::HashMap;
use std::fmt::Display;
use std::num::NonZeroUsize;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tracing::{event, Level};
use utilities::sync::trigger;

type ProfileResult = Result<Profile, ProfileRequestErr>;

const REQ_TX_ERR: &str = "Failed to return requested profile";

#[derive(Debug, PartialEq)]
pub enum ProfileRequestErr {
    NoSuchNode,
    NoSuchLane(RelativePath),
    NoSuchUplink(RelativePath),
    ChannelError(String),
}

impl From<MetricKind> for ProfileRequestErr {
    fn from(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Node => ProfileRequestErr::NoSuchNode,
            MetricKind::Lane(lane) => ProfileRequestErr::NoSuchLane(lane),
            MetricKind::Uplink(uplink) => ProfileRequestErr::NoSuchUplink(uplink),
        }
    }
}

impl From<SendError<ProfileRequest>> for ProfileRequestErr {
    fn from(e: SendError<ProfileRequest>) -> Self {
        ProfileRequestErr::ChannelError(e.to_string())
    }
}

pub struct ProfileRequest {
    kind: MetricKind,
    rx: oneshot::Sender<ProfileResult>,
}

impl ProfileRequest {
    pub fn new(kind: MetricKind, rx: oneshot::Sender<ProfileResult>) -> ProfileRequest {
        ProfileRequest { kind, rx }
    }
}

pub struct CollectorTask {
    node_id: String,
    // todo replace with `AddressedMetric` - a router handle to the lane to pipe the profiles
    profiles: HashMap<MetricKind, Profile>,
    stop_rx: trigger::Receiver,
    request_rx: mpsc::Receiver<ProfileRequest>,
    metric_rx: mpsc::Receiver<ObserverEvent>,
}

pub enum CollectorStopResult {
    Normal,
    Abnormal,
}

impl Display for CollectorStopResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectorStopResult::Normal => write!(f, "Collector stopped normally"),
            CollectorStopResult::Abnormal => write!(f, "Collected stopped abnormally"),
        }
    }
}

enum CollectorEvent {
    // No deregister as this happens when the channel closes
    Register(MetricKind),
    ObserverEvent(ObserverEvent),
    Request(ProfileRequest),
}

impl CollectorTask {
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        buffer: NonZeroUsize,
        metric_rx: mpsc::Receiver<ObserverEvent>,
    ) -> (CollectorTask, mpsc::Sender<ProfileRequest>) {
        let (request_tx, request_rx) = mpsc::channel(buffer.get());

        (
            CollectorTask {
                node_id,
                profiles: Default::default(),
                stop_rx,
                request_rx,
                metric_rx,
            },
            request_tx,
        )
    }

    // todo: pipe updates to the supply lanes
    pub async fn run(self) -> CollectorStopResult {
        let CollectorTask {
            node_id,
            mut profiles,
            stop_rx,
            request_rx,
            metric_rx,
        } = self;

        let mut fused_request_rx = request_rx.fuse();
        let mut fused_metric_rx = metric_rx.fuse();
        let mut fused_trigger = stop_rx.fuse();

        let stop_code = loop {
            let event: Option<CollectorEvent> = select! {
                _ = fused_trigger => {
                    break CollectorStopResult::Normal;
                },
                req = fused_request_rx.next() => req.map(CollectorEvent::Request),
                metric = fused_metric_rx.next() => metric.map(CollectorEvent::ObserverEvent),
            };
            match event {
                None => break CollectorStopResult::Abnormal,
                Some(CollectorEvent::Request(request)) => {
                    let ProfileRequest { kind, rx } = request;
                    let profile = profiles
                        .get(&kind)
                        .map(Clone::clone)
                        .ok_or_else(move || kind.into());

                    if rx.send(profile).is_err() {
                        event!(Level::ERROR, REQ_TX_ERR);
                    }
                }
                Some(CollectorEvent::ObserverEvent(event)) => match event {
                    ObserverEvent::Node(profile) => {
                        profiles.insert(MetricKind::Node, Profile::Node(profile));
                    }
                    ObserverEvent::Lane(addr, profile) => {
                        profiles.insert(MetricKind::Lane(addr), Profile::Lane(profile));
                    }
                    ObserverEvent::Uplink(addr, profile) => {
                        profiles.insert(MetricKind::Uplink(addr), Profile::Uplink(profile));
                    }
                },
                // register events should only occur when the meta lane has been linked to
                Some(CollectorEvent::Register(_kind)) => {
                    unimplemented!()
                }
                // deregister events should only occur when the meta lane has been unlinked from
                Some(CollectorEvent::Deregister(_kind)) => {
                    unimplemented!()
                }
            }
        };

        event!(Level::INFO, %stop_code, %node_id, "Metric collector stopped");

        stop_code
    }
}
