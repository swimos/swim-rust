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
    type Payload: Send + Sync + Clone + Default;

    fn split(self) -> (RelativePath, Self::Payload);

    fn tag_ref(&self) -> &RelativePath;

    fn tag(payload: Self::Payload, path: RelativePath) -> Self;
}

type Payload<AM> = <AM as AddressedMetric>::Payload;

pub trait MetricStage {
    const METRIC_KIND: MetricKind;

    type ProfileIn: AddressedMetric;
    type ProfileOut: AddressedMetric + Clone;
    type Pulse: From<Payload<Self::ProfileOut>> + Send + Sync + 'static;

    fn aggregate(
        old: Payload<Self::ProfileOut>,
        new: Payload<Self::ProfileIn>,
    ) -> Payload<Self::ProfileOut>;
}

struct Pulse<M: MetricStage> {
    previous: M::ProfileOut,
    lane: SupplyLane<M::Pulse>,
}

impl<M: MetricStage> Pulse<M> {
    pub fn new(previous: M::ProfileOut, lane: SupplyLane<M::Pulse>) -> Pulse<M> {
        Pulse { previous, lane }
    }
}

pub struct AggregatorTask<M, S>
where
    M: MetricStage,
    S: Stream<Item = M::ProfileIn> + Unpin,
{
    stop_rx: trigger::Receiver,
    sample_rate: Duration,
    last_report: Instant,
    pulse_lanes: HashMap<RelativePath, Pulse<M>>,
    input: S,
    output: Option<mpsc::Sender<M::ProfileOut>>,
}

impl<M, S> AggregatorTask<M, S>
where
    M: MetricStage,
    S: Stream<Item = M::ProfileIn> + Unpin,
{
    pub fn new(
        pulse_lanes: HashMap<RelativePath, SupplyLane<M::Pulse>>,
        sample_rate: Duration,
        stop_rx: trigger::Receiver,
        input: S,
        output: Option<mpsc::Sender<M::ProfileOut>>,
    ) -> AggregatorTask<M, S> {
        let pulse_lanes = pulse_lanes
            .into_iter()
            .map(|(k, lane)| {
                let key = k.clone();
                let profile = M::ProfileOut::tag(Default::default(), k);
                let pulse = Pulse::new(profile, lane);

                (key, pulse)
            })
            .collect();

        AggregatorTask {
            stop_rx,
            sample_rate,
            last_report: Instant::now(),
            pulse_lanes,
            input,
            output,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        let AggregatorTask {
            stop_rx,
            sample_rate,
            mut last_report,
            mut pulse_lanes,
            input,
            output,
        } = self;

        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let error = loop {
            let event: Option<M::ProfileIn> = select! {
                _ = fused_trigger => {
                    event!(Level::WARN, STOP_OK);
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
                    let (tag, payload) = profile.split();

                    match pulse_lanes.remove(&tag) {
                        Some(Pulse { previous, lane }) => {
                            // split old profile
                            let (profile_tag, old_profile) = previous.split();
                            // aggregate profile with old
                            let updated_profile = M::aggregate(old_profile, payload.clone());
                            // create pulse
                            let pulse = M::Pulse::from(updated_profile.clone());
                            // tag profile for forwarding
                            let tagged_profile = M::ProfileOut::tag(updated_profile, profile_tag);

                            if last_report.elapsed() > sample_rate {
                                // send pulse
                                if let Err(TrySupplyError::Closed) = lane.try_send(pulse) {
                                    // the lane has already been removed
                                    event!(Level::DEBUG, ?tag, REMOVING_LANE);
                                }

                                last_report = Instant::now();
                            }

                            // send tagged profile
                            if let Some(channel) = &output {
                                if let Err(TrySendError::Closed(_)) =
                                    channel.try_send(tagged_profile.clone())
                                {
                                    break AggregatorErrorKind::ForwardChannelClosed;
                                }
                            }

                            // store tagged profile
                            let _ = pulse_lanes.insert(tag, Pulse::new(tagged_profile, lane));
                        }
                        None => {
                            event!(Level::DEBUG, ?tag, LANE_NOT_FOUND);
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

        return Err(AggregatorError {
            aggregator: M::METRIC_KIND,
            error,
        });
    }
}
