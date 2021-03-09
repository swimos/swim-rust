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
use crate::meta::metric::aggregator::AddressedMetric;
use crate::meta::metric::lane::TaggedLaneProfile;
use crate::meta::metric::uplink::WarpUplinkPulse;
use crate::meta::metric::{AggregatorError, AggregatorErrorKind, MetricKind, WarpLaneProfile};
use crate::meta::metric::{STOP_CLOSED, STOP_OK};
use futures::select;
use futures::FutureExt;
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use utilities::sync::trigger;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct NodeProfile {
    pub uplink_event_delta: u32,
    pub uplink_event_rate: u64,
    pub uplink_event_count: u64,
    pub uplink_command_delta: u32,
    pub uplink_command_rate: u64,
    pub uplink_command_count: u64,
    pub uplink_open_delta: u32,
    pub uplink_open_count: u32,
    pub uplink_close_delta: u32,
    pub uplink_close_count: u32,
}

impl NodeProfile {
    fn accumulate(&mut self, other: WarpLaneProfile) {
        let NodeProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_command_delta,
            uplink_command_rate,
            uplink_open_delta,
            uplink_open_count,
            uplink_close_delta,
            uplink_close_count,
            ..
        } = self;

        let WarpLaneProfile {
            uplink_event_delta: other_uplink_event_delta,
            uplink_event_rate: other_uplink_event_rate,
            uplink_command_delta: other_uplink_command_delta,
            uplink_command_rate: other_uplink_command_rate,
            uplink_open_delta: other_uplink_open_delta,
            uplink_open_count: other_uplink_open_count,
            uplink_close_delta: other_uplink_close_delta,
            uplink_close_count: other_uplink_close_count,
            ..
        } = other;

        *uplink_event_delta = uplink_event_delta.wrapping_add(other_uplink_event_delta);
        *uplink_event_rate = uplink_event_rate.wrapping_add(other_uplink_event_rate);

        *uplink_command_delta = uplink_command_delta.wrapping_add(other_uplink_command_delta);
        *uplink_command_rate = uplink_command_rate.wrapping_add(other_uplink_command_rate);

        *uplink_open_delta = uplink_open_delta.wrapping_add(other_uplink_open_delta);
        *uplink_open_count = uplink_open_count.wrapping_add(other_uplink_open_count);

        *uplink_close_delta = uplink_close_delta.wrapping_add(other_uplink_close_delta);
        *uplink_close_count = uplink_close_count.wrapping_add(other_uplink_close_count);
    }

    fn collect(&mut self) -> NodePulse {
        let NodeProfile {
            uplink_event_delta,
            uplink_event_rate,
            uplink_event_count,
            uplink_command_delta,
            uplink_command_rate,
            uplink_command_count,
            uplink_open_delta,
            uplink_open_count,
            uplink_close_delta,
            uplink_close_count,
        } = self;

        let link_count = uplink_open_count.saturating_sub(*uplink_close_count);
        let event_count = uplink_event_count.wrapping_add(*uplink_event_delta as u64);
        let command_count = uplink_command_count.wrapping_add(*uplink_command_delta as u64);

        let pulse = NodePulse {
            uplinks: WarpUplinkPulse {
                link_count,
                event_rate: *uplink_event_rate,
                event_count,
                command_rate: *uplink_command_rate,
                command_count,
            },
        };

        *uplink_open_delta = 0;
        *uplink_close_delta = 0;
        *uplink_event_delta = 0;
        *uplink_event_rate = 0;
        *uplink_command_delta = 0;
        *uplink_command_rate = 0;

        pulse
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct NodePulse {
    pub uplinks: WarpUplinkPulse,
}

#[derive(Debug, Clone)]
pub struct TaggedNodeProfile {
    path: RelativePath,
    profile: NodeProfile,
}

pub struct NodeAggregatorTask {
    stop_rx: trigger::Receiver,
    sample_rate: Duration,
    last_report: Instant,
    lane: SupplyLane<NodePulse>,
    input: ReceiverStream<TaggedLaneProfile>,
    profile: NodeProfile,
}

impl NodeAggregatorTask {
    pub fn new(
        stop_rx: trigger::Receiver,
        sample_rate: Duration,
        lane: SupplyLane<NodePulse>,
        input: ReceiverStream<TaggedLaneProfile>,
    ) -> Self {
        NodeAggregatorTask {
            stop_rx,
            sample_rate,
            last_report: Instant::now(),
            lane,
            input,
            profile: NodeProfile::default(),
        }
    }
    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        let NodeAggregatorTask {
            stop_rx,
            sample_rate,
            mut last_report,
            lane,
            input,
            mut profile,
        } = self;

        let mut fused_metric_rx = input.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let error = loop {
            let event: Option<TaggedLaneProfile> = select! {
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
                Some(tagged) => {
                    let (_, new_profile) = tagged.unpack();
                    profile.accumulate(new_profile);
                    let pulse = profile.collect();

                    if last_report.elapsed() > sample_rate {
                        if let Err(TrySupplyError::Closed) = lane.try_send(pulse) {
                            break AggregatorErrorKind::ForwardChannelClosed;
                        } else {
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

        return Err(AggregatorError {
            aggregator: MetricKind::Node,
            error,
        });
    }
}
