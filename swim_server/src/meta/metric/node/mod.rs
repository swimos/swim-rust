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
use crate::meta::metric::{AggregatorError, AggregatorErrorKind, LaneProfile, MetricKind};
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
pub struct NodeProfile;

impl NodeProfile {
    pub fn accumulate(&mut self, _profile: &LaneProfile) {
        unimplemented!()
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct NodePulse;

impl NodePulse {
    fn accumulate(&mut self, _other: LaneProfile) {
        unimplemented!()
    }
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
    pulse: NodePulse,
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
            pulse: NodePulse::default(),
        }
    }
    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        let NodeAggregatorTask {
            stop_rx,
            sample_rate,
            mut last_report,
            lane,
            input,
            mut pulse,
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
                    let (_, profile) = tagged.unpack();
                    // pulse.accumulate(profile);

                    if last_report.elapsed() > sample_rate {
                        if let Err(TrySupplyError::Closed) = lane.try_send(pulse.clone()) {
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

impl From<NodeProfile> for NodePulse {
    fn from(_: NodeProfile) -> Self {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    // use crate::agent::meta::metric::node::NodeProfile;
    // use crate::agent::meta::metric::sender::TransformedSender;
    // use crate::agent::meta::metric::ObserverEvent;
    // use futures::FutureExt;
    // use tokio::sync::mpsc;
    //
    // #[tokio::test]
    // async fn test_node_surjection() {
    //     let (tx, mut rx) = mpsc::channel(1);
    //     let sender = TransformedSender::new(ObserverEvent::Node, tx);
    //     let profile = NodeProfile::default();
    //
    //     assert!(sender.try_send(profile.clone()).is_ok());
    //     assert_eq!(
    //         rx.recv().now_or_never().unwrap().unwrap(),
    //         ObserverEvent::Node(profile)
    //     );
    // }
}
