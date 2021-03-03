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

use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::lane::TaggedLaneProfile;
use crate::meta::metric::{AggregatorError, AggregatorKind, LaneProfile};
use std::num::NonZeroUsize;
use swim_common::form::Form;
use tokio::sync::mpsc;
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

pub struct NodeAggregatorTask {
    stop_rx: trigger::Receiver,
    metric_rx: mpsc::Receiver<TaggedLaneProfile>,
    pulse_lane: SupplyLane<NodePulse>,
}

impl NodeAggregatorTask {
    const COLLECTOR_KIND: AggregatorKind = AggregatorKind::Node;

    pub fn new(
        stop_rx: trigger::Receiver,
        metric_rx: mpsc::Receiver<TaggedLaneProfile>,
        pulse_lane: SupplyLane<NodePulse>,
    ) -> NodeAggregatorTask {
        NodeAggregatorTask {
            stop_rx,
            metric_rx,
            pulse_lane,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> Result<(), AggregatorError> {
        // let NodeAggregatorTask {
        //     stop_rx,
        //     metric_rx,
        //     pulse_lane,
        // } = self;
        //
        // let mut fused_metric_rx = ReceiverStream::new(metric_rx).fuse();
        // let mut fused_trigger = stop_rx.fuse();
        // let mut iteration_count: usize = 0;
        //
        // let yield_mod = yield_after.get();
        //
        // let stop_code = loop {
        //     let event: Option<TaggedLaneProfile> = select! {
        //         _ = fused_trigger => {
        //             event!(Level::WARN, %node_id, STOP_OK);
        //             break NodeAggregatorStopResult::Normal;
        //         },
        //         metric = fused_metric_rx.next() => metric,
        //     };
        //     match event {
        //         None => {
        //             event!(Level::WARN, %node_id, STOP_CLOSED);
        //             break NodeAggregatorStopResult::Abnormal;
        //         }
        //         Some(profile) => {
        //             // let (path, uplink_profile) = profile.split();
        //             // let lane_identifier = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
        //             //     node_uri: path.node,
        //             //     lane_uri: path.lane,
        //             // });
        //
        //             unimplemented!()
        //         }
        //     }
        //
        //     iteration_count = iteration_count.wrapping_add(1);
        //     if iteration_count % yield_mod == 0 {
        //         tokio::task::yield_now().await;
        //     }
        // };
        //
        // event!(Level::INFO, %stop_code, %node_id, STOP_OK);
        //
        // stop_code
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
