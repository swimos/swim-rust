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
use futures::FutureExt;
use std::collections::HashMap;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::meta::metric::aggregator::{Addressed, MetricAggregator};
use crate::meta::metric::uplink::TaggedWarpUplinkProfile;
use crate::meta::metric::{AggregatorKind, WarpUplinkProfile};
use futures::future::BoxFuture;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LaneProfile;

impl LaneProfile {
    pub fn accumulate(&mut self, _profile: &WarpUplinkProfile) {
        unimplemented!()
    }
}

pub struct TaggedLaneProfile {
    path: RelativePath,
    profile: LaneProfile,
}

impl Addressed for TaggedLaneProfile {
    type Tag = RelativePath;

    fn address(&self) -> &Self::Tag {
        &self.path
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LanePulse;

pub struct LaneAggregatorTask {
    pulse_lanes: HashMap<RelativePath, SupplyLane<LanePulse>>,
}

impl LaneAggregatorTask {
    pub fn new(pulse_lanes: HashMap<RelativePath, SupplyLane<LanePulse>>) -> Self {
        LaneAggregatorTask { pulse_lanes }
    }
}

impl MetricAggregator for LaneAggregatorTask {
    const AGGREGATOR_KIND: AggregatorKind = AggregatorKind::Lane;

    type Input = TaggedWarpUplinkProfile;
    type Output = TaggedLaneProfile;

    fn on_receive(
        &mut self,
        tagged_profile: Self::Input,
    ) -> BoxFuture<Result<Option<Self::Output>, ()>> {
        async move {
            // let TaggedWarpUplinkProfile {
            //     path: lane_id,
            //     profile,
            // } = tagged_profile;
            // let lane_uri = &lane_id.lane;
            //
            // match self.pulse_lanes.get(&lane_id) {
            //     Some(lane) => {
            //         let pulse = profile.clone().into();
            //         if let Err(_) = lane.try_send(pulse) {
            //             // todo log err
            //         }
            //     }
            //     None => {
            //         panic!()
            //     }
            // }
            //
            // Some(TaggedLaneProfile { lane_id, profile })
            unimplemented!()
        }
        .boxed()
    }
}

// pub struct LaneSurjection(pub RelativePath);
// impl Surjection<LaneProfile> for LaneSurjection {
//     fn onto(&self, input: LaneProfile) -> ObserverEvent {
//         ObserverEvent::Lane(self.0.clone(), input)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::agent::meta::metric::lane::{LaneProfile, LaneSurjection};
//     use crate::agent::meta::metric::sender::TransformedSender;
//     use crate::agent::meta::metric::ObserverEvent;
//     use futures::FutureExt;
//     use swim_common::warp::path::RelativePath;
//     use tokio::sync::mpsc;
//
//     #[tokio::test]
//     async fn test_lane_surjection() {
//         let path = RelativePath::new("/node", "/lane");
//         let (tx, mut rx) = mpsc::channel(1);
//         let sender = TransformedSender::new(LaneSurjection(path.clone()), tx);
//         let profile = LaneProfile::default();
//
//         assert!(sender.try_send(profile.clone()).is_ok());
//         assert_eq!(
//             rx.recv().now_or_never().unwrap().unwrap(),
//             ObserverEvent::Lane(path, profile)
//         );
//     }
// }
