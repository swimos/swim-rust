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

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::meta::metric::aggregator::{AddressedMetric, MetricStage};
use crate::meta::metric::uplink::TaggedWarpUplinkProfile;
use crate::meta::metric::{MetricKind, WarpUplinkProfile};

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LaneProfile;

impl LaneProfile {
    pub fn accumulate(&mut self, _profile: &WarpUplinkProfile) {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct TaggedLaneProfile {
    pub path: RelativePath,
    pub profile: LaneProfile,
}

impl AddressedMetric for TaggedLaneProfile {
    type Payload = LaneProfile;

    fn split(self) -> (RelativePath, Self::Payload) {
        let TaggedLaneProfile { path, profile } = self;
        (path, profile)
    }

    fn tag_ref(&self) -> &RelativePath {
        &self.path
    }

    fn tag(payload: Self::Payload, path: RelativePath) -> Self {
        TaggedLaneProfile {
            path,
            profile: payload,
        }
    }
}

pub struct LaneStage;
impl MetricStage for LaneStage {
    const METRIC_KIND: MetricKind = MetricKind::Lane;

    type ProfileIn = TaggedWarpUplinkProfile;
    type ProfileOut = TaggedLaneProfile;
    type Pulse = LanePulse;

    fn aggregate(_old: LaneProfile, _new: WarpUplinkProfile) -> LaneProfile {
        unimplemented!()
    }
}

impl From<LaneProfile> for LanePulse {
    fn from(_: LaneProfile) -> Self {
        unimplemented!()
    }
}

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LanePulse;

#[derive(Clone, PartialEq, Debug)]
pub struct TaggedLanePulse {
    path: RelativePath,
    pulse: LanePulse,
}

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
