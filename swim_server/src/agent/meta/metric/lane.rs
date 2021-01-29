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

use crate::agent::meta::metric::sender::{Surjection, TransformedSender};
use crate::agent::meta::metric::ObserverEvent;
use futures::FutureExt;
use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct LaneProfile;

pub struct LaneSurjection(pub RelativePath);
impl Surjection<LaneProfile> for LaneSurjection {
    fn onto(&self, input: LaneProfile) -> ObserverEvent {
        ObserverEvent::Lane(self.0.clone(), input)
    }
}

#[tokio::test]
async fn test_lane_surjection() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(1);
    let sender = TransformedSender::new(LaneSurjection(path.clone()), tx);
    let profile = LaneProfile::default();

    assert!(sender.try_send(profile.clone()).is_ok());
    assert_eq!(
        rx.recv().now_or_never().unwrap().unwrap(),
        ObserverEvent::Lane(path, profile)
    );
}
