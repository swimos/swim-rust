// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::lane::model::supply::make_lane_model;
use futures::StreamExt;
use std::num::NonZeroUsize;

#[tokio::test]
async fn receive_events() {
    let (lane, mut events) = make_lane_model(NonZeroUsize::new(1000).unwrap());

    let jh = tokio::spawn(async move {
        let mut expected = (0..=100).collect::<Vec<i32>>();
        expected.reverse();

        while let Some(event) = events.next().await {
            assert!(expected.contains(&event));
            let actual = expected.pop().unwrap();
            assert_eq!(event, actual);
        }
    });

    let values: Vec<i32> = (0..=100).collect();

    for v in values {
        assert!(lane.send(v).await.is_ok());
    }

    drop(lane);

    assert!(jh.await.is_ok());
}
