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

use crate::agent::lane::model::demand_map::{
    make_lane_model, DemandMapLaneEvent, DemandMapLaneUpdate,
};
use futures::future::{join, join3};
use futures::StreamExt;
use pin_utils::core_reexport::num::NonZeroUsize;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_sync() {
    let (tx, mut rx) = mpsc::channel(5);
    let (lane, _events) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap(), tx);
    let controller = lane.controller();
    let sync = controller.sync();

    let asserter = async move {
        match rx.next().await {
            Some(DemandMapLaneEvent::Sync(sender)) => {
                assert!(sender.send(vec![DemandMapLaneUpdate::make(5, 10)]).is_ok());
            }
            _ => panic!("Unexpected event"),
        }
    };

    let (result, _) = join(sync, asserter).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[tokio::test]
async fn test_cue_ok() {
    let (tx, mut rx) = mpsc::channel(5);
    let (lane, mut update_rx) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap(), tx);

    let cue_task = async move {
        match rx.next().await {
            Some(DemandMapLaneEvent::Cue(sender, _key)) => {
                assert!(sender.send(Some(5)).is_ok());
            }
            _ => panic!("Unexpected event"),
        }
    };

    let event_task = async move {
        match update_rx.next().await {
            Some(value) => {
                assert_eq!(value, DemandMapLaneUpdate::make(10, 5));
            }
            _ => {
                panic!("Expected a value");
            }
        }
    };

    let mut controller = lane.controller();
    let cue_future = controller.cue(10);
    let (_, cue_result, _) = join3(cue_task, cue_future, event_task).await;

    assert!(cue_result.is_ok());
}

#[tokio::test]
async fn test_cue_none() {
    let (tx, mut rx) = mpsc::channel(5);
    let (lane, _topic) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap(), tx);

    let cue_task = async move {
        match rx.next().await {
            Some(DemandMapLaneEvent::Cue(sender, _key)) => {
                assert!(sender.send(None).is_ok());
            }
            _ => panic!("Unexpected event"),
        }
    };

    let mut controller = lane.controller();
    let cue_future = controller.cue(10);
    let (_, cue_result) = join(cue_task, cue_future).await;

    assert!(cue_result.is_ok());
}
