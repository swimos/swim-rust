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

use crate::agent::lane::model::demand_map::{
    make_lane_model, DemandMapLaneCommand, DemandMapLaneEvent,
};
use futures::future::{join, join3};
use std::num::NonZeroUsize;
use swim_common::form::Form;
use swim_common::record;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_sync() {
    let (tx, mut rx) = mpsc::channel(5);
    let (lane, _events) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap(), tx);
    let controller = lane.controller();
    let sync = controller.sync();

    let asserter = async move {
        match rx.recv().await {
            Some(DemandMapLaneCommand::Sync(sender)) => {
                assert!(sender.send(vec![DemandMapLaneEvent::update(5, 10)]).is_ok());
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
        match rx.recv().await {
            Some(DemandMapLaneCommand::Cue(sender, _key)) => {
                assert!(sender.send(Some(5)).is_ok());
            }
            _ => panic!("Unexpected event"),
        }
    };

    let event_task = async move {
        match update_rx.recv().await {
            Some(value) => {
                assert_eq!(value, DemandMapLaneEvent::update(10, 5));
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
        match rx.recv().await {
            Some(DemandMapLaneCommand::Cue(sender, _key)) => {
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

#[tokio::test]
async fn test_remove() {
    let (tx, mut rx) = mpsc::channel(5);
    let (lane, _topic) = make_lane_model::<i32, i32>(NonZeroUsize::new(5).unwrap(), tx);

    let remove_task = async move {
        match rx.recv().await {
            Some(DemandMapLaneCommand::Remove(key)) => {
                assert_eq!(key, 1);
            }
            _ => panic!("Unexpected event"),
        }
    };

    let mut controller = lane.controller();
    let remove_future = controller.remove(1);
    let (_, remove_result) = join(remove_task, remove_future).await;

    assert!(remove_result.is_ok());
}

#[test]
fn test_form() {
    let remove_event: DemandMapLaneEvent<i32, i32> = DemandMapLaneEvent::remove(1);
    assert_eq!(
        remove_event.as_value(),
        record!(
            attrs => [("remove", record!(("key", 1)))]
        )
    );

    let update_event = DemandMapLaneEvent::update(1, 2);
    assert_eq!(
        update_event.as_value(),
        record!(
            attrs => [("update", record!(("key", 1)))],
            items => [2]
        )
    );
}
