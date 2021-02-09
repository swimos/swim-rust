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

use crate::agent::lane::channels::update::map::MapLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapSubscriber};
use crate::agent::lane::model::DeferredSubscription;
use crate::agent::lane::tests::ExactlyOnce;
use crate::routing::RoutingAddr;
use futures::future::{join, ready};
use futures::stream::once;
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_common::form::Form;
use swim_warp::model::map::MapUpdate;
use tokio::time::timeout;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn make_subscribable<K, V>(buffer_size: NonZeroUsize) -> (MapLane<K, V>, MapSubscriber<K, V>)
where
    K: Form + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let (lane, rx) = MapLane::observable(buffer_size);
    (lane, MapSubscriber::new(rx.into_subscriber()))
}

#[tokio::test]
async fn update_task_map_lane_update() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());

    let mut events = sub.subscribe().unwrap();

    let task = MapLaneUpdateTask::new(lane, || ExactlyOnce);

    let addr = RoutingAddr::remote(2);
    let upd: Result<(RoutingAddr, MapUpdate<i32, i32>), UpdateError> =
        Ok((addr, MapUpdate::Update(4, Arc::new(7))));

    let updates = once(ready(upd));

    let update_task = task.run_update(updates);
    let receive_task = timeout(Duration::from_secs(10), events.next());

    let (upd_result, rec_result) = join(update_task, receive_task).await;

    assert!(matches!(upd_result, Ok(())));
    assert!(matches!(rec_result, Ok(Some(MapLaneEvent::Update(4, v))) if *v == 7));
}

#[tokio::test]
async fn update_task_map_lane_remove() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());

    let mut events = sub.subscribe().unwrap();

    //Insert a record to remove and consume the generated event.
    assert!(lane
        .update_direct(4, Arc::new(7))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!(events.next().await.is_some());

    let task = MapLaneUpdateTask::new(lane, || ExactlyOnce);
    let addr = RoutingAddr::remote(2);
    let rem: Result<(RoutingAddr, MapUpdate<i32, i32>), UpdateError> =
        Ok((addr, MapUpdate::Remove(4)));

    let updates = once(ready(rem));

    let update_task = task.run_update(updates);
    let receive_task = timeout(Duration::from_secs(10), events.next());

    let (upd_result, rec_result) = join(update_task, receive_task).await;

    assert!(matches!(upd_result, Ok(())));
    assert!(matches!(rec_result, Ok(Some(MapLaneEvent::Remove(4)))));
}

#[tokio::test]
async fn update_task_map_lane_clear() {
    let (lane, sub) = make_subscribable::<i32, i32>(buffer_size());

    let mut events = sub.subscribe().unwrap();

    //Insert a record to remove and consume the generated event.
    assert!(lane
        .update_direct(4, Arc::new(7))
        .apply(ExactlyOnce)
        .await
        .is_ok());
    assert!(events.next().await.is_some());

    let task = MapLaneUpdateTask::new(lane, || ExactlyOnce);
    let addr = RoutingAddr::remote(2);
    let clear: Result<(RoutingAddr, MapUpdate<i32, i32>), UpdateError> =
        Ok((addr, MapUpdate::Clear));

    let updates = once(ready(clear));

    let update_task = task.run_update(updates);
    let receive_task = timeout(Duration::from_secs(10), events.next());

    let (upd_result, rec_result) = join(update_task, receive_task).await;

    assert!(matches!(upd_result, Ok(())));
    assert!(matches!(rec_result, Ok(Some(MapLaneEvent::Clear))));
}
