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

use crate::agent::lane::channels::update::value::ValueLaneUpdateTask;
use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::value::ValueLane;
use crate::routing::RoutingAddr;
use futures::future::{join, ready};
use futures::stream::once;
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::time::Duration;
use store::ValueDataMemStore;
use store::ValueDataModel;
use tokio::time::timeout;

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

#[tokio::test]
async fn update_task_value_lane() {
    let (store, rx) = ValueDataMemStore::observable(Default::default(), buffer_size());
    let model = ValueDataModel::Mem(store);
    let lane = ValueLane::new(model);

    let mut events = rx.into_stream();

    let task = ValueLaneUpdateTask::new(lane);

    let addr = RoutingAddr::remote(2);
    let value: Result<(RoutingAddr, i32), UpdateError> = Ok((addr, 7));

    let updates = once(ready(value));

    let update_task = task.run_update(updates);
    let receive_task = timeout(Duration::from_secs(10), events.next());

    let (_, rec_result) = join(update_task, receive_task).await;

    assert!(matches!(rec_result, Ok(Some(v)) if *v == 7));
}
