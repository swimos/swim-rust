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

use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler};
use crate::agent::lane::store::StoreIo;
use crate::agent::model::value::value_store::ValueDataModel;
use crate::agent::model::value::ValueLane;
use crate::agent::store::NodeStore;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

/// Value lane store IO task.
///
/// This task loads the backing value lane with a value from the store if it exists and then
/// stores all of the events into the delegate store. If any errors are produced, the task will fail
/// after the store error handler's maximum allowed error policy has been reached.
///
/// # Type parameters:
/// `T` - the type of the value lane.
/// `Events` - events produced by the lane.
pub struct ValueLaneStoreIo<Store, T, Events> {
    store: Store,
    lane: ValueLane<T>,
    events: Events,
}

impl<Store, T, Events> ValueLaneStoreIo<Store, T, Events> {
    pub fn new(
        store: Store,
        lane: ValueLane<T>,
        events: Events,
    ) -> ValueLaneStoreIo<Store, T, Events> {
        ValueLaneStoreIo {
            store,
            lane,
            events,
        }
    }
}

impl<Store, Events, T> StoreIo for ValueLaneStoreIo<Store, T, Events>
where
    Store: NodeStore,
    Events: Stream<Item = Arc<T>> + Unpin + Send + Sync + 'static,
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    fn attach(
        self,
        lane_uri: String,
        mut error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move {
            let ValueLaneStoreIo {
                store,
                lane,
                mut events,
            } = self;
            let lane_id = store.lane_id_of(lane_uri).await;
            let model = ValueDataModel::new(store, lane_id);

            match model.load() {
                Ok(Some(value)) => lane.store(value).await,
                Ok(None) => {}
                Err(e) => {
                    return Err(LaneStoreErrorReport::for_error(model.store_info(), e));
                }
            };

            while let Some(event) = events.next().await {
                match model.store(&event) {
                    Ok(()) => continue,
                    Err(e) => error_handler.on_error(e)?,
                }
            }
            Ok(())
        })
    }

    fn attach_boxed(
        self: Box<Self>,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(lane_uri, error_handler)
    }
}
