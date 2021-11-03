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

use crate::agent::lane::error::{LaneStoreErrorReport, StoreErrorHandler, StoreTaskError};
use crate::agent::lane::model::value::ValueDataModel;
use crate::agent::lane::StoreIo;
use crate::agent::NodeStore;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use swim_model::time::Timestamp;

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
    events: Events,
    model: ValueDataModel<Store, T>,
}

impl<Store, T, Events> ValueLaneStoreIo<Store, T, Events> {
    pub fn new(
        events: Events,
        model: ValueDataModel<Store, T>,
    ) -> ValueLaneStoreIo<Store, T, Events> {
        ValueLaneStoreIo { events, model }
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
        mut error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move {
            let ValueLaneStoreIo { mut events, model } = self;

            while let Some(event) = events.next().await {
                match model.store(&event) {
                    Ok(()) => continue,
                    Err(error) => error_handler.on_error(StoreTaskError {
                        timestamp: Timestamp::now(),
                        error,
                    })?,
                }
            }
            Ok(())
        })
    }

    fn attach_boxed(
        self: Box<Self>,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(error_handler)
    }
}
