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
use crate::agent::lane::model::map::{MapDataModel, MapStoreEvent};
use crate::agent::lane::StoreIo;
use crate::agent::NodeStore;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use swim_common::form::Form;
use swim_common::model::time::Timestamp;
use swim_store::StoreError;

/// Map lane store IO task.
///
/// This task loads the backing Map lane with a Map from the store if it exists and then
/// stores all of the events into the delegate store. If any errors are produced, the task will fail
/// after the store error handler's maximum allowed error policy has been reached.
///
/// # Type parameters:
/// `K` - map lane key type.
/// `V` - map lane value type.
/// `Events` - map lane event stream.
/// `Store` - delegate node store type.
pub struct MapLaneStoreIo<K, V, Events, Store> {
    events: Events,
    model: MapDataModel<Store, K, V>,
}

impl<K, V, Events, Store> MapLaneStoreIo<K, V, Events, Store> {
    pub fn new(
        events: Events,
        model: MapDataModel<Store, K, V>,
    ) -> MapLaneStoreIo<K, V, Events, Store> {
        MapLaneStoreIo { events, model }
    }
}

impl<Store, Events, K, V> StoreIo for MapLaneStoreIo<K, V, Events, Store>
where
    Store: NodeStore,
    Events: Stream<Item = MapStoreEvent<K, V>> + Unpin + Send + Sync + 'static,
    K: Form + Debug + Send + Sync + Serialize + DeserializeOwned + 'static,
    V: Debug + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    fn attach(
        self,
        mut error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move {
            let MapLaneStoreIo { mut events, model } = self;

            while let Some(event) = events.next().await {
                match event {
                    MapStoreEvent::Clear => on_event(&mut error_handler, || model.clear())?,
                    MapStoreEvent::Update(key, value) => {
                        on_event(&mut error_handler, || model.put(&key, &value))?
                    }
                    MapStoreEvent::Remove(key) => {
                        on_event(&mut error_handler, || model.delete(&key))?
                    }
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

fn on_event<F>(handler: &mut StoreErrorHandler, f: F) -> Result<(), LaneStoreErrorReport>
where
    F: Fn() -> Result<(), StoreError>,
{
    match f() {
        Ok(()) => Ok(()),
        Err(error) => handler.on_error(StoreTaskError {
            timestamp: Timestamp::now(),
            error,
        }),
    }
}
