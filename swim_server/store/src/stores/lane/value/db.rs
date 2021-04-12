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

use crate::stores::lane::observer::StoreObserver;
use crate::stores::lane::{serialize_then, LaneKey};
use crate::stores::node::SwimNodeStore;
use crate::{deserialize, StoreEngine, StoreError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use utilities::sync::topic;

#[derive(Debug)]
pub struct ValueDataDbStore<V> {
    store: SwimNodeStore,
    lane_uri: Arc<String>,
    observer: Option<Arc<Mutex<topic::Sender<Arc<V>>>>>,
    default_value: Arc<V>,
}

impl<V> PartialEq for ValueDataDbStore<V> {
    fn eq(&self, other: &Self) -> bool {
        self.lane_uri.eq(&other.lane_uri)
    }
}

impl<V> Clone for ValueDataDbStore<V> {
    fn clone(&self) -> Self {
        ValueDataDbStore {
            store: self.store.clone(),
            lane_uri: self.lane_uri.clone(),
            observer: self.observer.clone(),
            default_value: self.default_value.clone(),
        }
    }
}

impl<V> ValueDataDbStore<V>
where
    V: Any + Send + Sync,
{
    pub fn new(store: SwimNodeStore, lane_uri: String, default_value: V) -> ValueDataDbStore<V> {
        ValueDataDbStore {
            store,
            lane_uri: Arc::new(lane_uri),
            observer: None,
            default_value: Arc::new(default_value),
        }
    }

    pub fn observable(
        store: SwimNodeStore,
        lane_uri: String,
        buffer_size: NonZeroUsize,
        default_value: V,
    ) -> (ValueDataDbStore<V>, StoreObserver<V>) {
        let (tx, rx) = topic::channel(buffer_size);
        let store = ValueDataDbStore {
            store,
            lane_uri: Arc::new(lane_uri),
            observer: Some(Arc::new(Mutex::new(tx))),
            default_value: Arc::new(default_value),
        };
        (store, StoreObserver::Db(rx))
    }

    fn key(&self) -> LaneKey {
        LaneKey::Value {
            lane_uri: self.lane_uri.clone(),
        }
    }

    async fn send(&self, value: V) -> Result<(), StoreError> {
        match &self.observer {
            Some(observer) => {
                let mut channel = observer.lock().await;
                channel
                    .send(Arc::new(value))
                    .await
                    .map_err(|_| StoreError::ObserverClosed)
            }
            None => Ok(()),
        }
    }
}

impl<V> ValueDataDbStore<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn store(&self, value: V) -> Result<(), StoreError> {
        serialize_then(&self.store, &value, |delegate, bytes| {
            delegate.put(self.key(), bytes)
        })?;

        self.send(value).await
    }

    pub fn load(&self) -> Result<Arc<V>, StoreError> {
        match self.store.get(self.key()) {
            Ok(Some(bytes)) => deserialize(bytes.as_slice()),
            Ok(None) => {
                let value = self.default_value.clone();
                serialize_then(&self.store, &value, |delegate, bytes| {
                    delegate.put(self.key(), bytes)
                })?;
                Ok(value)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_for_update(&self, _op: impl Fn(Arc<V>) -> V + Sync) -> Result<(), StoreError> {
        unimplemented!()
    }
}
