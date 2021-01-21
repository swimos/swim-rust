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

use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::agent::lane::LaneModel;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::topic::MpscTopic;

#[cfg(test)]
mod tests;

#[derive(Form, Debug, Clone, PartialEq)]
#[form(tag = "update")]
pub struct DemandMapLaneUpdate<Key, Value>(
    #[form(header, name = "key")] Key,
    #[form(body)] Arc<Value>,
)
where
    Key: Form,
    Value: Form;

impl<K, V> From<DemandMapLaneUpdate<K, V>> for Value
where
    K: Form,
    V: Form,
{
    fn from(update: DemandMapLaneUpdate<K, V>) -> Self {
        update.into_value()
    }
}

impl<Key, Value> DemandMapLaneUpdate<Key, Value>
where
    Key: Form,
    Value: Form,
{
    pub fn make(key: Key, value: Value) -> DemandMapLaneUpdate<Key, Value> {
        DemandMapLaneUpdate(key, Arc::new(value))
    }

    #[cfg(test)]
    pub(crate) fn key(&self) -> &Key {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn value(&self) -> &Value {
        &self.1
    }
}

pub enum DemandMapLaneEvent<Key, Value>
where
    Key: Form,
    Value: Form,
{
    Sync(oneshot::Sender<Vec<DemandMapLaneUpdate<Key, Value>>>),
    Cue(oneshot::Sender<Option<Value>>, Key),
}

/// A controller for a demand map lane that may be used to cue a value by its key.
pub struct DemandMapLaneController<Key, Value>(DemandMapLane<Key, Value>)
where
    Key: Form,
    Value: Form;

impl<Key, Value> DemandMapLaneController<Key, Value>
where
    Key: Clone + Form,
    Value: Form,
{
    /// Syncs this lane. Called by the uplink.
    pub(crate) async fn sync(self) -> Result<Vec<DemandMapLaneUpdate<Key, Value>>, ()> {
        let (tx, rx) = oneshot::channel();

        if self
            .0
            .lifecycle_sender
            .send(DemandMapLaneEvent::Sync(tx))
            .await
            .is_err()
        {
            return Err(());
        }

        rx.await.map_err(|_| ())
    }

    /// Cues a value. Returns `Ok(true)` if the key mapped successfully to a value or `Ok(false)`
    /// if it was not. Returns `Err(())` if an error occurred.
    pub async fn cue(&mut self, key: Key) -> Result<bool, ()> {
        let (tx, rx) = oneshot::channel();
        if self
            .0
            .lifecycle_sender
            .send(DemandMapLaneEvent::Cue(tx, key.clone()))
            .await
            .is_err()
        {
            return Err(());
        }

        match rx.await {
            Ok(Some(value)) => {
                let _ = self
                    .0
                    .uplink_sender
                    .send(DemandMapLaneUpdate::make(key, value))
                    .await;
                Ok(true)
            }
            Ok(None) => Ok(false),
            _ => Err(()),
        }
    }
}

/// A model for a map lane that has no state and fetches its values from the lifecycle
/// implementation.
#[derive(Debug)]
pub struct DemandMapLane<Key, Value>
where
    Key: Form,
    Value: Form,
{
    uplink_sender: mpsc::Sender<DemandMapLaneUpdate<Key, Value>>,
    lifecycle_sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
    id: Arc<()>,
}

impl<Key, Value> Clone for DemandMapLane<Key, Value>
where
    Key: Form,
    Value: Form,
{
    fn clone(&self) -> Self {
        DemandMapLane {
            uplink_sender: self.uplink_sender.clone(),
            lifecycle_sender: self.lifecycle_sender.clone(),
            id: self.id.clone(),
        }
    }
}

impl<Key, Value> DemandMapLane<Key, Value>
where
    Key: Clone + Form,
    Value: Clone + Form,
{
    pub(crate) fn new(
        uplink_sender: mpsc::Sender<DemandMapLaneUpdate<Key, Value>>,
        lifecycle_sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
    ) -> DemandMapLane<Key, Value> {
        DemandMapLane {
            uplink_sender,
            lifecycle_sender,
            id: Default::default(),
        }
    }

    pub fn controller(&self) -> DemandMapLaneController<Key, Value> {
        DemandMapLaneController(self.clone())
    }
}

impl<Key, Value> LaneModel for DemandMapLane<Key, Value>
where
    Key: Form,
    Value: Form,
{
    type Event = Key;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

/// Create a new demand map lane model. Returns a demand map lane instance and a topic containing a
/// stream of cued values.
///
/// # Arguments
/// `buffer_size`: the size of the topic's buffer.
/// `lifecycle_sender`: a sender to the `DemandMapLaneLifecycle`.
pub fn make_lane_model<Key, Value>(
    buffer_size: NonZeroUsize,
    lifecycle_sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
) -> (
    DemandMapLane<Key, Value>,
    MpscTopic<DemandMapLaneUpdate<Key, Value>>,
)
where
    Key: Send + Clone + Form + Sync + 'static,
    Value: Send + Clone + Form + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let (topic, _rec) = MpscTopic::new(rx, buffer_size, buffer_size);
    let lane = DemandMapLane::new(tx, lifecycle_sender);

    (lane, topic)
}
