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

use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use swim_form::Form;
use swim_model::Value;

use crate::agent::lane::LaneModel;

#[cfg(test)]
mod tests;

#[derive(Form, Debug, Clone, PartialEq)]
pub enum DemandMapLaneEvent<Key, Value>
where
    Key: Form,
    Value: Form,
{
    #[form(tag = "remove")]
    Remove(#[form(header, name = "key")] Key),
    #[form(tag = "update")]
    Update(#[form(header, name = "key")] Key, #[form(body)] Arc<Value>),
}

impl<K, V> From<DemandMapLaneEvent<K, V>> for Value
where
    K: Form,
    V: Form,
{
    fn from(update: DemandMapLaneEvent<K, V>) -> Self {
        update.into_value()
    }
}

impl<Key, Value> DemandMapLaneEvent<Key, Value>
where
    Key: Form,
    Value: Form,
{
    pub fn update(key: Key, value: Value) -> DemandMapLaneEvent<Key, Value> {
        DemandMapLaneEvent::Update(key, Arc::new(value))
    }

    pub fn remove(key: Key) -> DemandMapLaneEvent<Key, Value> {
        DemandMapLaneEvent::Remove(key)
    }
}

pub enum DemandMapLaneCommand<Key, Value>
where
    Key: Form,
    Value: Form,
{
    Sync(oneshot::Sender<Vec<DemandMapLaneEvent<Key, Value>>>),
    Cue(oneshot::Sender<Option<Value>>, Key),
    Remove(Key),
}

/// A controller for a demand map lane that may be used to cue a value by its key.
pub struct DemandMapLaneController<Key, Value>(DemandMapLane<Key, Value>)
where
    Key: Form,
    Value: Form;

#[derive(Debug)]
pub struct ControllerError;

impl Display for ControllerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "The controller lifecycle stopped responding.")
    }
}

impl std::error::Error for ControllerError {}

impl<Key, Value> DemandMapLaneController<Key, Value>
where
    Key: Clone + Form,
    Value: Form,
{
    /// Syncs this lane. Called by the uplink.
    pub(crate) async fn sync(self) -> Result<Vec<DemandMapLaneEvent<Key, Value>>, ControllerError> {
        let (tx, rx) = oneshot::channel();

        if self
            .0
            .lifecycle_sender
            .send(DemandMapLaneCommand::Sync(tx))
            .await
            .is_err()
        {
            return Err(ControllerError);
        }

        rx.await.map_err(|_| ControllerError)
    }

    /// Cues a value. Returns `Ok(true)` if the key mapped successfully to a value or `Ok(false)`
    /// if it was not. Returns `Err(())` if an error occurred.
    pub async fn cue(&mut self, key: Key) -> Result<bool, ()> {
        let (tx, rx) = oneshot::channel();
        if self
            .0
            .lifecycle_sender
            .send(DemandMapLaneCommand::Cue(tx, key.clone()))
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
                    .send(DemandMapLaneEvent::update(key, value))
                    .await;
                Ok(true)
            }
            Ok(None) => Ok(false),
            _ => Err(()),
        }
    }

    /// Removes a value from the map.
    pub async fn remove(&mut self, key: Key) -> Result<(), ControllerError> {
        if self
            .0
            .lifecycle_sender
            .send(DemandMapLaneCommand::Remove(key.clone()))
            .await
            .is_err()
        {
            return Err(ControllerError);
        }

        self.0
            .uplink_sender
            .send(DemandMapLaneEvent::remove(key))
            .await
            .map_err(|_| ControllerError)
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
    uplink_sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
    lifecycle_sender: mpsc::Sender<DemandMapLaneCommand<Key, Value>>,
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
        uplink_sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
        lifecycle_sender: mpsc::Sender<DemandMapLaneCommand<Key, Value>>,
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
    lifecycle_sender: mpsc::Sender<DemandMapLaneCommand<Key, Value>>,
) -> (
    DemandMapLane<Key, Value>,
    mpsc::Receiver<DemandMapLaneEvent<Key, Value>>,
)
where
    Key: Send + Clone + Form + Sync + 'static,
    Value: Send + Clone + Form + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = DemandMapLane::new(tx, lifecycle_sender);
    (lane, rx)
}
