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

use crate::agent::lane::LaneModel;
use futures::Stream;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct CueRequest<Key>(pub Key);

/// Model for a stateless, lazy, lane that uses its lifecycle to generate a map from keys to values.
#[derive(Debug)]
pub struct DemandMapLane<Key, Value> {
    cue_requests_tx: mpsc::Sender<CueRequest<Key>>,
    sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
    id: Arc<()>,
}

impl<Key, Value> Clone for DemandMapLane<Key, Value> {
    fn clone(&self) -> Self {
        DemandMapLane {
            cue_requests_tx: self.cue_requests_tx.clone(),
            sender: self.sender.clone(),
            id: self.id.clone(),
        }
    }
}

impl<Key, Value> DemandMapLane<Key, Value> {
    pub(crate) fn new(
        sender: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
        buffer_size: NonZeroUsize,
    ) -> (
        DemandMapLane<Key, Value>,
        impl Stream<Item = CueRequest<Key>>,
    ) {
        let (cue_requests_tx, cue_requests_rx) = mpsc::channel(buffer_size.get());

        (
            DemandMapLane {
                sender,
                cue_requests_tx,
                id: Default::default(),
            },
            cue_requests_rx,
        )
    }

    /// Create a new `DemandMapLaneController` that can be used to cue a value.
    pub fn controller(&self) -> DemandMapLaneController<Key, Value> {
        DemandMapLaneController::new(self.sender.clone())
    }
}

/// A controller that can be used to cue a value to an associated `DemandMapLane`.
///
/// # Type Parameters
///
/// * `T` - The type of the events produced by the `DemandMapLane`.
pub struct DemandMapLaneController<Key, Value> {
    tx: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
}
impl<Key, Value> Clone for DemandMapLaneController<Key, Value> {
    fn clone(&self) -> Self {
        DemandMapLaneController {
            tx: self.tx.clone(),
        }
    }
}

impl<Key, Value> DemandMapLaneController<Key, Value> {
    fn new(
        tx: mpsc::Sender<DemandMapLaneEvent<Key, Value>>,
    ) -> DemandMapLaneController<Key, Value> {
        DemandMapLaneController { tx }
    }

    pub async fn sync(&mut self) -> oneshot::Receiver<Vec<Value>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let DemandMapLaneController { tx, .. } = self;

        if tx.send(DemandMapLaneEvent::sync(resp_tx)).await.is_err() {
            todo!("Logging")
        }

        resp_rx
    }

    pub async fn sync_and_await(&mut self) -> Result<Vec<Value>, ()> {
        self.sync().await.await.map_err(|_| ())
    }

    pub async fn cue_and_await(&mut self, key: Key) -> Result<Option<Value>, ()> {
        self.cue(key).await.await.map_err(|_| ())
    }

    pub async fn cue(&mut self, key: Key) -> oneshot::Receiver<Option<Value>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let DemandMapLaneController { tx, .. } = self;

        if tx
            .send(DemandMapLaneEvent::cue(key, resp_tx))
            .await
            .is_err()
        {
            todo!("Logging")
        }

        resp_rx
    }
}

#[derive(Debug)]
pub enum DemandMapLaneEvent<Key, Value> {
    Sync(oneshot::Sender<Vec<Value>>),
    Cue(oneshot::Sender<Option<Value>>, Key),
}

impl<Key, Value> DemandMapLaneEvent<Key, Value> {
    fn sync(tx: oneshot::Sender<Vec<Value>>) -> DemandMapLaneEvent<Key, Value> {
        DemandMapLaneEvent::Sync(tx)
    }

    fn cue(key: Key, tx: oneshot::Sender<Option<Value>>) -> DemandMapLaneEvent<Key, Value> {
        DemandMapLaneEvent::Cue(tx, key)
    }
}

impl<Key, Value> LaneModel for DemandMapLane<Key, Value> {
    type Event = Key;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

/// Create a new demand lane model. Returns a new demand lane model and a stream of unit values that
/// represent a cue request.
pub fn make_lane_model<Key, Value>(
    buffer_size: NonZeroUsize,
) -> (
    DemandMapLane<Key, Value>,
    impl Stream<Item = DemandMapLaneEvent<Key, Value>> + Send + 'static,
    impl Stream<Item = CueRequest<Key>>,
)
where
    Key: Send + Sync + 'static,
    Value: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let (lane, cue_requests_rx) = DemandMapLane::new(tx, buffer_size);
    (lane, rx, cue_requests_rx)
}
