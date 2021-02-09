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

use crate::agent::lane::LaneModel;
use futures::Stream;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Model for a stateless, lazy, lane that uses its lifecycle to generate a value.
///
/// # Type Parameters
///
/// * `T` - The type of the events produced.
#[derive(Debug)]
pub struct DemandLane<Event> {
    sender: mpsc::Sender<()>,
    id: Arc<()>,
    _pd: PhantomData<Event>,
}

impl<Event> Clone for DemandLane<Event> {
    fn clone(&self) -> Self {
        DemandLane {
            sender: self.sender.clone(),
            id: self.id.clone(),
            _pd: Default::default(),
        }
    }
}

impl<Event> DemandLane<Event> {
    pub(crate) fn new(sender: mpsc::Sender<()>) -> DemandLane<Event> {
        DemandLane {
            sender,
            id: Default::default(),
            _pd: Default::default(),
        }
    }

    /// Create a new `DemandLaneController` that can be used to cue a value.
    pub fn controller(&self) -> DemandLaneController<Event> {
        DemandLaneController::new(self.sender.clone())
    }
}

/// A controller that can be used to cue a value to an associated `DemandLane`.
///
/// # Type Parameters
///
/// * `T` - The type of the events produced by the `DemandLane`.
pub struct DemandLaneController<Event> {
    tx: mpsc::Sender<()>,
    _pd: PhantomData<Event>,
}

impl<Event> DemandLaneController<Event> {
    fn new(tx: mpsc::Sender<()>) -> DemandLaneController<Event> {
        DemandLaneController {
            tx,
            _pd: Default::default(),
        }
    }

    /// Cue a value to the `DemandLane`. Returns whether or not the operation was successful.
    pub async fn cue(&mut self) -> bool {
        self.tx.send(()).await.is_ok()
    }
}

impl<Event> LaneModel for DemandLane<Event> {
    type Event = ();

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

/// Create a new demand lane model. Returns a new demand lane model and a stream of unit values that
/// represent a cue request.
pub fn make_lane_model<Event>(
    buffer_size: NonZeroUsize,
) -> (DemandLane<Event>, impl Stream<Item = ()> + Send + 'static)
where
    Event: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = DemandLane::new(tx);
    (lane, rx)
}
