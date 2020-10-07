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
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct DemandLane<Value> {
    sender: mpsc::Sender<()>,
    id: Arc<()>,
    _pd: PhantomData<Value>,
}

impl<Value> Clone for DemandLane<Value> {
    fn clone(&self) -> Self {
        DemandLane {
            sender: self.sender.clone(),
            id: self.id.clone(),
            _pd: Default::default(),
        }
    }
}

impl<Value> DemandLane<Value> {
    pub(crate) fn new(sender: mpsc::Sender<()>) -> DemandLane<Value> {
        DemandLane {
            sender,
            id: Default::default(),
            _pd: Default::default(),
        }
    }

    pub async fn cue(&mut self) -> bool {
        self.sender.send(()).await.is_ok()
    }
}

impl<Value> LaneModel for DemandLane<Value> {
    type Event = ();

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

pub fn make_lane_model<Value>(
    buffer_size: NonZeroUsize,
) -> (DemandLane<Value>, impl Stream<Item = ()> + Send + 'static)
where
    Value: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = DemandLane::new(tx);
    (lane, rx)
}
