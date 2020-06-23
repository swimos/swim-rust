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

use crate::agent::lane::strategy::{Buffered, ChannelObserver, Dropping, Queue};
use crate::agent::lane::BroadcastStream;
use futures::Stream;
use std::any::Any;
use std::sync::Arc;
use stm::stm::Stm;
use stm::var::observer::StaticObserver;
use stm::var::TVar;
use tokio::sync::{broadcast, mpsc, watch};

/// A lane containing a single value.
pub struct ValueLane<T> {
    value: TVar<T>,
}

/// Create a new value lane with the specified watch strategy.
pub fn make_lane<T, W>(init: T, watch: W) -> (ValueLane<T>, W::View)
where
    T: Any + Send + Sync,
    W: ValueLaneWatch<T>,
{
    let (observer, view) = watch.make_watch();
    let var = TVar::new_with_observer(init, observer);
    let lane = ValueLane { value: var };
    (lane, view)
}

impl<T: Any + Send + Sync> ValueLane<T> {
    /// Get the current value.
    pub fn get(&self) -> impl Stm<Result = Arc<T>> {
        self.value.get()
    }

    /// Update the current value.
    pub fn set(&self, value: T) -> impl Stm<Result = ()> {
        self.value.put(value)
    }
}

/// Adapts a watch strategy for use with a [`ValueLane`].
pub trait ValueLaneWatch<T> {
    /// The type of the observer to watch the value of the lane.
    type Obs: StaticObserver<Arc<T>> + Send + Sync + 'static;
    /// The type of the stream of values produced by the lane.
    type View: Stream<Item = Arc<T>> + Send + Sync + 'static;

    /// Create a linked observer and view stream.
    fn make_watch(&self) -> (Self::Obs, Self::View);
}

impl<T> ValueLaneWatch<T> for Queue
where
    T: Any + Send + Sync,
{
    type Obs = ChannelObserver<mpsc::Sender<Arc<T>>>;
    type View = mpsc::Receiver<Arc<T>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let Queue(n) = self;
        let (tx, rx) = mpsc::channel(n.get());
        let observer = ChannelObserver::new(tx);
        (observer, rx)
    }
}

impl<T> ValueLaneWatch<T> for Dropping
where
    T: Any + Default + Send + Sync,
{
    type Obs = ChannelObserver<watch::Sender<Arc<T>>>;
    type View = watch::Receiver<Arc<T>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let (tx, rx) = watch::channel(Default::default());
        let observer = ChannelObserver::new(tx);
        (observer, rx)
    }
}

impl<T> ValueLaneWatch<T> for Buffered
where
    T: Any + Default + Send + Sync,
{
    type Obs = ChannelObserver<broadcast::Sender<Arc<T>>>;
    type View = BroadcastStream<Arc<T>>;

    fn make_watch(&self) -> (Self::Obs, Self::View) {
        let Buffered(n) = self;
        let (tx, rx) = broadcast::channel(n.get());
        let observer = ChannelObserver::new(tx);
        (observer, BroadcastStream(rx))
    }
}
