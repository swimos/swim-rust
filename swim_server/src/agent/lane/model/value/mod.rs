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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::{
    DeferredBroadcastView, DeferredLaneView, DeferredMpscView, DeferredWatchView,
};
use crate::agent::lane::strategy::{
    Buffered, ChannelObserver, DeferredChannelObserver, Dropping, Queue,
};
use crate::agent::lane::{BroadcastStream, LaneModel};
use futures::Stream;
use std::any::Any;
use std::sync::Arc;
use stm::stm::Stm;
use stm::var::observer::{self, JoinObserver, StaticObserver};
use stm::var::TVar;
use swim_common::topic::BroadcastSender;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

#[cfg(test)]
mod tests;

/// A lane containing a single value.
#[derive(Debug)]
pub struct ValueLane<T> {
    value: TVar<T>,
}

impl<T> Clone for ValueLane<T> {
    fn clone(&self) -> Self {
        ValueLane {
            value: self.value.clone(),
        }
    }
}

impl<T: Any + Send + Sync> ValueLane<T> {
    pub fn new(init: T) -> Self {
        ValueLane {
            value: TVar::new(init),
        }
    }
}

impl<T> LaneModel for ValueLane<T>
where
    T: Send + Sync + 'static,
{
    type Event = Arc<T>;

    fn same_lane(this: &Self, other: &Self) -> bool {
        TVar::same_var(&this.value, &other.value)
    }
}

/// Create a new value lane with the specified watch strategy.
pub fn make_lane_model<T, W>(init: T, watch: W) -> (ValueLane<T>, W::View)
where
    T: Any + Send + Sync + Default,
    W: ValueLaneWatch<T>,
{
    let value = Arc::new(init);
    let (observer, view) = watch.make_watch(&value);
    let var = TVar::from_arc_with_observer(value, observer);
    let lane = ValueLane { value: var };
    (lane, view)
}

/// Create a new value lane with the specified watch strategy, attaching an additional, deferred
/// observer channel.
pub fn make_lane_model_deferred<T, W>(
    init: T,
    watch: W,
    config: &AgentExecutionConfig,
) -> (ValueLane<T>, W::View, W::DeferredView)
where
    T: Any + Send + Sync,
    W: ValueLaneWatch<T>,
{
    let value = Arc::new(init);
    let (observer, view, deferred) = watch.make_watch_with_deferred(&value, config);
    let var = TVar::from_arc_with_observer(value, observer);
    let lane = ValueLane { value: var };
    (lane, view, deferred)
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

    /// Get the current value, outside of a transaction.
    pub async fn load(&self) -> Arc<T> {
        self.value.load().await
    }

    /// Store a value to the lane, outside of a transaction.
    pub async fn store(&self, value: T) {
        self.value.store(value).await;
    }

    /// Locks the variable, preventing it from being read from or written to. This is
    /// required to force the ordering of events in some unit tests.
    #[cfg(test)]
    pub async fn lock(&self) -> stm::var::TVarLock {
        self.value.lock().await
    }
}

/// Adapts a watch strategy for use with a [`ValueLane`].
pub trait ValueLaneWatch<T> {
    /// The type of the observer to watch the value of the lane.
    type Obs: StaticObserver<Arc<T>> + Send + Sync + 'static;

    /// The type of the observer to watch the value of the lane with an additional, deferred
    /// observer.
    type WithDefObs: StaticObserver<Arc<T>> + Send + Sync + 'static;

    /// The type of the stream of values produced by the lane.
    type View: Stream<Item = Arc<T>> + Send + Sync + 'static;

    type DeferredView: DeferredLaneView<Arc<T>> + Send + Sync + 'static;

    /// Create a linked observer and view stream.
    fn make_watch(&self, init: &Arc<T>) -> (Self::Obs, Self::View);

    fn make_watch_with_deferred(
        &self,
        init: &Arc<T>,
        config: &AgentExecutionConfig,
    ) -> (Self::WithDefObs, Self::View, Self::DeferredView);
}

type MpscArcSender<T> = mpsc::Sender<Arc<T>>;

impl<T> ValueLaneWatch<T> for Queue
where
    T: Any + Send + Sync,
{
    type Obs = ChannelObserver<mpsc::Sender<Arc<T>>>;
    type WithDefObs =
        JoinObserver<ChannelObserver<MpscArcSender<T>>, DeferredChannelObserver<MpscArcSender<T>>>;
    type View = mpsc::Receiver<Arc<T>>;
    type DeferredView = DeferredMpscView<Arc<T>>;

    fn make_watch(&self, _init: &Arc<T>) -> (Self::Obs, Self::View) {
        let Queue(n) = self;
        let (tx, rx) = mpsc::channel(n.get());
        let observer = ChannelObserver::new(tx);
        (observer, rx)
    }

    fn make_watch_with_deferred(
        &self,
        _init: &Arc<T>,
        config: &AgentExecutionConfig,
    ) -> (Self::WithDefObs, Self::View, Self::DeferredView) {
        let Queue(n) = self;
        let (tx, rx) = mpsc::channel(n.get());
        let observer = ChannelObserver::new(tx);
        let (tx_init, rx_init) = oneshot::channel();
        let deferred = DeferredChannelObserver::Uninitialized(rx_init);
        let joined = observer::join(observer, deferred);
        let deferred_view = DeferredMpscView::new(tx_init, *n, config.yield_after);
        (joined, rx, deferred_view)
    }
}

type WatchArcSender<T> = watch::Sender<Arc<T>>;
type WatchOptArcSender<T> = watch::Sender<Option<Arc<T>>>;

impl<T> ValueLaneWatch<T> for Dropping
where
    T: Any + Default + Send + Sync,
{
    type Obs = ChannelObserver<watch::Sender<Arc<T>>>;
    type WithDefObs = JoinObserver<
        ChannelObserver<WatchArcSender<T>>,
        DeferredChannelObserver<WatchOptArcSender<T>>,
    >;
    type View = watch::Receiver<Arc<T>>;
    type DeferredView = DeferredWatchView<Arc<T>>;

    fn make_watch(&self, init: &Arc<T>) -> (Self::Obs, Self::View) {
        let (tx, rx) = watch::channel(init.clone());
        let observer = ChannelObserver::new(tx);
        (observer, rx)
    }

    fn make_watch_with_deferred(
        &self,
        init: &Arc<T>,
        _config: &AgentExecutionConfig,
    ) -> (Self::WithDefObs, Self::View, Self::DeferredView) {
        let (tx, rx) = watch::channel::<Arc<T>>(init.clone());
        let observer = ChannelObserver::new(tx);
        let (tx_init, rx_init) = oneshot::channel();
        let deferred = DeferredChannelObserver::Uninitialized(rx_init);
        let joined = observer::join(observer, deferred);
        let deferred_view = DeferredWatchView::new(tx_init);
        (joined, rx, deferred_view)
    }
}

type BroadcastArcSender<T> = broadcast::Sender<Arc<T>>;

impl<T> ValueLaneWatch<T> for Buffered
where
    T: Any + Default + Send + Sync,
{
    type Obs = ChannelObserver<broadcast::Sender<Arc<T>>>;
    type WithDefObs = JoinObserver<
        ChannelObserver<BroadcastArcSender<T>>,
        DeferredChannelObserver<BroadcastSender<Arc<T>>>,
    >;
    type View = BroadcastStream<Arc<T>>;
    type DeferredView = DeferredBroadcastView<Arc<T>>;

    fn make_watch(&self, _init: &Arc<T>) -> (Self::Obs, Self::View) {
        let Buffered(n) = self;
        let (tx, rx) = broadcast::channel(n.get());
        let observer = ChannelObserver::new(tx);
        (observer, BroadcastStream::new(rx))
    }

    fn make_watch_with_deferred(
        &self,
        _init: &Arc<T>,
        _config: &AgentExecutionConfig,
    ) -> (Self::WithDefObs, Self::View, Self::DeferredView) {
        let Buffered(n) = self;
        let (tx, rx) = broadcast::channel(n.get());
        let observer = ChannelObserver::new(tx);
        let (tx_init, rx_init) = oneshot::channel();
        let deferred = DeferredChannelObserver::Uninitialized(rx_init);
        let joined = observer::join(observer, deferred);
        let deferred_view = DeferredBroadcastView::new(tx_init, *n);
        (joined, BroadcastStream::new(rx), deferred_view)
    }
}
