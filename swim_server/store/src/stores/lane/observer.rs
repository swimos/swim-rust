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

use utilities::sync::topic;

use crate::engines::mem::var::observer::{Observer, ObserverStream, ObserverSubscriber};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use utilities::sync::topic::{SubscribeError, TryRecvError};

pub enum StoreObserver<T> {
    Db(topic::Receiver<Arc<T>>),
    Mem(Observer<T>),
}

impl<T> StoreObserver<T> {
    pub fn subscriber(&self) -> StoreSubscriber<T> {
        match self {
            StoreObserver::Db(rx) => StoreSubscriber::Db(rx.subscriber()),
            StoreObserver::Mem(rx) => StoreSubscriber::Mem(rx.subscriber()),
        }
    }

    pub fn into_subscriber(self) -> StoreSubscriber<T> {
        match self {
            StoreObserver::Db(rx) => StoreSubscriber::Db(rx.subscriber()),
            StoreObserver::Mem(rx) => StoreSubscriber::Mem(rx.subscriber()),
        }
    }
}

impl<T: Any + Send + Sync> StoreObserver<T> {
    pub async fn recv(&mut self) -> Option<Arc<T>> {
        match self {
            StoreObserver::Db(rx) => rx.recv().await.map(|e| e.clone()),
            StoreObserver::Mem(rx) => rx.recv().await,
        }
    }

    pub fn try_recv(&mut self) -> Result<Arc<T>, TryRecvError> {
        match self {
            StoreObserver::Db(rx) => rx.try_recv().map(|e| e.clone()),
            StoreObserver::Mem(rx) => rx.try_recv(),
        }
    }

    pub fn into_stream(self) -> StoreStream<T> {
        match self {
            StoreObserver::Db(rx) => StoreStream::Db(rx.into_stream()),
            StoreObserver::Mem(rx) => StoreStream::Mem(rx.into_stream()),
        }
    }
}

pub enum StoreSubscriber<T> {
    Db(topic::Subscriber<Arc<T>>),
    Mem(ObserverSubscriber<T>),
}

impl<T> Clone for StoreSubscriber<T> {
    fn clone(&self) -> Self {
        match self {
            StoreSubscriber::Db(rx) => StoreSubscriber::Db(rx.clone()),
            StoreSubscriber::Mem(rx) => StoreSubscriber::Mem(rx.clone()),
        }
    }
}

impl<T> StoreSubscriber<T> {
    pub fn subscribe(&self) -> Result<StoreObserver<T>, SubscribeError> {
        match self {
            StoreSubscriber::Db(rx) => rx.subscribe().map(|s| StoreObserver::Db(s)),
            StoreSubscriber::Mem(rx) => rx.subscribe().map(|s| StoreObserver::Mem(s)),
        }
    }
}

pub enum StoreStream<T> {
    Db(topic::ReceiverStream<Arc<T>>),
    Mem(ObserverStream<T>),
}

impl<T: Any + Send + Sync> Stream for StoreStream<T> {
    type Item = Arc<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            StoreStream::Db(rx) => rx.poll_next_unpin(cx),
            StoreStream::Mem(rx) => rx.poll_next_unpin(cx),
        }
    }
}
