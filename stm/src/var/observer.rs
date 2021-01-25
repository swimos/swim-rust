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

use crate::var::Contents;
use futures::{Stream, StreamExt};
use futures_util::task::Context;
use std::any::{type_name, Any};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::macros::support::{Pin, Poll};
use utilities::sync::topic::{self, SubscribeError, TryRecvError};

pub struct Observer<T> {
    inner: topic::Receiver<Contents>,
    _type: PhantomData<fn() -> Arc<T>>,
}

pub struct ObserverSubscriber<T> {
    inner: topic::Subscriber<Contents>,
    _type: PhantomData<fn() -> Arc<T>>,
}

pub struct ObserverStream<T> {
    inner: topic::ReceiverStream<Contents>,
    _type: PhantomData<fn() -> Arc<T>>,
}

impl<T> Observer<T> {
    pub(super) fn new(inner: topic::Receiver<Contents>) -> Self {
        Observer {
            inner,
            _type: PhantomData,
        }
    }

    pub fn subscriber(&self) -> ObserverSubscriber<T> {
        ObserverSubscriber {
            inner: self.inner.subscriber(),
            _type: PhantomData,
        }
    }

    pub fn into_subscriber(self) -> ObserverSubscriber<T> {
        ObserverSubscriber {
            inner: self.inner.subscriber(),
            _type: PhantomData,
        }
    }
}

impl<T> Clone for Observer<T> {
    fn clone(&self) -> Self {
        Observer {
            inner: self.inner.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Debug for Observer<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Observer")
            .field("inner", &self.inner)
            .field("type", &type_name::<T>())
            .finish()
    }
}

impl<T: Any + Send + Sync> Observer<T> {
    pub async fn recv(&mut self) -> Option<Arc<T>> {
        self.inner.recv().await.map(|contents| {
            contents
                .clone()
                .downcast()
                .expect("Unexpected type from TVar")
        })
    }

    pub fn try_recv(&mut self) -> Result<Arc<T>, TryRecvError> {
        self.inner.try_recv().map(|contents| {
            contents
                .clone()
                .downcast()
                .expect("Unexpected type from TVar")
        })
    }

    pub fn into_stream(self) -> ObserverStream<T> {
        ObserverStream {
            inner: self.inner.into_stream(),
            _type: PhantomData,
        }
    }
}

impl<T> ObserverSubscriber<T> {
    pub fn subscribe(&self) -> Result<Observer<T>, SubscribeError> {
        self.inner.subscribe().map(Observer::new)
    }
}

impl<T> Clone for ObserverSubscriber<T> {
    fn clone(&self) -> Self {
        ObserverSubscriber {
            inner: self.inner.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: Any + Send + Sync> Stream for ObserverStream<T> {
    type Item = Arc<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_next_unpin(cx)
            .map(|maybe_contents| maybe_contents.and_then(|contents| contents.downcast().ok()))
    }
}
