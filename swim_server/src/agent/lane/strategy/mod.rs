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

use futures::future::{ready, Either, Ready};
use futures::ready;
use pin_project::pin_project;
use std::any::Any;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use stm::var::observer::Observer;
use swim_common::sink::item::MpscSend;
use swim_common::topic::BroadcastSender;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

#[cfg(test)]
mod tests;

//Strategies for watching events from a lane.

/// Push lane events into a bounded queue.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Queue(pub NonZeroUsize);

/// Publish the latest lane event.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Dropping;

/// Publish the latest lane events to a bounded buffer.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Buffered(pub NonZeroUsize);

/// The default buffer size for the [`Queue`] and [`Buffered`] strategies.
const DEFAULT_BUFFER: usize = 10;

fn default_buffer() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_BUFFER).unwrap()
}

impl Default for Queue {
    fn default() -> Self {
        Queue(default_buffer())
    }
}

impl Default for Buffered {
    fn default() -> Self {
        Buffered(default_buffer())
    }
}

/// Transactional variable observer based on a channel sender.
pub struct ChannelObserver<S>(S, bool);

impl<S> ChannelObserver<S> {
    pub fn new(sender: S) -> Self {
        ChannelObserver(sender, false)
    }
}

/// Transactional variable observer that is initially absent.
pub enum DeferredChannelObserver<S> {
    Uninitialized(oneshot::Receiver<S>),
    Initialized(ChannelObserver<S>),
    Closed,
}

impl<S> DeferredChannelObserver<S> {
    pub fn new(sender: oneshot::Receiver<S>) -> Self {
        DeferredChannelObserver::Uninitialized(sender)
    }
}

type ArcSend<'a, T> = MpscSend<'a, Arc<T>, mpsc::error::SendError<Arc<T>>>;

impl<'a, T> Observer<'a, Arc<T>> for ChannelObserver<mpsc::Sender<Arc<T>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Either<Ready<()>, DiscardError<'a, ArcSend<'a, T>>>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        let ChannelObserver(sender, is_dead) = self;
        if *is_dead {
            Either::Left(ready(()))
        } else {
            Either::Right(DiscardError {
                future: MpscSend::new(sender, value),
                is_dead,
            })
        }
    }
}

/// Adapts a fallible future to discard the error and set the owning [`Observer`] to be dead.
#[pin_project]
pub struct DiscardError<'a, F> {
    #[pin]
    future: F,
    is_dead: &'a mut bool,
}

impl<'a, T> Future for DiscardError<'a, MpscSend<'a, T, mpsc::error::SendError<T>>> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let result = ready!(projected.future.poll(cx));
        if result.is_err() {
            **projected.is_dead = true;
        }
        Poll::Ready(())
    }
}

impl<'a, T> Observer<'a, Arc<T>> for ChannelObserver<watch::Sender<Arc<T>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Ready<()>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        let ChannelObserver(sender, is_dead) = self;
        if !*is_dead && sender.broadcast(value).is_err() {
            *is_dead = true;
        }
        ready(())
    }
}

impl<'a, T> Observer<'a, Arc<T>> for ChannelObserver<watch::Sender<Option<Arc<T>>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Ready<()>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        let ChannelObserver(sender, is_dead) = self;
        if !*is_dead && sender.broadcast(Some(value)).is_err() {
            *is_dead = true;
        }
        ready(())
    }
}

impl<'a, T> Observer<'a, Arc<T>> for ChannelObserver<broadcast::Sender<Arc<T>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Ready<()>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        let ChannelObserver(sender, is_dead) = self;
        if !*is_dead && sender.send(value).is_err() {
            *is_dead = true;
        }
        ready(())
    }
}

impl<'a, T> Observer<'a, Arc<T>> for ChannelObserver<BroadcastSender<Arc<T>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Ready<()>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        let ChannelObserver(sender, is_dead) = self;
        if !*is_dead && sender.send(value).is_err() {
            *is_dead = true;
        }
        ready(())
    }
}

impl<'a, T> Observer<'a, Arc<T>> for DeferredChannelObserver<mpsc::Sender<Arc<T>>>
where
    T: Any + Send + Sync,
{
    type RecFuture = Either<Ready<()>, DiscardError<'a, ArcSend<'a, T>>>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        loop {
            match self {
                DeferredChannelObserver::Uninitialized(rec) => match rec.try_recv() {
                    Ok(obs) => {
                        *self = DeferredChannelObserver::Initialized(ChannelObserver::new(obs));
                    }
                    Err(TryRecvError::Empty) => {
                        return Either::Left(ready(()));
                    }
                    Err(TryRecvError::Closed) => {
                        *self = DeferredChannelObserver::Closed;
                        return Either::Left(ready(()));
                    }
                },
                DeferredChannelObserver::Initialized(obs) => {
                    return obs.notify(value);
                }
                DeferredChannelObserver::Closed => {
                    return Either::Left(ready(()));
                }
            };
        }
    }
}

impl<'a, T, S> Observer<'a, Arc<T>> for DeferredChannelObserver<S>
where
    S: 'static,
    T: Any + Send + Sync,
    ChannelObserver<S>: Observer<'a, Arc<T>, RecFuture = Ready<()>>,
{
    type RecFuture = Ready<()>;

    fn notify(&'a mut self, value: Arc<T>) -> Self::RecFuture {
        loop {
            match self {
                DeferredChannelObserver::Uninitialized(rec) => match rec.try_recv() {
                    Ok(obs) => {
                        *self = DeferredChannelObserver::Initialized(ChannelObserver::new(obs));
                    }
                    Err(TryRecvError::Empty) => {
                        return ready(());
                    }
                    Err(TryRecvError::Closed) => {
                        *self = DeferredChannelObserver::Closed;
                        return ready(());
                    }
                },
                DeferredChannelObserver::Initialized(obs) => {
                    return obs.notify(value);
                }
                DeferredChannelObserver::Closed => {
                    return ready(());
                }
            };
        }
    }
}
