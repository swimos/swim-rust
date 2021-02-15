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

use futures::future::{ready, BoxFuture};
use futures::{FutureExt, Stream, TryFutureExt};
use pin_project::pin_project;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::error::TrySendError as MpscTrySendError;
use tokio::sync::watch::error::SendError as WatchSendError;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, WatchStream as TokioWatchStream};

pub type BoxSupplier<T> = Box<dyn Supplier<T> + Send + Sync + 'static>;

pub struct SupplyError;

pub enum TrySupplyError {
    Closed,
    Capacity,
}

pub trait Supplier<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), TrySupplyError>;

    fn supply(&self, item: T) -> BoxFuture<Result<(), SupplyError>>;
}

pub trait SupplyLaneObserver<T: Clone>
where
    T: Send + Sync + 'static,
{
    type Sender: Supplier<T> + Send + Sync + 'static;
    type View: Stream<Item = T> + Send + Sync + 'static;

    fn make_observer(&self) -> (Self::Sender, Self::View);
}

impl<T> Supplier<T> for mpsc::Sender<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), TrySupplyError> {
        self.try_send(item).map_err(Into::into)
    }

    fn supply(&self, item: T) -> BoxFuture<Result<(), SupplyError>> {
        self.send(item).map_err(|_| SupplyError).boxed()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Dropping;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Queue(pub NonZeroUsize);

impl Default for Queue {
    fn default() -> Self {
        Queue(NonZeroUsize::new(10).unwrap())
    }
}

impl<T> From<MpscTrySendError<T>> for TrySupplyError {
    fn from(e: MpscTrySendError<T>) -> Self {
        match e {
            MpscTrySendError::Closed(_) => TrySupplyError::Closed,
            MpscTrySendError::Full(_) => TrySupplyError::Capacity,
        }
    }
}

impl<T> From<WatchSendError<T>> for TrySupplyError {
    fn from(_: WatchSendError<T>) -> Self {
        TrySupplyError::Closed
    }
}

impl From<TrySupplyError> for SupplyError {
    fn from(_: TrySupplyError) -> Self {
        SupplyError
    }
}

impl<T> SupplyLaneObserver<T> for Queue
where
    T: Clone + Send + Sync + 'static,
{
    type Sender = mpsc::Sender<T>;
    type View = ReceiverStream<T>;

    fn make_observer(&self) -> (Self::Sender, Self::View) {
        let Queue(size) = self;
        let (tx, rx) = mpsc::channel(size.get());

        (tx, ReceiverStream::new(rx))
    }
}

pub struct WatchSupplier<T>(watch::Sender<Option<T>>);

impl<T> Supplier<T> for WatchSupplier<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), TrySupplyError> {
        self.0.send(Some(item)).map_err(Into::into)
    }

    fn supply(&self, item: T) -> BoxFuture<Result<(), SupplyError>> {
        ready(self.try_supply(item).map_err(Into::into)).boxed()
    }
}

#[pin_project]
pub struct WatchStream<T>(#[pin] TokioWatchStream<Option<T>>);
impl<T> Stream for WatchStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx).map(Option::flatten)
    }
}

impl<T> SupplyLaneObserver<T> for Dropping
where
    T: Clone + Send + Sync + Unpin + 'static,
{
    type Sender = WatchSupplier<T>;
    type View = WatchStream<T>;

    fn make_observer(&self) -> (Self::Sender, Self::View) {
        let (tx, rx) = watch::channel(None);
        (WatchSupplier(tx), WatchStream(TokioWatchStream::new(rx)))
    }
}
