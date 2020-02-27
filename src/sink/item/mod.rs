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

use std::future::Future;

use futures::{future, FutureExt};
use futures::future::{Map, Ready};
use tokio::sync::{mpsc, watch};

/// An alternative to the [`futures::Sink`] trait for sinks that can consume their inputs in a
/// single operation. This can simplify operations where one can guarantee that the target sink
/// is a queue and will not be performing IO directly (for example in lane models).
pub trait ItemSink<'a, T> {
    type Error;
    type SendFuture: Future<Output=Result<(), Self::Error>> + Send + 'a;

    /// Attempt to send an item into the sink.
    fn send_item(&'a mut self, value: T) -> Self::SendFuture;
}

/// Wraps a type that cannot directly implement [`ItemSink`].
pub struct ItemSinkWrapper<S, F> {
    sink: S,
    send_operation: F,
}

impl<S, F> ItemSinkWrapper<S, F> {
    pub fn new(sink: S, send_operation: F) -> ItemSinkWrapper<S, F> {
        ItemSinkWrapper {
            sink,
            send_operation,
        }
    }
}

pub type MpscErr<T> = mpsc::error::SendError<T>;
pub type WatchErr<T> = watch::error::SendError<T>;

fn transform_err<T, Err: From<MpscErr<T>>>(result: Result<(), MpscErr<T>>) -> Result<(), Err> {
    result.map_err(|e| e.into())
}

/// Wrap an [`mpsc::Sender`] as an item sink. It is not possible to implement the trait
/// directly as the `send` method returns an anonymous type.
pub fn for_mpsc_sender<T: Send + 'static, Err: From<MpscErr<T>> + 'static>(
    sender: mpsc::Sender<T>,
) -> impl for<'a> ItemSink<'a, T, Error=Err> {
    let err_trans = || transform_err::<T, Err>;

    map_err(ItemSinkWrapper::new(sender, mpsc::Sender::send), err_trans)
}

/// Transform the error type of an [`ItemSink`].
pub fn map_err<T, E1, E2, Snk, Fac, F>(sink: Snk, f: Fac) -> ItemSinkMapErr<Snk, Fac>
    where
        Snk: for<'a> ItemSink<'a, T, Error=E1>,
        F: FnMut(Result<(), E1>) -> Result<(), E2>,
        Fac: FnMut() -> F,
{
    ItemSinkMapErr::new(sink, f)
}

impl<'a, T, Err, S, F, Fut> ItemSink<'a, T> for ItemSinkWrapper<S, F>
    where
        S: 'a,
        Fut: Future<Output=Result<(), Err>> + Send + 'a,
        F: FnMut(&'a mut S, T) -> Fut,
{
    type Error = Err;
    type SendFuture = Fut;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        let ItemSinkWrapper {
            sink,
            send_operation,
        } = self;
        send_operation(sink, value)
    }
}

pub struct ItemSinkMapErr<Snk, Fac> {
    sink: Snk,
    fac: Fac,
}

impl<Snk, Fac> ItemSinkMapErr<Snk, Fac> {
    fn new(sink: Snk, fac: Fac) -> ItemSinkMapErr<Snk, Fac> {
        ItemSinkMapErr { sink, fac }
    }
}

impl<'a, T, E1, E2, Snk, Fac, F> ItemSink<'a, T> for ItemSinkMapErr<Snk, Fac>
    where
        Snk: ItemSink<'a, T, Error=E1>,
        F: FnMut(Result<(), E1>) -> Result<(), E2> + Send + 'a,
        Fac: FnMut() -> F,
{
    type Error = E2;
    type SendFuture = Map<Snk::SendFuture, F>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        let ItemSinkMapErr { sink, fac } = self;
        let f: F = fac();
        sink.send_item(value).map(f)
    }
}

impl<'a, T: Send + 'a> ItemSink<'a, T> for watch::Sender<T> {
    type Error = watch::error::SendError<T>;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        future::ready(self.broadcast(value))
    }
}
