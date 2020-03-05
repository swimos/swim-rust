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

use futures::future::{BoxFuture, Map, Ready};
use futures::task::{Context, Poll};
use futures::{future, FutureExt};
use std::marker::PhantomData;
use std::pin::Pin;
use tokio::sync::{mpsc, watch};

/// An alternative to the [`futures::Sink`] trait for sinks that can consume their inputs in a
/// single operation. This can simplify operations where one can guarantee that the target sink
/// is a queue and will not be performing IO directly (for example in lane models).
pub trait ItemSink<'a, T> {
    type Error;
    type SendFuture: Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Attempt to send an item into the sink.
    fn send_item(&'a mut self, value: T) -> Self::SendFuture;
}

pub type BoxItemSink<T, E> =
    Box<dyn for<'a> ItemSink<'a, T, Error = E, SendFuture = BoxFuture<'a, Result<(), E>>>>;

pub fn boxed<T, E, Snk>(sink: Snk) -> BoxItemSink<T, E>
where
    T: 'static,
    Snk: for<'a> ItemSink<'a, T, Error = E> + 'static,
{
    let boxing_sink = BoxingSink(sink);
    let boxed: BoxItemSink<T, E> = Box::new(boxing_sink);
    boxed
}

pub type MpscErr<T> = mpsc::error::SendError<T>;
pub type WatchErr<T> = watch::error::SendError<T>;

fn transform_err<T, Err: From<MpscErr<T>>>(result: Result<(), MpscErr<T>>) -> Result<(), Err> {
    result.map_err(|e| e.into())
}

/// Wrap an [`mpsc::Sender`] as an item sink. It is not possible to implement the trait
/// directly as the `send` method returns an anonymous type.
pub fn for_mpsc_sender<T: Unpin + Send + 'static, Err: From<MpscErr<T>> + 'static>(
    sender: mpsc::Sender<T>,
) -> impl for<'a> ItemSink<'a, T, Error = Err> {
    let err_trans = || transform_err::<T, Err>;

    map_err(sender, err_trans)
}

pub struct MpscSend<'a, T, E> {
    sender: Pin<&'a mut mpsc::Sender<T>>,
    value: Option<T>,
    _err: PhantomData<E>,
}

impl<'a, T, E> MpscSend<'a, T, E> {
    pub fn new(sender: &'a mut mpsc::Sender<T>, value: T) -> MpscSend<'a, T, E> {
        MpscSend {
            sender: Pin::new(sender),
            value: Some(value),
            _err: PhantomData,
        }
    }
}

impl<'a, T: Unpin, E> Future for MpscSend<'a, T, E>
where
    E: From<mpsc::error::SendError<T>> + Unpin,
{
    type Output = Result<(), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let MpscSend { sender, value, .. } = self.get_mut();

        match sender.poll_ready(cx) {
            Poll::Ready(Ok(_)) => match value.take() {
                Some(t) => match sender.try_send(t) {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(mpsc::error::TrySendError::Closed(t)) => {
                        Poll::Ready(Err(mpsc::error::SendError(t).into()))
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => unreachable!(),
                },
                _ => panic!("Send future evaluated twice."),
            },
            Poll::Ready(Err(_)) => match value.take() {
                Some(t) => Poll::Ready(Err(mpsc::error::SendError(t).into())),
                _ => panic!("Send future evaluated twice."),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a, T: Unpin + Send + 'a> ItemSink<'a, T> for mpsc::Sender<T> {
    type Error = mpsc::error::SendError<T>;
    type SendFuture = MpscSend<'a, T, mpsc::error::SendError<T>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        MpscSend {
            sender: Pin::new(self),
            value: Some(value),
            _err: PhantomData,
        }
    }
}

/// Transform the error type of an [`ItemSink`].
pub fn map_err<T, E1, E2, Snk, Fac, F>(sink: Snk, f: Fac) -> ItemSinkMapErr<Snk, Fac>
where
    Snk: for<'a> ItemSink<'a, T, Error = E1>,
    F: FnMut(Result<(), E1>) -> Result<(), E2>,
    Fac: FnMut() -> F,
{
    ItemSinkMapErr::new(sink, f)
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
    Snk: ItemSink<'a, T, Error = E1>,
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

struct BoxingSink<Snk>(Snk);

impl<'a, T, Snk> ItemSink<'a, T> for BoxingSink<Snk>
where
    Snk: ItemSink<'a, T> + 'a,
    T: 'a,
{
    type Error = Snk::Error;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        FutureExt::boxed(self.0.send_item(value))
    }
}

impl<'a, T, E: 'a> ItemSink<'a, T> for BoxItemSink<T, E> {
    type Error = E;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        (**self).send_item(value)
    }
}
