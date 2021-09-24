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

use std::future::Future;

use futures::future::{ready, BoxFuture, Ready};
use futures::FutureExt;
use std::convert::Infallible;
use swim_sync::{circular_buffer, topic};
use tokio::sync::{broadcast, mpsc, watch};

pub mod comap;
pub mod drop_all;
pub mod either;
pub mod fail_all;

/// An alternative to the [`futures::Sink`] trait for sinks that can consume their inputs in a
/// single operation. This can simplify operations where one can guarantee that the target sink
/// is a queue and will not be performing IO directly (for example in lane models).
pub trait ItemSink<'a, T> {
    type Error;
    type SendFuture: Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Attempt to send an item into the sink.
    fn send_item(&'a mut self, value: T) -> Self::SendFuture;
}

pub trait ItemSender<T, E>: for<'a> ItemSink<'a, T, Error = E> {
    fn map_err_into<E2>(self) -> map_err::SenderErrInto<Self, E2>
    where
        Self: Sized,
        E2: From<E>,
    {
        map_err::SenderErrInto::new(self)
    }

    fn comap<S, F>(self, f: F) -> comap::ItemSenderComap<Self, F>
    where
        Self: Sized,
        F: FnMut(S) -> T,
    {
        comap::ItemSenderComap::new(self, f)
    }
}

impl<X, T, E> ItemSender<T, E> for X where X: for<'a> ItemSink<'a, T, Error = E> {}

#[derive(Clone, Debug)]
pub struct SendError;

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
pub type BroadcastErr<T> = broadcast::error::SendError<T>;

pub mod map_err {
    use crate::sink::item::ItemSink;
    use futures::future::ErrInto;
    use futures_util::future::TryFutureExt;
    use std::marker::PhantomData;

    #[derive(Debug)]
    pub struct SenderErrInto<Sender, E> {
        sender: Sender,
        _target: PhantomData<E>,
    }

    impl<Sender: Clone, E> Clone for SenderErrInto<Sender, E> {
        fn clone(&self) -> Self {
            SenderErrInto {
                sender: self.sender.clone(),
                _target: PhantomData,
            }
        }
    }

    impl<Sender, E> SenderErrInto<Sender, E> {
        pub fn new(sender: Sender) -> SenderErrInto<Sender, E> {
            SenderErrInto {
                sender,
                _target: PhantomData,
            }
        }
    }

    impl<'a, T, E, Sender> super::ItemSink<'a, T> for SenderErrInto<Sender, E>
    where
        Sender: ItemSink<'a, T>,
        E: From<Sender::Error> + Send + 'a,
    {
        type Error = E;
        type SendFuture = ErrInto<Sender::SendFuture, E>;

        fn send_item(&'a mut self, value: T) -> Self::SendFuture {
            self.sender.send_item(value).err_into()
        }
    }
}

pub struct WatchSink<T>(watch::Sender<Option<T>>);

impl<T> WatchSink<T> {
    pub fn broadcast(&self, value: T) -> Result<(), SendError> {
        match self.0.send(Some(value)) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError {}),
        }
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

impl<'a, T> ItemSink<'a, T> for Vec<T> {
    //TODO Ideally this should be ! but that is still experimental.
    type Error = Infallible;
    type SendFuture = Ready<Result<(), Infallible>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        self.push(value);
        ready(Ok(()))
    }
}

#[derive(Debug, Clone)]
pub struct FnMutSender<S, F> {
    sender: S,
    send_op: F,
}

impl<S, F> FnMutSender<S, F> {
    pub fn new(sender: S, send_op: F) -> Self {
        FnMutSender { sender, send_op }
    }
}

impl<'a, T, E, S, F, Fut> ItemSink<'a, T> for FnMutSender<S, F>
where
    S: 'static,
    F: FnMut(&'a mut S, T) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send + 'a,
{
    type Error = E;
    type SendFuture = Fut;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        let FnMutSender { sender, send_op } = self;
        send_op(sender, value)
    }
}

fn mpsc_send_op<'a, T: Send + 'static>(
    tx: &'a mut mpsc::Sender<T>,
    t: T,
) -> impl Future<Output = Result<(), mpsc::error::SendError<T>>> + Send + 'a {
    tx.send(t)
}

fn watch_send_op<'a, T: Send + 'static>(
    tx: &'a mut WatchSink<T>,
    t: T,
) -> impl Future<Output = Result<(), SendError>> + Send + 'a {
    ready(tx.broadcast(t))
}

fn broadcast_send_op<'a, T: Send + 'static>(
    tx: &'a mut broadcast::Sender<T>,
    t: T,
) -> impl Future<Output = Result<(), broadcast::error::SendError<T>>> + Send + 'a {
    ready(tx.send(t).map(|_| ()))
}

pub fn for_mpsc_sender<T: Send + 'static>(
    tx: mpsc::Sender<T>,
) -> impl ItemSender<T, mpsc::error::SendError<T>> + Clone {
    FnMutSender::new(tx, mpsc_send_op)
}

pub fn for_watch_sender<T: Send + 'static>(
    tx: watch::Sender<Option<T>>,
) -> impl ItemSender<T, SendError> {
    FnMutSender::new(WatchSink(tx), watch_send_op)
}

pub fn for_broadcast_sender<T: Send + 'static>(
    tx: broadcast::Sender<T>,
) -> impl ItemSender<T, broadcast::error::SendError<T>> {
    FnMutSender::new(tx, broadcast_send_op)
}

impl<'a, T> ItemSink<'a, T> for topic::Sender<T>
where
    T: Send + Sync + 'a,
{
    type Error = topic::SendError<T>;
    type SendFuture = topic::TopicSend<'a, T>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        self.send(value)
    }
}

/// Wraps a [`topic::Sender`] for a sink implementation that uses the discarding send function.
pub struct Discarding<T>(pub topic::Sender<T>);

impl<'a, T> ItemSink<'a, T> for Discarding<T>
where
    T: Send + Sync + 'a,
{
    type Error = topic::SendError<T>;
    type SendFuture = topic::TopicSend<'a, T>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        let Discarding(sender) = self;
        sender.discarding_send(value)
    }
}

impl<'a, T: 'a> ItemSink<'a, T> for circular_buffer::Sender<T>
where
    T: Send + Sync,
{
    type Error = circular_buffer::error::SendError<T>;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        ready(self.try_send(value))
    }
}
