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

use futures::future::BoxFuture;
use futures::{Future, FutureExt};

pub trait WebSocketSink<'a, M, C> {
    type Error;
    type SendFuture: Future<Output = Result<(), Self::Error>> + Send + 'a;
    type CloseFuture: Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn send_item(&'a mut self, value: M) -> Self::SendFuture;

    fn close(self, with: C) -> Self::CloseFuture;
}

pub trait WebSocketSender<M, C, E>: for<'a> WebSocketSink<'a, M, C, Error = E> {
    fn map_err_into<E2>(self) -> map_err::WsSenderErrInto<Self, E2>
    where
        Self: Sized,
        E2: From<E>,
    {
        map_err::WsSenderErrInto::new(self)
    }
}

struct BoxingSink<Snk>(Snk);

impl<'a, M, C, Snk> WebSocketSink<'a, M, C> for BoxingSink<Snk>
where
    Snk: WebSocketSink<'a, M, C> + 'a,
    M: 'a,
    C: 'a,
{
    type Error = Snk::Error;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;
    type CloseFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: M) -> Self::SendFuture {
        FutureExt::boxed(self.0.send_item(value))
    }

    fn close(self, with: C) -> Self::CloseFuture {
        FutureExt::boxed(self.0.close(with))
    }
}

impl<'a, M, C, E> WebSocketSink<'a, M, C> for BoxWebSocketSender<M, C, E>
where
    E: 'static,
    M: 'static,
    C: 'static,
{
    type Error = E;
    type SendFuture = BoxFuture<'a, Result<(), E>>;
    type CloseFuture = BoxFuture<'a, Result<(), E>>;

    fn send_item(&'a mut self, _value: M) -> Self::SendFuture {
        todo!()
    }

    fn close(self, _with: C) -> Self::CloseFuture {
        todo!()
    }
}

pub type BoxWebSocketSender<M, C, E> = Box<
    dyn for<'a> WebSocketSink<
        'a,
        M,
        C,
        Error = E,
        SendFuture = BoxFuture<'a, Result<(), E>>,
        CloseFuture = BoxFuture<'a, Result<(), E>>,
    >,
>;

pub fn boxed<M, C, E, Snk>(sink: Snk) -> BoxWebSocketSender<M, C, E>
where
    M: 'static,
    C: 'static,
    Snk: for<'a> WebSocketSink<'a, M, C, Error = E> + 'static,
{
    let boxing_sink = BoxingSink(sink);
    let boxed: BoxWebSocketSender<M, C, E> = Box::new(boxing_sink);
    boxed
}

impl<X, M, C, E> WebSocketSender<M, C, E> for X where X: for<'a> WebSocketSink<'a, M, C, Error = E> {}

#[derive(Debug, Clone)]
pub struct FnMutSender<S, F, C> {
    sender: S,
    send_op: F,
    close_op: C,
}

impl<S, F, C> FnMutSender<S, F, C> {
    pub fn new(sender: S, send_op: F, close_op: C) -> Self {
        FnMutSender {
            sender,
            send_op,
            close_op,
        }
    }
}

impl<'a, M, C, E, S, SendFn, SendFut, CloseFn, CloseFut> WebSocketSink<'a, M, C>
    for FnMutSender<S, SendFn, CloseFn>
where
    S: 'static,
    SendFn: FnMut(&'a mut S, M) -> SendFut,
    SendFut: Future<Output = Result<(), E>> + Send + 'a,
    CloseFn: FnMut(S, C) -> CloseFut,
    CloseFut: Future<Output = Result<(), E>> + Send + 'a,
{
    type Error = E;
    type SendFuture = SendFut;
    type CloseFuture = CloseFut;

    fn send_item(&'a mut self, value: M) -> Self::SendFuture {
        let FnMutSender {
            sender, send_op, ..
        } = self;
        send_op(sender, value)
    }

    fn close(self, with: C) -> Self::CloseFuture {
        let FnMutSender {
            sender,
            mut close_op,
            ..
        } = self;
        close_op(sender, with)
    }
}

pub mod map_err {
    use crate::ws2::sender::WebSocketSink;
    use futures::future::ErrInto;
    use futures::TryFutureExt;
    use std::marker::PhantomData;

    #[derive(Debug)]
    pub struct WsSenderErrInto<Sender, E> {
        sender: Sender,
        _target: PhantomData<E>,
    }

    impl<Sender: Clone, E> Clone for WsSenderErrInto<Sender, E> {
        fn clone(&self) -> Self {
            WsSenderErrInto {
                sender: self.sender.clone(),
                _target: PhantomData,
            }
        }
    }

    impl<Sender, E> WsSenderErrInto<Sender, E> {
        pub fn new(sender: Sender) -> WsSenderErrInto<Sender, E> {
            WsSenderErrInto {
                sender,
                _target: PhantomData,
            }
        }
    }

    impl<'a, M, C, E, Sender> WebSocketSink<'a, M, C> for WsSenderErrInto<Sender, E>
    where
        Sender: WebSocketSink<'a, M, C>,
        E: From<Sender::Error> + Send + 'a,
    {
        type Error = E;
        type SendFuture = ErrInto<Sender::SendFuture, E>;
        type CloseFuture = ErrInto<Sender::CloseFuture, E>;

        fn send_item(&'a mut self, value: M) -> Self::SendFuture {
            self.sender.send_item(value).err_into()
        }

        fn close(self, with: C) -> Self::CloseFuture {
            self.sender.close(with).err_into()
        }
    }
}
