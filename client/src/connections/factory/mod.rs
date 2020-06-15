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

use futures::{Future, Sink, Stream};

use crate::connections::ConnectionError;

#[cfg(test)]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
pub mod tungstenite;

#[allow(dead_code)]
#[cfg(target_arch = "wasm32")]
pub mod wasm;

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    /// Type of the stream of incoming messages.
    type WsStream: Stream<Item = Result<String, ConnectionError>> + Unpin + Send + 'static;

    /// Type of the sink for outgoing messages.
    type WsSink: Sink<String> + Unpin + Send + 'static;

    type ConnectFut: Future<Output = Result<(Self::WsSink, Self::WsStream), ConnectionError>>
        + Send
        + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> Self::ConnectFut;
}

#[cfg(not(target_arch = "wasm32"))]
pub mod async_factory {
    use futures::future::ErrInto as FutErrInto;
    use futures::stream::StreamExt;
    use futures::TryFutureExt;
    use futures::{Future, Sink, Stream};
    use tokio::sync::{mpsc, oneshot};

    use common::request::request_future::{RequestFuture, SendAndAwait, Sequenced};
    use common::request::Request;

    use crate::connections::factory::errors::FlattenErrors;
    use crate::connections::factory::WebsocketFactory;
    use crate::connections::ConnectionError;
    use utilities::rt::task::{spawn, TaskHandle};

    /// A request for a new connection.
    pub struct ConnReq<Snk, Str> {
        request: Request<Result<(Snk, Str), ConnectionError>>,
        url: url::Url,
    }

    /// Abstract asynchronous factory where requests are serviced by an independent task.
    pub struct AsyncFactory<Snk, Str> {
        pub(in crate::connections::factory) sender: mpsc::Sender<ConnReq<Snk, Str>>,
        _task: TaskHandle<()>,
    }

    impl<Snk, Str> AsyncFactory<Snk, Str>
    where
        Str: Send + 'static,
        Snk: Send + 'static,
    {
        /// Create a new factory where the task operates off a queue with [`buffer_size`] entries
        /// and uses [`connect_async`] to service the requests.
        pub(in crate::connections) async fn new<Fac, Fut>(
            buffer_size: usize,
            connect_async: Fac,
        ) -> Self
        where
            Fac: FnMut(url::Url) -> Fut + Send + 'static,
            Fut: Future<Output = Result<(Snk, Str), ConnectionError>> + Send + 'static,
        {
            let (tx, rx) = mpsc::channel(buffer_size);
            let task = spawn(factory_task(rx, connect_async));
            AsyncFactory {
                sender: tx,
                _task: task,
            }
        }
    }

    async fn factory_task<Snk, Str, Fac, Fut>(
        mut receiver: mpsc::Receiver<ConnReq<Snk, Str>>,
        mut connect_async: Fac,
    ) where
        Str: Send + 'static,
        Snk: Send + 'static,
        Fac: FnMut(url::Url) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(Snk, Str), ConnectionError>> + Send + 'static,
    {
        while let Some(ConnReq { request, url }) = receiver.next().await {
            let conn: Result<(Snk, Str), ConnectionError> = connect_async(url).await;
            let _ = request.send(conn);
        }
    }

    pub type ConnectionFuture<Str, Snk> =
        SendAndAwait<ConnReq<Snk, Str>, Result<(Snk, Str), ConnectionError>>;

    impl<Snk, Str> WebsocketFactory for AsyncFactory<Snk, Str>
    where
        Str: Stream<Item = Result<String, ConnectionError>> + Unpin + Send + 'static,
        Snk: Sink<String> + Unpin + Send + 'static,
    {
        type WsStream = Str;
        type WsSink = Snk;
        type ConnectFut = FlattenErrors<FutErrInto<ConnectionFuture<Str, Snk>, ConnectionError>>;

        fn connect(&mut self, url: url::Url) -> Self::ConnectFut {
            let (tx, rx) = oneshot::channel();
            let req = ConnReq {
                request: Request::new(tx),
                url,
            };
            let req_fut = RequestFuture::new(self.sender.clone(), req);
            FlattenErrors::new(TryFutureExt::err_into::<ConnectionError>(Sequenced::new(
                req_fut, rx,
            )))
        }
    }
}

pub mod errors {
    use futures::task::{Context, Poll};
    use futures::Future;
    use tokio::macros::support::Pin;

    pub struct FlattenErrors<F> {
        inner: F,
    }

    impl<F: Unpin> Unpin for FlattenErrors<F> {}

    impl<F> FlattenErrors<F> {
        pub fn new(inner: F) -> Self {
            FlattenErrors { inner }
        }
    }

    impl<F, T, E> Future for FlattenErrors<F>
    where
        F: Future<Output = Result<Result<T, E>, E>> + Unpin,
    {
        type Output = Result<T, E>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let f = &mut self.get_mut().inner;
            Pin::new(f).poll(cx).map(|r| r.and_then(|r2| r2))
        }
    }
}
