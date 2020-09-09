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

#[cfg(test)]
mod tests;

#[cfg(feature = "websocket")]
pub mod tungstenite;

#[cfg(not(target_arch = "wasm32"))]
pub mod async_factory {
    use futures::future::ErrInto as FutErrInto;
    use futures::stream::StreamExt;
    use futures::TryFutureExt;
    use futures::{Future, Sink, Stream};
    use tokio::sync::{mpsc, oneshot};

    use swim_common::request::request_future::{RequestFuture, SendAndAwait, Sequenced};
    use swim_common::request::Request;

    use crate::connections::factory::tungstenite::HostConfig;
    use swim_common::ws::error::ConnectionError;
    use swim_common::ws::WsMessage;
    use swim_runtime::task::{spawn, TaskHandle};
    use utilities::errors::FlattenErrors;

    /// A request for a new connection.
    pub struct ConnReq<Snk, Str> {
        pub(crate) request: Request<Result<(Snk, Str), ConnectionError>>,
        url: url::Url,
        config: HostConfig,
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
        #[allow(dead_code)]
        pub(in crate::connections) async fn new<Fac, Fut>(
            buffer_size: usize,
            connect_async: Fac,
        ) -> Self
        where
            Fac: FnMut(url::Url, HostConfig) -> Fut + Send + 'static,
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

    #[allow(dead_code)]
    async fn factory_task<Snk, Str, Fac, Fut>(
        mut receiver: mpsc::Receiver<ConnReq<Snk, Str>>,
        mut connect_async: Fac,
    ) where
        Str: Send + 'static,
        Snk: Send + 'static,
        Fac: FnMut(url::Url, HostConfig) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(Snk, Str), ConnectionError>> + Send + 'static,
    {
        while let Some(ConnReq {
            request,
            url,
            config,
        }) = receiver.next().await
        {
            let conn: Result<(Snk, Str), ConnectionError> = connect_async(url, config).await;
            let _ = request.send(conn);
        }
    }

    pub type ConnectionFuture<Str, Snk> =
        SendAndAwait<ConnReq<Snk, Str>, Result<(Snk, Str), ConnectionError>>;

    impl<Snk, Str> AsyncFactory<Snk, Str>
    where
        Str: Stream<Item = Result<WsMessage, ConnectionError>> + Unpin + Send + 'static,
        Snk: Sink<WsMessage> + Unpin + Send + 'static,
    {
        pub fn connect_using(
            &mut self,
            url: url::Url,
            config: HostConfig,
        ) -> FlattenErrors<FutErrInto<ConnectionFuture<Str, Snk>, ConnectionError>> {
            let (tx, rx) = oneshot::channel();
            let req = ConnReq {
                request: Request::new(tx),
                url,
                config,
            };
            let req_fut = RequestFuture::new(self.sender.clone(), req);
            FlattenErrors::new(TryFutureExt::err_into::<ConnectionError>(Sequenced::new(
                req_fut, rx,
            )))
        }
    }
}
