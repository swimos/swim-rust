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

use swim_runtime::ws::{CompressionSwitcherProvider, Protocol};

#[cfg(test)]
mod tests;

pub mod stream;
mod swim_ratchet;

pub use swim_ratchet::RatchetWebSocketFactory;

#[derive(Clone)]
pub struct HostConfig {
    pub protocol: Protocol,
    pub compression_level: CompressionSwitcherProvider,
}

pub mod async_factory {
    use futures::Future;
    use ratchet::{Extension, WebSocket};
    use tokio::sync::{mpsc, oneshot};

    use swim_async_runtime::task::{spawn, TaskHandle};
    use swim_runtime::error::ConnectionError;
    use swim_utilities::future::request::Request;

    use crate::connections::factory::HostConfig;

    /// A request for a new connection.
    pub struct ConnReq<Sock, Ext> {
        pub(crate) request: Request<Result<WebSocket<Sock, Ext>, ConnectionError>>,
        url: url::Url,
        config: HostConfig,
    }

    /// Abstract asynchronous factory where requests are serviced by an independent task.
    pub struct AsyncFactory<Sock, Ext> {
        pub(in crate::connections::factory) sender: mpsc::Sender<ConnReq<Sock, Ext>>,
        _task: TaskHandle<()>,
    }

    impl<Sock, Ext> AsyncFactory<Sock, Ext>
    where
        Sock: Send + Sync + 'static,
        Ext: Extension + Send + Sync + 'static,
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
            Fut: Future<Output = Result<WebSocket<Sock, Ext>, ConnectionError>> + Send + 'static,
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
    async fn factory_task<Sock, Ext, Fac, Fut>(
        mut receiver: mpsc::Receiver<ConnReq<Sock, Ext>>,
        mut connect_async: Fac,
    ) where
        Sock: Send + Sync + 'static,
        Ext: Extension + Send + Sync + 'static,
        Fac: FnMut(url::Url, HostConfig) -> Fut + Send + 'static,
        Fut: Future<Output = Result<WebSocket<Sock, Ext>, ConnectionError>> + Send + 'static,
    {
        while let Some(ConnReq {
            request,
            url,
            config,
        }) = receiver.recv().await
        {
            let conn: Result<WebSocket<Sock, Ext>, ConnectionError> =
                connect_async(url, config).await;
            let _ = request.send(conn);
        }
    }

    impl<Sock, Ext> AsyncFactory<Sock, Ext>
    where
        Sock: Send + Sync + 'static,
        Ext: Extension + Send + Sync + 'static,
    {
        pub async fn connect_using(
            &mut self,
            url: url::Url,
            config: HostConfig,
        ) -> Result<WebSocket<Sock, Ext>, ConnectionError> {
            let (tx, rx) = oneshot::channel();
            let req = ConnReq {
                request: Request::new(tx),
                url,
                config,
            };
            self.sender.send(req).await?;
            Ok(rx.await??)
        }
    }
}
