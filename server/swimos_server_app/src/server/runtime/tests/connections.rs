// Copyright 2015-2024 Swim Inc.
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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
use std::{net::SocketAddr, pin::Pin};

use bytes::BytesMut;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use ratchet::{ExtensionProvider, Role, WebSocket, WebSocketConfig, WebSocketStream};
use swimos_messages::remote_protocol::FindNode;
use swimos_remote::dns::{DnsFut, DnsResolver};
use swimos_remote::websocket::{RatchetError, WebsocketClient, WebsocketServer, WsOpenFuture};
use swimos_remote::{
    ConnectionError, ExternalConnections, Listener, ListenerError, ListenerResult, Scheme,
};
use tokio::{
    io::{self, DuplexStream},
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug)]
#[allow(dead_code)]
enum ConnReq {
    Remote(
        Scheme,
        SocketAddr,
        oneshot::Sender<Result<DuplexStream, ConnectionError>>,
    ),
    Resolve(String, u16, oneshot::Sender<io::Result<Vec<SocketAddr>>>),
    Listener(
        SocketAddr,
        oneshot::Sender<Result<TestListener, ConnectionError>>,
    ),
}

/// Fake networking that communicates over in-memory buffers.
#[derive(Debug, Clone)]
pub struct TestConnections {
    requests: mpsc::Sender<ConnReq>,
}

/// Fake websockets implementation that skips the handshake.
#[derive(Default)]
pub struct TestWs {
    config: WebSocketConfig,
}

impl WebsocketClient for TestWs {
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        _provider: &'a Provider,
        _addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static,
    {
        ready(Ok(WebSocket::from_upgraded(
            self.config,
            socket,
            None,
            BytesMut::new(),
            Role::Client,
        )))
        .boxed()
    }
}

impl WebsocketServer for TestWs {
    type WsStream<Sock, Ext> =
        BoxStream<'static, Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>>;

    fn wrap_listener<Sock, L, Provider>(
        &self,
        listener: L,
        _provider: Provider,
        _find: mpsc::Sender<FindNode>,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: io::AsyncRead + io::AsyncWrite + Unpin + Send + Sync + 'static,
        L: Listener<Sock> + Send + 'static,
        Provider: ratchet::ExtensionProvider + Send + Sync + Unpin + 'static,
        Provider::Extension: Send + Sync + Unpin + 'static,
    {
        let config = self.config;
        listener
            .into_stream()
            .map(move |result| {
                result.map(|(sock, _, addr)| {
                    (
                        WebSocket::from_upgraded(config, sock, None, BytesMut::new(), Role::Server),
                        addr,
                    )
                })
            })
            .boxed()
    }
}

const CHAN_SIZE: usize = 8;

impl TestConnections {
    pub fn new(
        resolve: HashMap<(String, u16), SocketAddr>,
        remotes: HashMap<SocketAddr, DuplexStream>,
        incoming: mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>,
    ) -> (Self, TestConnectionsTask) {
        let (tx, rx) = mpsc::channel(CHAN_SIZE);
        let conn = TestConnections { requests: tx };

        let task = TestConnectionsTask::new(resolve, remotes, incoming, rx);
        (conn, task)
    }
}

pub struct TestListener(BoxedAcc<ListenerResult<(DuplexStream, Scheme, SocketAddr)>>);

impl Debug for TestListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TestListener").finish()
    }
}

impl DnsResolver for TestConnections {
    type ResolveFuture = DnsFut;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Resolve(host, port, tx))
                .await
                .expect("Channel closed.");
            rx.await.expect("Connections task stopped.")
        }
        .boxed()
    }
}

impl ExternalConnections for TestConnections {
    type Socket = DuplexStream;

    type ListenerType = TestListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, Result<(SocketAddr, Self::ListenerType), ConnectionError>> {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Listener(addr, tx))
                .await
                .expect("Channel closed.");
            rx.await
                .expect("Connections task stopped.")
                .map(|listener| (addr, listener))
        }
        .boxed()
    }

    fn try_open(
        &self,
        scheme: Scheme,
        _host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'static, Result<Self::Socket, ConnectionError>> {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Remote(scheme, addr, tx))
                .await
                .expect("Channel closed.");
            rx.await.expect("Connections task stopped.")
        }
        .boxed()
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        self.resolve(host, port)
    }

    fn dns_resolver(&self) -> swimos_remote::dns::BoxDnsResolver {
        Box::new(self.clone())
    }
}

pub type BoxedAcc<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

impl Listener<DuplexStream> for TestListener {
    type AcceptStream = BoxedAcc<ListenerResult<(DuplexStream, Scheme, SocketAddr)>>;

    fn into_stream(self) -> Self::AcceptStream {
        self.0
    }
}

pub struct TestConnectionsTask {
    resolve: HashMap<(String, u16), SocketAddr>,
    remotes: HashMap<SocketAddr, DuplexStream>,
    incoming: Option<mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>>,
    receiver: mpsc::Receiver<ConnReq>,
}

impl TestConnectionsTask {
    fn new(
        resolve: HashMap<(String, u16), SocketAddr>,
        remotes: HashMap<SocketAddr, DuplexStream>,
        incoming: mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>,
        receiver: mpsc::Receiver<ConnReq>,
    ) -> Self {
        TestConnectionsTask {
            resolve,
            remotes,
            incoming: Some(incoming),
            receiver,
        }
    }

    pub async fn run(self) {
        let TestConnectionsTask {
            resolve,
            mut remotes,
            mut incoming,
            mut receiver,
        } = self;
        while let Some(req) = receiver.recv().await {
            match req {
                ConnReq::Remote(_, addr, tx) => {
                    let result = if let Some(stream) = remotes.remove(&addr) {
                        Ok(stream)
                    } else {
                        Err(io::Error::from(ErrorKind::ConnectionAborted).into())
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
                ConnReq::Resolve(host, port, tx) => {
                    let result = if let Some(addr) = resolve.get(&(host, port)).cloned() {
                        Ok(vec![addr])
                    } else {
                        Err(io::Error::from(ErrorKind::NotFound))
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
                ConnReq::Listener(_, tx) => {
                    let result = if let Some(incoming) = incoming.take() {
                        let stream = Box::pin(
                            UnboundedReceiverStream::new(incoming)
                                .map(|(addr, s)| Ok((s, Scheme::Ws, addr))),
                        );
                        let listener = TestListener(stream);
                        Ok(listener)
                    } else {
                        Err(io::Error::from(ErrorKind::AddrInUse).into())
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
            }
        }
    }
}
