// Copyright 2015-2021 Swim Inc.
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
use futures::{future::BoxFuture, stream::Fuse, FutureExt, Stream, StreamExt};
use ratchet::{NegotiatedExtension, NoExt, Role, WebSocket, WebSocketConfig};
use swim_runtime::error::ConnectionError;
use swim_runtime::remote::Scheme;
use swim_runtime::remote::{
    table::SchemeHostPort, ExternalConnections, Listener, SchemeSocketAddr,
};
use swim_runtime::ws::{WsConnections, WsOpenFuture};
use tokio::{
    io::{self, DuplexStream},
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug)]
enum ConnReq {
    Remote(SocketAddr, oneshot::Sender<io::Result<DuplexStream>>),
    Resolve(
        SchemeHostPort,
        oneshot::Sender<io::Result<Vec<SchemeSocketAddr>>>,
    ),
    Listener(SocketAddr, oneshot::Sender<io::Result<TestListener>>),
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

impl WsConnections<DuplexStream> for TestWs {
    type Ext = NoExt;

    type Error = ConnectionError;

    fn open_connection(
        &self,
        socket: DuplexStream,
        _addr: String,
    ) -> WsOpenFuture<DuplexStream, Self::Ext, Self::Error> {
        ready(Ok(WebSocket::from_upgraded(
            self.config,
            socket,
            NegotiatedExtension::from(None),
            BytesMut::new(),
            Role::Client,
        )))
        .boxed()
    }

    fn accept_connection(
        &self,
        socket: DuplexStream,
    ) -> WsOpenFuture<DuplexStream, Self::Ext, Self::Error> {
        ready(Ok(WebSocket::from_upgraded(
            self.config,
            socket,
            NegotiatedExtension::from(None),
            BytesMut::new(),
            Role::Server,
        )))
        .boxed()
    }
}

const CHAN_SIZE: usize = 8;

impl TestConnections {
    pub fn new(
        resolve: HashMap<SchemeHostPort, SocketAddr>,
        remotes: HashMap<SocketAddr, DuplexStream>,
        incoming: mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>,
    ) -> (Self, TestConnectionsTask) {
        let (tx, rx) = mpsc::channel(CHAN_SIZE);
        let conn = TestConnections { requests: tx };

        let task = TestConnectionsTask::new(resolve, remotes, incoming, rx);
        (conn, task)
    }
}

pub struct TestListener(BoxedAcc<io::Result<(DuplexStream, SchemeSocketAddr)>>);

impl Debug for TestListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TestListener").finish()
    }
}

impl ExternalConnections for TestConnections {
    type Socket = DuplexStream;

    type ListenerType = TestListener;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::ListenerType>> {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Listener(addr, tx))
                .await
                .expect("Channel closed.");
            rx.await.expect("Connections task stopped.")
        }
        .boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>> {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Remote(addr, tx))
                .await
                .expect("Channel closed.");
            rx.await.expect("Connections task stopped.")
        }
        .boxed()
    }

    fn lookup(
        &self,
        host: SchemeHostPort,
    ) -> BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>> {
        let sender = self.requests.clone();
        async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(ConnReq::Resolve(host, tx))
                .await
                .expect("Channel closed.");
            rx.await.expect("Connections task stopped.")
        }
        .boxed()
    }
}

pub type BoxedAcc<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

impl Listener for TestListener {
    type Socket = DuplexStream;

    type AcceptStream = Fuse<BoxedAcc<io::Result<(Self::Socket, SchemeSocketAddr)>>>;

    fn into_stream(self) -> Self::AcceptStream {
        self.0.fuse()
    }
}

pub struct TestConnectionsTask {
    resolve: HashMap<SchemeHostPort, SocketAddr>,
    remotes: HashMap<SocketAddr, DuplexStream>,
    incoming: Option<mpsc::UnboundedReceiver<(SocketAddr, DuplexStream)>>,
    receiver: mpsc::Receiver<ConnReq>,
}

impl TestConnectionsTask {
    fn new(
        resolve: HashMap<SchemeHostPort, SocketAddr>,
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
                ConnReq::Remote(addr, tx) => {
                    let result = if let Some(stream) = remotes.remove(&addr) {
                        Ok(stream)
                    } else {
                        Err(io::Error::from(ErrorKind::ConnectionAborted))
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
                ConnReq::Resolve(host, tx) => {
                    let result = if let Some(addr) = resolve.get(&host).cloned() {
                        Ok(vec![SchemeSocketAddr::new(Scheme::Ws, addr)])
                    } else {
                        Err(io::Error::from(ErrorKind::NotFound))
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
                ConnReq::Listener(_, tx) => {
                    let result = if let Some(incoming) = incoming.take() {
                        let stream = Box::pin(
                            UnboundedReceiverStream::new(incoming)
                                .map(|(addr, s)| Ok((s, SchemeSocketAddr::new(Scheme::Ws, addr)))),
                        );
                        let listener = TestListener(stream);
                        Ok(listener)
                    } else {
                        Err(io::Error::from(ErrorKind::AddrInUse))
                    };
                    tx.send(result).expect("Oneshot dropped.");
                }
            }
        }
    }
}
