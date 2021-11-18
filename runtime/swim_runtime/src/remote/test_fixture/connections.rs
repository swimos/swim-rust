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

use crate::error::ConnectionError;
use crate::remote::table::SchemeHostPort;
use crate::remote::{ExternalConnections, Listener, Scheme, SchemeSocketAddr};
use crate::ws::{WsConnections, WsOpenFuture};
use bytes::BytesMut;
use futures::stream::Fuse;
use futures::FutureExt;
use futures_util::future::{ready, BoxFuture};
use futures_util::StreamExt;
use parking_lot::Mutex;
use ratchet::{NegotiatedExtension, NoExt, Role, WebSocket, WebSocketConfig};
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone)]
pub enum ErrorMode {
    /// Return an error when sending messages.
    Send,
    /// Return an error when receiving messages.
    Receive,
    /// Do not return any errors.
    None,
}

#[derive(Debug)]
struct FakeConnectionsInner {
    sockets: HashMap<SchemeSocketAddr, Result<DuplexStream, io::Error>>,
    incoming: Option<FakeListener>,
    dns: HashMap<String, Vec<SchemeSocketAddr>>,
}

#[derive(Debug, Clone)]
pub struct FakeConnections {
    inner: Arc<Mutex<FakeConnectionsInner>>,
    // The count of open requests that will return error before it starts handling them normally.
    open_error_count: Arc<AtomicIsize>,
}

impl FakeConnections {
    pub fn new(
        sockets: HashMap<SchemeSocketAddr, Result<DuplexStream, io::Error>>,
        dns: HashMap<String, Vec<SchemeSocketAddr>>,
        incoming: Option<mpsc::Receiver<io::Result<(DuplexStream, SchemeSocketAddr)>>>,
        open_error_count: usize,
    ) -> Self {
        FakeConnections {
            inner: Arc::new(Mutex::new(FakeConnectionsInner {
                sockets,
                incoming: incoming.map(FakeListener),
                dns,
            })),
            open_error_count: Arc::new(AtomicIsize::new(open_error_count as isize)),
        }
    }

    pub fn add_dns(&self, host: String, sock_addr: SchemeSocketAddr) {
        self.inner.lock().dns.insert(host, vec![sock_addr]);
    }

    pub fn add_socket(&self, sock_addr: SchemeSocketAddr, socket: DuplexStream) {
        self.inner.lock().sockets.insert(sock_addr, Ok(socket));
    }

    pub fn add_error(&self, sock_addr: SchemeSocketAddr, err: io::Error) {
        self.inner.lock().sockets.insert(sock_addr, Err(err));
    }
}

impl ExternalConnections for FakeConnections {
    type Socket = DuplexStream;
    type ListenerType = FakeListener;

    fn bind(&self, _addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::ListenerType>> {
        let result = self
            .inner
            .lock()
            .incoming
            .take()
            .map(Ok)
            .unwrap_or_else(|| Err(ErrorKind::AddrNotAvailable.into()));
        ready(result).boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>> {
        let count = self.open_error_count.fetch_sub(1, Ordering::AcqRel);
        if count > 0 {
            return ready(Err(io::Error::new(ErrorKind::InvalidInput, "Test Error"))).boxed();
        }

        let result = self
            .inner
            .lock()
            .sockets
            .remove(&SchemeSocketAddr::new(Scheme::Ws, addr))
            .unwrap_or_else(|| Err(ErrorKind::NotFound.into()));
        ready(result).boxed()
    }

    fn lookup(
        &self,
        host_and_port: SchemeHostPort,
    ) -> BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>> {
        let result = self
            .inner
            .lock()
            .dns
            .get(&host_and_port.to_string())
            .map(Clone::clone)
            .map(Ok)
            .unwrap_or_else(|| Err(ErrorKind::NotFound.into()));
        ready(result).boxed()
    }
}

#[derive(Debug)]
pub struct FakeListener(mpsc::Receiver<io::Result<(DuplexStream, SchemeSocketAddr)>>);

impl FakeListener {
    pub fn new(rx: mpsc::Receiver<io::Result<(DuplexStream, SchemeSocketAddr)>>) -> Self {
        FakeListener(rx)
    }
}

impl Listener for FakeListener {
    type Socket = DuplexStream;
    #[allow(clippy::type_complexity)]
    type AcceptStream = Fuse<ReceiverStream<io::Result<(Self::Socket, SchemeSocketAddr)>>>;

    fn into_stream(self) -> Self::AcceptStream {
        let FakeListener(rx) = self;
        ReceiverStream::new(rx).fuse()
    }
}

pub struct FakeWebsockets;

impl WsConnections<DuplexStream> for FakeWebsockets {
    type Ext = NoExt;
    type Error = ConnectionError;

    fn open_connection(
        &self,
        socket: DuplexStream,
        _addr: String,
    ) -> WsOpenFuture<DuplexStream, Self::Ext, Self::Error> {
        ready(Ok(WebSocket::from_upgraded(
            WebSocketConfig {
                max_message_size: usize::MAX,
            },
            socket,
            NegotiatedExtension::from(NoExt),
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
            WebSocketConfig {
                max_message_size: usize::MAX,
            },
            socket,
            NegotiatedExtension::from(NoExt),
            BytesMut::new(),
            Role::Server,
        )))
        .boxed()
    }
}
