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

use crate::routing::error::RouterError;
use crate::routing::remote::net::{ExternalConnections, Listener};
use crate::routing::remote::table::HostAndPort;
use crate::routing::remote::ConnectionDropped;
use crate::routing::{
    Route, RoutingAddr, ServerRouter, ServerRouterFactory, TaggedEnvelope, TaggedSender,
};
use futures::future::{ready, BoxFuture};
use futures::io::ErrorKind;
use futures::stream::Fuse;
use futures::task::{AtomicWaker, Context, Poll};
use futures::{FutureExt, Sink, Stream, StreamExt};
use http::StatusCode;
use parking_lot::Mutex;
use ratchet::{NoExt, WebSocket};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use swim_runtime::error::{
    CloseError, ConnectionError, HttpError, HttpErrorKind, ResolutionError, ResolutionErrorKind,
};
use swim_runtime::ws::{WsConnections, WsMessage};
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

#[derive(Debug)]
struct Entry {
    route: Route,
    on_drop: promise::Sender<ConnectionDropped>,
    countdown: u8,
}

#[derive(Debug, Default)]
pub struct LocalRoutesInner {
    routes: HashMap<RoutingAddr, Entry>,
    uri_mappings: HashMap<RelativeUri, (RoutingAddr, u8)>,
    counter: u32,
}

#[derive(Debug, Clone)]
pub struct LocalRoutes(RoutingAddr, Arc<Mutex<LocalRoutesInner>>);

impl LocalRoutes {
    pub(crate) fn new(owner_addr: RoutingAddr) -> Self {
        LocalRoutes(owner_addr, Default::default())
    }
}

impl ServerRouter for LocalRoutes {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        let lock = self.1.lock();
        let result = if let Some(Entry {
            route, countdown, ..
        }) = lock.routes.get(&addr)
        {
            if *countdown == 0 {
                Ok(route.clone())
            } else {
                Err(ResolutionError::unresolvable(addr.to_string()))
            }
        } else {
            Err(ResolutionError::unresolvable(addr.to_string()))
        };
        ready(result).boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        let mut lock = self.1.lock();
        let result = if host.is_some() {
            Err(RouterError::ConnectionFailure(ConnectionError::Resolution(
                ResolutionError::new(ResolutionErrorKind::Unresolvable, None),
            )))
        } else {
            if let Some((addr, countdown)) = lock.uri_mappings.get_mut(&route) {
                if *countdown == 0 {
                    Ok(*addr)
                } else {
                    *countdown -= 1;
                    let addr = *addr;
                    if let Some(Entry { countdown, .. }) = lock.routes.get_mut(&addr) {
                        *countdown -= 1;
                    }
                    // A non-fatal error that will allow a retry.
                    Err(RouterError::ConnectionFailure(ConnectionError::Http(
                        HttpError::new(HttpErrorKind::StatusCode(Some(StatusCode::OK)), None),
                    )))
                }
            } else {
                Err(RouterError::NoAgentAtRoute(route))
            }
        };
        ready(result).boxed()
    }
}

impl LocalRoutes {
    pub fn add_with_countdown(
        &self,
        uri: RelativeUri,
        countdown: u8,
    ) -> mpsc::Receiver<TaggedEnvelope> {
        let (tx, rx) = mpsc::channel(8);
        self.add_sender_with_countdown(uri, tx, countdown);
        rx
    }

    pub fn add_sender(&self, uri: RelativeUri, tx: mpsc::Sender<TaggedEnvelope>) {
        self.add_sender_with_countdown(uri, tx, 0);
    }

    fn add_sender_with_countdown(
        &self,
        uri: RelativeUri,
        tx: mpsc::Sender<TaggedEnvelope>,
        countdown: u8,
    ) {
        let LocalRoutes(owner_addr, inner) = self;
        let LocalRoutesInner {
            routes,
            uri_mappings,
            counter,
        } = &mut *inner.lock();
        if uri_mappings.contains_key(&uri) {
            panic!("Duplicate registration.");
        } else {
            let id = RoutingAddr::local(*counter);
            *counter += 1;
            uri_mappings.insert(uri, (id, countdown));
            let (drop_tx, drop_rx) = promise::promise();
            let route = Route::new(TaggedSender::new(*owner_addr, tx), drop_rx);
            routes.insert(
                id,
                Entry {
                    route,
                    on_drop: drop_tx,
                    countdown,
                },
            );
        }
    }

    pub fn add(&self, uri: RelativeUri) -> mpsc::Receiver<TaggedEnvelope> {
        self.add_with_countdown(uri, 0)
    }

    pub fn remove(&self, uri: RelativeUri) -> promise::Sender<ConnectionDropped> {
        let LocalRoutesInner {
            routes,
            uri_mappings,
            ..
        } = &mut *self.1.lock();
        let Entry { on_drop, .. } = uri_mappings
            .remove(&uri)
            .and_then(|(id, _)| routes.remove(&id))
            .unwrap();
        on_drop
    }
}

impl ServerRouterFactory for LocalRoutes {
    type Router = LocalRoutes;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        let LocalRoutes(_, inner) = self;
        LocalRoutes(addr, inner.clone())
    }
}

pub mod fake_channel {

    use futures::channel::mpsc;
    use futures::{ready, Sink, SinkExt, Stream, StreamExt};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    pub struct TwoWayMpsc<T, E> {
        tx: mpsc::Sender<T>,
        rx: mpsc::Receiver<Result<T, E>>,
        failures: Box<dyn Fn(&T) -> Option<E> + Send + Unpin>,
    }

    impl<T, E> TwoWayMpsc<T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        pub fn new<F>(tx: mpsc::Sender<T>, rx: mpsc::Receiver<Result<T, E>>, failures: F) -> Self
        where
            F: Fn(&T) -> Option<E> + Send + Unpin + 'static,
        {
            TwoWayMpsc {
                tx,
                rx,
                failures: Box::new(failures),
            }
        }
    }

    impl<T, E> Sink<T> for TwoWayMpsc<T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
        E: From<mpsc::SendError>,
    {
        type Error = E;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(ready!(self.get_mut().tx.poll_ready_unpin(cx))?))
        }

        fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
            if let Some(err) = (self.as_ref().get_ref().failures)(&item) {
                Err(err)
            } else {
                self.get_mut().tx.start_send_unpin(item)?;
                Ok(())
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(ready!(self.get_mut().tx.poll_flush_unpin(cx))?))
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(ready!(self.get_mut().tx.poll_close_unpin(cx))?))
        }
    }

    impl<T, E> Stream for TwoWayMpsc<T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
        E: From<mpsc::SendError>,
    {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.get_mut().rx.poll_next_unpin(cx)
        }
    }
}

#[derive(Debug, Default)]
pub struct FakeSocket {
    input: Vec<WsMessage>,
    offset_in: usize,
    stop_when_exhausted: bool,
    output: Vec<WsMessage>,
}

impl FakeSocket {
    pub fn new(data: Vec<WsMessage>, initial_out_cap: usize, stop_when_exhausted: bool) -> Self {
        FakeSocket {
            input: data,
            offset_in: 0,
            stop_when_exhausted,
            output: Vec::with_capacity(initial_out_cap),
        }
    }

    pub fn trivial() -> Self {
        Self::new(vec![], 0, true)
    }

    pub fn duplicate(&self) -> Self {
        let FakeSocket {
            input,
            stop_when_exhausted,
            ..
        } = self;
        FakeSocket {
            input: input.clone(),
            offset_in: 0,
            stop_when_exhausted: *stop_when_exhausted,
            output: vec![],
        }
    }
}

#[derive(Debug)]
struct FakeConnectionsInner {
    sockets: HashMap<SocketAddr, Result<FakeSocket, io::Error>>,
    incoming: Option<FakeListener>,
    dns: HashMap<String, Vec<SocketAddr>>,
}

#[derive(Debug, Clone)]
pub struct FakeConnections {
    inner: Arc<Mutex<FakeConnectionsInner>>,
}

impl FakeConnections {
    pub fn new(
        sockets: HashMap<SocketAddr, Result<FakeSocket, io::Error>>,
        dns: HashMap<String, Vec<SocketAddr>>,
        incoming: Option<mpsc::Receiver<io::Result<(FakeSocket, SocketAddr)>>>,
    ) -> Self {
        FakeConnections {
            inner: Arc::new(Mutex::new(FakeConnectionsInner {
                sockets,
                incoming: incoming.map(FakeListener),
                dns,
            })),
        }
    }

    pub fn add_dns(&self, host: String, sock_addr: SocketAddr) {
        self.inner.lock().dns.insert(host, vec![sock_addr]);
    }

    pub fn add_socket(&self, sock_addr: SocketAddr, socket: FakeSocket) {
        self.inner.lock().sockets.insert(sock_addr, Ok(socket));
    }

    pub fn add_error(&self, sock_addr: SocketAddr, err: io::Error) {
        self.inner.lock().sockets.insert(sock_addr, Err(err));
    }
}

impl ExternalConnections for FakeConnections {
    type Socket = FakeSocket;
    type ListenerType = FakeListener;

    fn bind(&self, _addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::ListenerType>> {
        let result = self
            .inner
            .lock()
            .incoming
            .take()
            .map(Ok)
            .unwrap_or(Err(ErrorKind::AddrNotAvailable.into()));
        ready(result).boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, io::Result<Self::Socket>> {
        let result = self
            .inner
            .lock()
            .sockets
            .remove(&addr)
            .unwrap_or(Err(ErrorKind::NotFound.into()));
        ready(result).boxed()
    }

    fn lookup(
        &self,
        host_and_port: HostAndPort,
    ) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        let result = self
            .inner
            .lock()
            .dns
            .get(&host_and_port.to_string())
            .map(Clone::clone)
            .map(Ok)
            .unwrap_or(Err(ErrorKind::NotFound.into()));
        ready(result).boxed()
    }
}

#[derive(Debug)]
pub struct FakeListener(mpsc::Receiver<io::Result<(FakeSocket, SocketAddr)>>);

impl FakeListener {
    pub fn new(rx: mpsc::Receiver<io::Result<(FakeSocket, SocketAddr)>>) -> Self {
        FakeListener(rx)
    }
}

impl Listener for FakeListener {
    type Socket = FakeSocket;
    type AcceptStream = Fuse<ReceiverStream<io::Result<(Self::Socket, SocketAddr)>>>;

    fn into_stream(self) -> Self::AcceptStream {
        let FakeListener(rx) = self;
        ReceiverStream::new(rx).fuse()
    }
}

pub(crate) struct FakeWebsockets;

impl WsConnections<FakeSocket> for FakeWebsockets {
    type Ext = NoExt;
    type Error = FakeErr;

    fn open_connection(
        &self,
        socket: FakeSocket,
        _host: String,
    ) -> BoxFuture<Result<WebSocket<FakeSocket, Self::Ext>, Self::Error>> {
        unimplemented!()
        // let ws = WebSocket::from_upgraded(
        //     WebSocketConfig::default(),
        //     socket,
        //     NegotiatedExtension::from(Some(NoExt)),
        //     BytesMut::default(),
        //     Role::Client,
        // );
        //
        // ready(Ok(ws)).boxed()
    }

    fn accept_connection(
        &self,
        socket: FakeSocket,
    ) -> BoxFuture<Result<WebSocket<FakeSocket, Self::Ext>, Self::Error>> {
        unimplemented!()
        // let ws = WebSocket::from_upgraded(
        //     WebSocketConfig::default(),
        //     socket,
        //     NegotiatedExtension::from(Some(NoExt)),
        //     BytesMut::default(),
        //     Role::Server,
        // );
        //
        // ready(Ok(ws)).boxed()
    }
}

pub struct FakeErr;
impl From<FakeErr> for ConnectionError {
    fn from(_: FakeErr) -> Self {
        panic!("Unexpected error")
    }
}

#[derive(Debug)]
pub struct FakeWebsocket {
    inner: FakeSocket,
    closed: bool,
    waker: AtomicWaker,
}

impl FakeWebsocket {
    pub fn new(socket: FakeSocket) -> Self {
        FakeWebsocket {
            inner: socket,
            closed: false,
            waker: AtomicWaker::default(),
        }
    }
}

impl Stream for FakeWebsocket {
    type Item = Result<WsMessage, ConnectionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let FakeWebsocket {
            inner,
            closed,
            waker,
        } = self.get_mut();
        if *closed {
            Poll::Ready(None)
        } else {
            let FakeSocket {
                input,
                offset_in,
                stop_when_exhausted,
                ..
            } = inner;
            let result = input.get(*offset_in).map(Clone::clone).map(Ok);
            if result.is_some() {
                *offset_in += 1;
                Poll::Ready(result)
            } else if *stop_when_exhausted {
                Poll::Ready(result)
            } else {
                waker.register(cx.waker());
                Poll::Pending
            }
        }
    }
}

impl Sink<WsMessage> for FakeWebsocket {
    type Error = ConnectionError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            Poll::Ready(Err(ConnectionError::Closed(CloseError::closed())))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
        if self.closed {
            Err(ConnectionError::Closed(CloseError::closed()))
        } else {
            Ok(self.get_mut().inner.output.push(item))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            Poll::Ready(Err(ConnectionError::Closed(CloseError::closed())))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let FakeWebsocket { closed, waker, .. } = self.get_mut();
        *closed = true;
        waker.wake();
        Poll::Ready(Ok(()))
    }
}
