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

use crate::routing::error::{ConnectionError, ResolutionError, RouterError, Unresolvable};
use crate::routing::remote::net::{ExternalConnections, Listener};
use crate::routing::remote::ConnectionDropped;
use crate::routing::ws::{CloseReason, JoinedStreamSink, WsConnections};
use crate::routing::{
    Route, RoutingAddr, ServerRouter, ServerRouterFactory, TaggedEnvelope, TaggedSender,
};
use futures::future::{ready, BoxFuture};
use futures::io::ErrorKind;
use futures::stream::Fuse;
use futures::task::{AtomicWaker, Context, Poll};
use futures::{ready, FutureExt, Sink, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use swim_common::sink::{MpscSink, SinkSendError};
use swim_common::ws::WsMessage;
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[derive(Debug)]
struct Entry {
    route: Route<TaggedSender>,
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
    type Sender = TaggedSender;

    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, ResolutionError>> {
        let lock = self.1.lock();
        let result = if let Some(Entry {
            route, countdown, ..
        }) = lock.routes.get(&addr)
        {
            if *countdown == 0 {
                Ok(route.clone())
            } else {
                Err(ResolutionError::Unresolvable(Unresolvable(addr)))
            }
        } else {
            Err(ResolutionError::Unresolvable(Unresolvable(addr)))
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
            Err(RouterError::ConnectionFailure(ConnectionError::Resolution))
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
                    Err(RouterError::ConnectionFailure(ConnectionError::Warp(
                        "Oh no!".to_string(),
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
            let (tx, rx) = mpsc::channel(8);
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
            rx
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

pub struct TwoWayMpsc<T, E> {
    tx: MpscSink<T>,
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
            tx: MpscSink::wrap(tx),
            rx,
            failures: Box::new(failures),
        }
    }

    pub fn no_send_errors<F>(tx: mpsc::Sender<T>, rx: mpsc::Receiver<Result<T, E>>) -> Self {
        TwoWayMpsc {
            tx: MpscSink::wrap(tx),
            rx,
            failures: Box::new(|_| None),
        }
    }
}

impl<T, E> Sink<T> for TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    E: From<SinkSendError<T>>,
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
    E: From<SinkSendError<T>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_next_unpin(cx)
    }
}

impl<T, E> JoinedStreamSink<T, E> for TwoWayMpsc<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    E: From<SinkSendError<T>>,
{
    type CloseFut = BoxFuture<'static, Result<(), E>>;

    fn close(self, _reason: Option<CloseReason>) -> Self::CloseFut {
        ready(Ok(())).boxed()
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

    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        let result = self
            .inner
            .lock()
            .dns
            .get(&host)
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
    type AcceptStream = Fuse<mpsc::Receiver<io::Result<(Self::Socket, SocketAddr)>>>;

    fn into_stream(self) -> Self::AcceptStream {
        let FakeListener(rx) = self;
        rx.fuse()
    }
}

pub(crate) struct FakeWebsockets;

impl WsConnections<FakeSocket> for FakeWebsockets {
    type StreamSink = FakeWebsocket;
    type Fut = BoxFuture<'static, Result<Self::StreamSink, ConnectionError>>;

    fn open_connection(&self, socket: FakeSocket) -> Self::Fut {
        ready(Ok(FakeWebsocket::new(socket))).boxed()
    }

    fn accept_connection(&self, socket: FakeSocket) -> Self::Fut {
        ready(Ok(FakeWebsocket::new(socket))).boxed()
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
            Poll::Ready(Err(ConnectionError::Closed))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
        if self.closed {
            Err(ConnectionError::Closed)
        } else {
            Ok(self.get_mut().inner.output.push(item))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.closed {
            Poll::Ready(Err(ConnectionError::Closed))
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

impl JoinedStreamSink<WsMessage, ConnectionError> for FakeWebsocket {
    type CloseFut = BoxFuture<'static, Result<(), ConnectionError>>;

    fn close(&mut self, _reason: Option<CloseReason>) -> Self::CloseFut {
        let FakeWebsocket { closed, waker, .. } = self;
        *closed = true;
        waker.wake();
        ready(Ok(())).boxed()
    }
}
