// Copyright 2015-2023 Swim Inc.
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

use std::{
    marker::PhantomData,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::{
    future::{ready, Ready},
    ready,
    stream::{BoxStream, FuturesUnordered},
    Future, FutureExt, Stream, StreamExt,
};
use hyper::{
    server::conn::http1,
    service::Service,
    upgrade::{Parts, Upgraded},
    Body, Request, Response,
};
use pin_project::pin_project;
use ratchet::{
    Extension, ExtensionProvider, ProtocolRegistry, WebSocket, WebSocketConfig, WebSocketStream,
};
use swim_http::{Negotiated, SockUnwrap, UpgradeError, UpgradeFuture};
use swim_runtime::{
    net::{Listener, ListenerError, ListenerResult, Scheme},
    ws::{RatchetError, WebsocketClient, WebsocketServer, WsOpenFuture, PROTOCOLS},
};
use tokio::sync::mpsc::{self, OwnedPermit};

#[cfg(test)]
mod tests;

pub type WsWithAddr<Ext, Sock> = (WebSocket<Sock, Ext>, Scheme, SocketAddr);
pub type ListenResult<Ext, Sock> = Result<WsWithAddr<Ext, Sock>, ListenerError>;

/// Hyper based web-server that will attempt to negotiate a server websocket over
/// every incoming connection.
///
/// # Arguments
/// * `listener` - Listener providing a stream of incoming connections.
/// * `extension_provider` - Web socket extension provider.
/// * `config` - Web socket configuration parameters.
/// * `max_negotiations` - Maximum number of concurrent websocket handshakes. If this many are
/// pending, no more connections will be accepted from the listener until a space becomes free.
pub fn hyper_http_server<Sock, L, Ext>(
    listener: L,
    extension_provider: Ext,
    config: Option<WebSocketConfig>,
    max_negotiations: NonZeroUsize,
) -> impl Stream<Item = ListenResult<Ext::Extension, Sock>> + Send
where
    Sock: WebSocketStream + Send + Sync,
    L: Listener<Sock> + Send,
    Ext: ExtensionProvider + Send + Sync + 'static,
    Ext::Extension: Send + Unpin,
{
    let state = HttpServerState::<L::AcceptStream, Sock, Ext, _, _, _, _>::new(
        listener.into_stream(),
        extension_provider,
        config,
        max_negotiations,
        mpsc::Sender::reserve_owned,
        |sock, svc| {
            http1::Builder::new()
                .serve_connection(sock, svc)
                .with_upgrades()
        },
    );

    futures::stream::unfold(state, |mut state| async move {
        state.next().await.map(move |result| (result, state))
    })
}

enum TaskResult<Ext, Sock> {
    ConnectionComplete(Result<(), hyper::Error>),
    UpgradeComplete(Result<(WebSocket<Sock, Ext>, Scheme, SocketAddr), hyper::Error>),
    Reserved(Result<OwnedPermit<UpgradeFutureWithSock<Ext, Sock>>, mpsc::error::SendError<()>>),
}

#[pin_project(project = TaskFutureProj)]
enum TaskFuture<Sock, Ext, Con, Fut>
where
    Sock: WebSocketStream,
    Ext: ExtensionProvider,
    Ext::Extension: Send,
{
    Connection(Con),
    Upgrade(UpgradeFutureWithSock<Ext::Extension, Sock>),
    Reserve(#[pin] Fut),
}

impl<Sock, Ext, Con, Fut> Future for TaskFuture<Sock, Ext, Con, Fut>
where
    Sock: WebSocketStream,
    Ext: ExtensionProvider,
    Ext::Extension: Send + Unpin,
    Con: Future<Output = Result<(), hyper::Error>> + Unpin,
    Fut: Future<
        Output = Result<
            OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>,
            mpsc::error::SendError<()>,
        >,
    >,
{
    type Output = TaskResult<Ext::Extension, Sock>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            TaskFutureProj::Connection(fut) => {
                Poll::Ready(TaskResult::ConnectionComplete(ready!(fut.poll_unpin(cx))))
            }
            TaskFutureProj::Upgrade(fut) => {
                Poll::Ready(TaskResult::UpgradeComplete(ready!(fut.poll_unpin(cx))))
            }
            TaskFutureProj::Reserve(fut) => Poll::Ready(TaskResult::Reserved(ready!(fut.poll(cx)))),
        }
    }
}

enum ReservationTracker<F> {
    Closed,
    Reserving(mpsc::Sender<F>),
    Reserved(mpsc::Sender<F>, mpsc::OwnedPermit<F>),
}

impl<F> ReservationTracker<F> {
    fn is_closed(&self) -> bool {
        matches!(self, ReservationTracker::Closed)
    }

    fn new<Fut>(tx: mpsc::Sender<F>, f: impl Fn(mpsc::Sender<F>) -> Fut) -> (Self, Option<Fut>) {
        match tx.clone().try_reserve_owned() {
            Ok(r) => (ReservationTracker::Reserved(tx, r), None),
            Err(mpsc::error::TrySendError::Closed(_)) => (ReservationTracker::Closed, None),
            Err(mpsc::error::TrySendError::Full(tx2)) => {
                (ReservationTracker::Reserving(tx), Some(f(tx2)))
            }
        }
    }

    fn add_reservation(&mut self, res: mpsc::OwnedPermit<F>) {
        *self = match std::mem::replace(self, ReservationTracker::Closed) {
            ReservationTracker::Closed => ReservationTracker::Closed,
            ReservationTracker::Reserving(tx) => ReservationTracker::Reserved(tx, res),
            ReservationTracker::Reserved(tx, r) => ReservationTracker::Reserved(tx, r),
        };
    }

    fn take_reservation(&mut self) -> Option<mpsc::OwnedPermit<F>> {
        let (replacement, result) = match std::mem::replace(self, ReservationTracker::Closed) {
            ReservationTracker::Closed => (ReservationTracker::Closed, None),
            ReservationTracker::Reserving(tx) => (ReservationTracker::Reserving(tx), None),
            ReservationTracker::Reserved(tx, r) => (ReservationTracker::Reserving(tx), Some(r)),
        };
        *self = replacement;
        result
    }

    fn update_reservation<Fut>(&mut self, f: impl Fn(mpsc::Sender<F>) -> Fut) -> Option<Fut> {
        match std::mem::replace(self, ReservationTracker::Closed) {
            ReservationTracker::Reserving(tx) => match tx.clone().try_reserve_owned() {
                Ok(res) => {
                    *self = ReservationTracker::Reserved(tx, res);
                    None
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    *self = ReservationTracker::Closed;
                    None
                }
                Err(mpsc::error::TrySendError::Full(tx2)) => {
                    *self = ReservationTracker::Reserving(tx);
                    Some(f(tx2))
                }
            },
            ow => {
                *self = ow;
                None
            }
        }
    }
}

/// Represents the internal state of the embedded HTTP server. This is used to create an instance
/// of [`Stream`], using [`futures::stream::unfold`], that yields negotiated web-socket connections (or
/// errors when connections fail).
///
/// Only a fixed number of web-socket handshakes are permitted to be running at any one time. This is
/// managed used an [`tokio::sync::mpsc`] channel; new incoming sockets will only be accepted if the
/// state holds a permit to push a new entry into this queue. When a response has been produced, the
/// slot in the queue will be freed.
///
/// #Type Parameters
/// * `L` - The type of the listener for incoming connections.
/// * `Sock` - The type of the connections produced by the listener.
/// * `Ext` - The websocket extension provider for negotiating connections.
/// * `Con` - The type of the future that handles an HTTP connection (this parameter exists to
/// give a name to the return type of an async function).
/// * `Fut` - The type of the future to wait for a reservation on the queue to become available.
/// (this parameter exists to give a name to the return type of an async function).
/// * `FR` - A function to produce a future to get a reservation on the queue (this parameter
/// exists to give a name to the type of an async function).
/// * `FC` - A function to produce the future to handle an HTTP connection (this parameter exists to
/// give a name to the type of an async function).
struct HttpServerState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: WebSocketStream,
    Ext: ExtensionProvider,
    Ext::Extension: Send,
{
    listener_stream: L,
    connection_tasks: FuturesUnordered<TaskFuture<Sock, Ext, Con, Fut>>,
    upgrader: Upgrader<Ext>,
    reservation_tracker: ReservationTracker<UpgradeFutureWithSock<Ext::Extension, Sock>>,
    upgrade_rx: mpsc::Receiver<UpgradeFutureWithSock<Ext::Extension, Sock>>,
    reserve_fn: FR,
    connect_fn: FC,
}

impl<L, Sock, Ext, Con, Fut, FR, FC> HttpServerState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: WebSocketStream,
    Ext: ExtensionProvider + Send + Sync,
    Ext::Extension: Send,
    FR: Fn(mpsc::Sender<UpgradeFutureWithSock<Ext::Extension, Sock>>) -> Fut
        + Copy
        + Send
        + 'static,
    FC: Fn(Sock, UpgradeService<Ext, Sock>) -> Con + Copy + Send + 'static,
{
    /// #Arguments
    /// * `listener_stream` - A listener that produced a stream of incoming connections.
    /// * `extension_provider` - Extension provider to use when negotiating websocket connections.
    /// * `config` - Configuration parameters for websocket connections.
    /// * `max_negotiations` - Maximum number of connections to be accepting at one time.
    /// * `reserve_fn` - Async function to get a new reservation in the queue.
    /// * `connect_fn` - Async function to handle an incoming HTTP connection.
    fn new(
        listener_stream: L,
        extension_provider: Ext,
        config: Option<WebSocketConfig>,
        max_negotiations: NonZeroUsize,
        reserve_fn: FR,
        connect_fn: FC,
    ) -> Self {
        let (upgrade_tx, upgrade_rx) = mpsc::channel(max_negotiations.get());
        let connection_tasks = FuturesUnordered::new();
        let (reservable, fut) = ReservationTracker::new(upgrade_tx, reserve_fn);
        if let Some(fut) = fut {
            connection_tasks.push(TaskFuture::Reserve(fut));
        };
        HttpServerState {
            listener_stream,
            connection_tasks,
            upgrader: Upgrader::new(extension_provider, config),
            reservation_tracker: reservable,
            upgrade_rx,
            reserve_fn,
            connect_fn,
        }
    }
}

enum Event<Sock, Ext> {
    Upgrade(UpgradeFutureWithSock<Ext, Sock>),
    TaskComplete(TaskResult<Ext, Sock>),
    Incoming(
        Sock,
        Scheme,
        SocketAddr,
        mpsc::OwnedPermit<UpgradeFutureWithSock<Ext, Sock>>,
    ),
    IncomingFailed(ListenerError),
    Continue,
    Stop,
}

impl<L, Sock, Ext, Con, Fut, FR, FC> HttpServerState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: WebSocketStream,
    L: Stream<Item = ListenerResult<(Sock, Scheme, SocketAddr)>> + Send + Unpin,
    Ext: ExtensionProvider + Send + Sync,
    Ext::Extension: Send + Unpin,
    Con: Future<Output = Result<(), hyper::Error>> + Unpin,
    Fut: Future<
        Output = Result<
            OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>,
            mpsc::error::SendError<()>,
        >,
    >,
    FR: Fn(mpsc::Sender<UpgradeFutureWithSock<Ext::Extension, Sock>>) -> Fut
        + Copy
        + Send
        + 'static,
    FC: Fn(Sock, UpgradeService<Ext, Sock>) -> Con + Copy + Send + 'static,
{
    async fn next(&mut self) -> Option<ListenResult<Ext::Extension, Sock>> {
        let HttpServerState {
            listener_stream,
            connection_tasks,
            upgrader,
            reservation_tracker,
            upgrade_rx,
            reserve_fn,
            connect_fn,
        } = self;
        loop {
            if reservation_tracker.is_closed() {
                return None;
            }
            let event = if let Some(res) = reservation_tracker.take_reservation() {
                // We have a reservation so we can wait for new incoming connections.
                tokio::select! {
                    biased;
                    maybe_fut = upgrade_rx.recv() => {
                        // An handshake has completed and we can now wait to receive the the upgraded channel.
                        // We don't need the reservation we can return it.
                        reservation_tracker.add_reservation(res);
                        if let Some(fut) = maybe_fut {
                            Event::Upgrade(fut)
                        } else {
                            Event::Continue
                        }
                    }
                    maybe_event = connection_tasks.next(), if !connection_tasks.is_empty() => {
                        // An HTTP request has completed (the response has been sent). We don't need
                        // the reservation so we can return it.
                        reservation_tracker.add_reservation(res);
                        if let Some(ev) = maybe_event {
                            Event::TaskComplete(ev)
                        } else {
                            Event::Continue
                        }
                    }
                    maybe_incoming = listener_stream.next() => {
                        // Either we have a new incoming connection or the listener is closed.
                        match maybe_incoming {
                            Some(Ok((sock, scheme, addr))) => {
                                if let Some(fut) = reservation_tracker.update_reservation(*reserve_fn) {
                                    connection_tasks.push(TaskFuture::Reserve(fut));
                                }
                                Event::Incoming(sock, scheme, addr, res)
                            },
                            Some(Err(err)) => {
                                reservation_tracker.add_reservation(res);
                                Event::IncomingFailed(err)
                            },
                            _ => {
                                // The listener is closed so wen should stop handling connections
                                *reservation_tracker = ReservationTracker::Closed;
                                Event::Stop
                            },
                        }
                    }
                }
            } else {
                // We don't have a reservation to start a new connection so only wait for existing tasks
                // to complete.
                tokio::select! {
                    biased;
                    maybe_fut = upgrade_rx.recv() => {
                        if let Some(fut) = maybe_fut {
                            Event::Upgrade(fut)
                        } else {
                            Event::Continue
                        }
                    }
                    maybe_event = connection_tasks.next(), if !connection_tasks.is_empty() => {
                        if let Some(ev) = maybe_event {
                            Event::TaskComplete(ev)
                        } else {
                            Event::Continue
                        }
                    }
                }
            };
            match event {
                Event::Upgrade(fut) => {
                    connection_tasks.push(TaskFuture::Upgrade(fut));
                }
                Event::TaskComplete(TaskResult::ConnectionComplete(Err(err))) => {
                    break Some(Err(ListenerError::NegotiationFailed(Box::new(err))));
                }
                Event::TaskComplete(TaskResult::ConnectionComplete(Ok(_))) => continue,
                Event::TaskComplete(TaskResult::Reserved(Ok(res))) => {
                    reservation_tracker.add_reservation(res);
                    continue;
                }
                Event::TaskComplete(TaskResult::Reserved(Err(_))) => break None,
                Event::TaskComplete(TaskResult::UpgradeComplete(Ok((ws, scheme, addr)))) => {
                    break Some(Ok((ws, scheme, addr)));
                }
                Event::TaskComplete(TaskResult::UpgradeComplete(Err(err))) => {
                    break Some(Err(ListenerError::NegotiationFailed(Box::new(err))));
                }
                Event::Incoming(sock, scheme, addr, res) => {
                    let svc = upgrader.make_service(scheme, addr, res);
                    connection_tasks.push(TaskFuture::Connection(connect_fn(sock, svc)));
                    continue;
                }
                Event::IncomingFailed(err) => break Some(Err(err)),
                Event::Continue => continue,
                Event::Stop => break None,
            }
        }
    }
}

/// Perform the websocket negotiation and send the resulting future back, freeing up the reservation
/// on the queue.
fn perform_upgrade<Ext, Sock, Err>(
    request: Request<Body>,
    config: Option<WebSocketConfig>,
    result: Result<Option<Negotiated<'_, Ext>>, UpgradeError<Err>>,
    reservation: Option<OwnedPermit<UpgradeFutureWithSock<Ext, Sock>>>,
    scheme: Scheme,
    addr: SocketAddr,
) -> Result<Response<Body>, hyper::Error>
where
    Sock: WebSocketStream,
    Ext: Extension + Send,
    Err: std::error::Error + Send,
{
    match result {
        Ok(Some(negotiated)) => {
            let (response, upgrade_fut) =
                swim_http::upgrade(request, negotiated, config, ReclaimSock::<Sock>::default());
            if let Some(reservation) = reservation {
                reservation.send(UpgradeFutureWithSock::new(upgrade_fut, scheme, addr));
            }
            Ok(response)
        }
        Ok(None) => todo!(),
        Err(err) => Ok(swim_http::fail_upgrade(err)),
    }
}

/// A factory for hyper services that perform websocket upgrades.
struct Upgrader<Ext: ExtensionProvider> {
    extension_provider: Arc<Ext>,
    config: Option<WebSocketConfig>,
}

impl<Ext> Upgrader<Ext>
where
    Ext: ExtensionProvider + Send + Sync,
{
    fn new(extension_provider: Ext, config: Option<WebSocketConfig>) -> Self {
        Upgrader {
            extension_provider: Arc::new(extension_provider),
            config,
        }
    }

    fn make_service<Sock>(
        &self,
        scheme: Scheme,
        addr: SocketAddr,
        reservation: OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>,
    ) -> UpgradeService<Ext, Sock>
    where
        Sock: WebSocketStream,
    {
        let Upgrader {
            extension_provider,
            config,
        } = self;
        UpgradeService::new(
            extension_provider.clone(),
            reservation,
            *config,
            scheme,
            addr,
        )
    }
}

/// A hyper service that will attempt to upgrade the connection to a websocket and pass back the
/// websocket using a reservation on an MPSC queue.
struct UpgradeService<Ext: ExtensionProvider, Sock> {
    extension_provider: Arc<Ext>,
    reservation: Option<mpsc::OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>>,
    config: Option<WebSocketConfig>,
    scheme: Scheme,
    addr: SocketAddr,
}

impl<Ext: ExtensionProvider, Sock> UpgradeService<Ext, Sock>
where
    Sock: WebSocketStream,
{
    fn new(
        extension_provider: Arc<Ext>,
        reservation: mpsc::OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>,
        config: Option<WebSocketConfig>,
        scheme: Scheme,
        addr: SocketAddr,
    ) -> Self {
        UpgradeService {
            extension_provider,
            reservation: Some(reservation),
            config,
            scheme,
            addr,
        }
    }
}

impl<Ext, Sock> Service<Request<Body>> for UpgradeService<Ext, Sock>
where
    Sock: WebSocketStream,
    Ext: ExtensionProvider,
    Ext::Extension: Send,
{
    type Response = Response<Body>;

    type Error = hyper::Error;

    type Future = Ready<Result<Response<Body>, hyper::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let UpgradeService {
            extension_provider,
            reservation,
            config,
            scheme,
            addr,
        } = self;
        let result =
            swim_http::negotiate_upgrade(&request, &PROTOCOLS, extension_provider.as_ref());
        ready(perform_upgrade(
            request,
            *config,
            result,
            reservation.take(),
            *scheme,
            *addr,
        ))
    }
}

/// Unwraps the opaque upgraded socket returned by hyper as the underlying socket type
/// that we originally passed in.
struct ReclaimSock<Sock>(PhantomData<fn(Upgraded) -> Sock>);

impl<Sock> Default for ReclaimSock<Sock> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<Sock> SockUnwrap for ReclaimSock<Sock>
where
    Sock: WebSocketStream,
{
    type Sock = Sock;

    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut) {
        let Parts { io, read_buf, .. } = upgraded
            .downcast::<Sock>()
            .expect("Hyper should not alter underlying socket type.");
        (io, BytesMut::from(read_buf.as_ref()))
    }
}

/// Associates a [`Scheme`] and [`SocketAddr`] with the future performing the websocket upgrade.
struct UpgradeFutureWithSock<Ext, Sock> {
    inner: UpgradeFuture<Ext, ReclaimSock<Sock>>,
    scheme: Scheme,
    addr: SocketAddr,
}

impl<Ext, Sock> UpgradeFutureWithSock<Ext, Sock> {
    pub fn new(
        inner: UpgradeFuture<Ext, ReclaimSock<Sock>>,
        scheme: Scheme,
        addr: SocketAddr,
    ) -> Self {
        UpgradeFutureWithSock {
            inner,
            scheme,
            addr,
        }
    }
}

impl<Ext, Sock> Future for UpgradeFutureWithSock<Ext, Sock>
where
    Sock: WebSocketStream,
    Ext: Extension + Unpin,
{
    type Output = Result<(WebSocket<Sock, Ext>, Scheme, SocketAddr), hyper::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let UpgradeFutureWithSock {
            inner,
            scheme,
            addr,
        } = self.get_mut();
        let ws = ready!(inner.poll_unpin(cx))?;
        Poll::Ready(Ok((ws, *scheme, *addr)))
    }
}

/// Implementation of [`WebsocketServer`] and [`WebsocketClient`] that uses [`hyper`] to upgrade
/// HTTP connections to [`ratchet`] web-socket connections.
pub struct HyperWebsockets {
    config: WebSocketConfig,
    max_negotiations: NonZeroUsize,
}

impl HyperWebsockets {
    /// #Arguments
    ///
    /// * `config` - Ratchet websocket configuration.
    /// * `max_negotiations` - The maximum number of concurrent connections that the server
    /// will handle concurrently.
    pub fn new(config: WebSocketConfig, max_negotiations: NonZeroUsize) -> Self {
        HyperWebsockets {
            config,
            max_negotiations,
        }
    }
}

impl WebsocketServer for HyperWebsockets {
    type WsStream<Sock, Ext> =
        BoxStream<'static, Result<(WebSocket<Sock, Ext>, SocketAddr), ListenerError>>;

    fn wrap_listener<Sock, L, Provider>(
        &self,
        listener: L,
        provider: Provider,
    ) -> Self::WsStream<Sock, Provider::Extension>
    where
        Sock: WebSocketStream + Send + Sync,
        L: Listener<Sock> + Send + 'static,
        Provider: ExtensionProvider + Send + Sync + Unpin + 'static,
        Provider::Extension: Send + Sync + Unpin + 'static,
    {
        let HyperWebsockets {
            config,
            max_negotiations,
        } = self;
        hyper_http_server(listener, provider, Some(*config), *max_negotiations)
            .map(|r| r.map(|(ws, _, addr)| (ws, addr)))
            .boxed()
    }
}

impl WebsocketClient for HyperWebsockets {
    fn open_connection<'a, Sock, Provider>(
        &self,
        socket: Sock,
        provider: &'a Provider,
        addr: String,
    ) -> WsOpenFuture<'a, Sock, Provider::Extension, RatchetError>
    where
        Sock: WebSocketStream + Send,
        Provider: ExtensionProvider + Send + Sync + 'static,
        Provider::Extension: Send + Sync + 'static,
    {
        let HyperWebsockets { config, .. } = self;

        let config = *config;
        Box::pin(async move {
            let subprotocols = ProtocolRegistry::new(PROTOCOLS.iter().copied())?;
            let socket = ratchet::subscribe_with(config, socket, addr, provider, subprotocols)
                .await?
                .into_websocket();
            Ok(socket)
        })
    }
}
