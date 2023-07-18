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
use ratchet::{Extension, ExtensionProvider, ProtocolRegistry, WebSocket, WebSocketConfig};
use swim_http::{Negotiated, SockUnwrap, UpgradeError, UpgradeFuture};
use swim_runtime::{
    net::{Listener, ListenerError, ListenerResult, Scheme},
    ws::{RatchetError, WebsocketClient, WebsocketServer, WsOpenFuture, PROTOCOLS},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{self, OwnedPermit},
};

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
    Sock: Unpin + Send + Sync + AsyncRead + AsyncWrite + 'static,
    L: Listener<Sock> + Send,
    Ext: ExtensionProvider + Send + Sync + 'static,
    Ext::Extension: Send + Unpin,
{
    let state = StreamState::<L::AcceptStream, Sock, Ext, _, _, _, _>::new(
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
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
    Ext: ExtensionProvider,
    Ext::Extension: Send,
{
    Connection(Con),
    Upgrade(UpgradeFutureWithSock<Ext::Extension, Sock>),
    Reserve(#[pin] Fut),
}

impl<Sock, Ext, Con, Fut> Future for TaskFuture<Sock, Ext, Con, Fut>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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

enum Reservable<F> {
    Closed,
    Reserving(mpsc::Sender<F>),
    Reserved(mpsc::Sender<F>, mpsc::OwnedPermit<F>),
}

impl<F> Reservable<F> {
    fn is_closed(&self) -> bool {
        matches!(self, Reservable::Closed)
    }

    fn new<Fut>(tx: mpsc::Sender<F>, f: impl Fn(mpsc::Sender<F>) -> Fut) -> (Self, Option<Fut>) {
        match tx.clone().try_reserve_owned() {
            Ok(r) => (Reservable::Reserved(tx, r), None),
            Err(mpsc::error::TrySendError::Closed(_)) => (Reservable::Closed, None),
            Err(mpsc::error::TrySendError::Full(tx2)) => (Reservable::Reserving(tx), Some(f(tx2))),
        }
    }

    fn add_reservation(&mut self, res: mpsc::OwnedPermit<F>) {
        *self = match std::mem::replace(self, Reservable::Closed) {
            Reservable::Closed => Reservable::Closed,
            Reservable::Reserving(tx) => Reservable::Reserved(tx, res),
            Reservable::Reserved(tx, r) => Reservable::Reserved(tx, r),
        };
    }

    fn take_reservation(&mut self) -> Option<mpsc::OwnedPermit<F>> {
        let (replacement, result) = match std::mem::replace(self, Reservable::Closed) {
            Reservable::Closed => (Reservable::Closed, None),
            Reservable::Reserving(tx) => (Reservable::Reserving(tx), None),
            Reservable::Reserved(tx, r) => (Reservable::Reserving(tx), Some(r)),
        };
        *self = replacement;
        result
    }

    fn update_reservation<Fut>(&mut self, f: impl Fn(mpsc::Sender<F>) -> Fut) -> Option<Fut> {
        match std::mem::replace(self, Reservable::Closed) {
            Reservable::Reserving(tx) => match tx.clone().try_reserve_owned() {
                Ok(res) => {
                    *self = Reservable::Reserved(tx, res);
                    None
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    *self = Reservable::Closed;
                    None
                }
                Err(mpsc::error::TrySendError::Full(tx2)) => {
                    *self = Reservable::Reserving(tx);
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

struct StreamState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
    Ext: ExtensionProvider,
    Ext::Extension: Send,
{
    listener_stream: L,
    connection_tasks: FuturesUnordered<TaskFuture<Sock, Ext, Con, Fut>>,
    upgrader: Upgrader<Ext>,
    reservable: Reservable<UpgradeFutureWithSock<Ext::Extension, Sock>>,
    upgrade_rx: mpsc::Receiver<UpgradeFutureWithSock<Ext::Extension, Sock>>,
    reserve_fn: FR,
    connect_fn: FC,
}

impl<L, Sock, Ext, Con, Fut, FR, FC> StreamState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
    Ext: ExtensionProvider + Send + Sync,
    Ext::Extension: Send,
    FR: Fn(mpsc::Sender<UpgradeFutureWithSock<Ext::Extension, Sock>>) -> Fut
        + Copy
        + Send
        + 'static,
    FC: Fn(Sock, UpgradeService<Ext, Sock>) -> Con + Copy + Send + 'static,
{
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
        let (reservable, fut) = Reservable::new(upgrade_tx, reserve_fn);
        if let Some(fut) = fut {
            connection_tasks.push(TaskFuture::Reserve(fut));
        };
        StreamState {
            listener_stream,
            connection_tasks,
            upgrader: Upgrader::new(extension_provider, config),
            reservable,
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

impl<L, Sock, Ext, Con, Fut, FR, FC> StreamState<L, Sock, Ext, Con, Fut, FR, FC>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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
        let StreamState {
            listener_stream,
            connection_tasks,
            upgrader,
            reservable,
            upgrade_rx,
            reserve_fn,
            connect_fn,
        } = self;
        loop {
            if reservable.is_closed() {
                return None;
            }
            let event = if let Some(res) = reservable.take_reservation() {
                tokio::select! {
                    biased;
                    maybe_fut = upgrade_rx.recv() => {
                        reservable.add_reservation(res);
                        if let Some(fut) = maybe_fut {
                            Event::Upgrade(fut)
                        } else {
                            Event::Continue
                        }
                    }
                    maybe_event = connection_tasks.next(), if !connection_tasks.is_empty() => {
                        reservable.add_reservation(res);
                        if let Some(ev) = maybe_event {
                            Event::TaskComplete(ev)
                        } else {
                            Event::Continue
                        }
                    }
                    maybe_incoming = listener_stream.next() => {
                        match maybe_incoming {
                            Some(Ok((sock, scheme, addr))) => {
                                if let Some(fut) = reservable.update_reservation(*reserve_fn) {
                                    connection_tasks.push(TaskFuture::Reserve(fut));
                                }
                                Event::Incoming(sock, scheme, addr, res)
                            },
                            Some(Err(err)) => {
                                reservable.add_reservation(res);
                                Event::IncomingFailed(err)
                            },
                            _ => {
                                *reservable = Reservable::Closed;
                                Event::Stop
                            },
                        }
                    }
                }
            } else {
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
                    reservable.add_reservation(res);
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

fn perform_upgrade<Ext, Sock, Err>(
    request: Request<Body>,
    config: Option<WebSocketConfig>,
    result: Result<Option<Negotiated<'_, Ext>>, UpgradeError<Err>>,
    reservation: Option<OwnedPermit<UpgradeFutureWithSock<Ext, Sock>>>,
    scheme: Scheme,
    addr: SocketAddr,
) -> Result<Response<Body>, hyper::Error>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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
        Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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

struct UpgradeService<Ext: ExtensionProvider, Sock> {
    extension_provider: Arc<Ext>,
    reservation: Option<mpsc::OwnedPermit<UpgradeFutureWithSock<Ext::Extension, Sock>>>,
    config: Option<WebSocketConfig>,
    scheme: Scheme,
    addr: SocketAddr,
}

impl<Ext: ExtensionProvider, Sock> UpgradeService<Ext, Sock>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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

struct ReclaimSock<Sock>(PhantomData<fn(Upgraded) -> Sock>);

impl<Sock> Default for ReclaimSock<Sock> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<Sock> SockUnwrap for ReclaimSock<Sock>
where
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
{
    type Sock = Sock;

    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut) {
        let Parts { io, read_buf, .. } = upgraded
            .downcast::<Sock>()
            .expect("Hyper should not alter underlying socket type.");
        (io, BytesMut::from(read_buf.as_ref()))
    }
}

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
    Sock: AsyncRead + AsyncWrite + Unpin + 'static,
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
        Sock: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
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
        Sock: AsyncRead + AsyncWrite + Send + Unpin + 'static,
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
