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

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use futures::future::{BoxFuture, Either};
use futures::stream::{unfold, BoxStream, FuturesUnordered};
use futures::{Future, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::macros::support::Poll;
use tokio::net::{TcpListener, TcpStream};

use tokio_native_tls::{
    native_tls::{Certificate, Identity, TlsConnector as NativeTlsConnector},
    TlsAcceptor, TlsConnector,
};
use tracing::error;

use crate::net::{dns::Resolver, IoResult, Listener, Scheme};
use pin_project::pin_project;

use super::dns::{BoxDnsResolver, DnsResolver};
use super::{
    ClientConnections, ConnResult, ConnectionError, ListenerError, ListenerResult,
    ServerConnections,
};

pub type TlsStream = tokio_native_tls::TlsStream<TcpStream>;
pub type TlsHandshakeResult = IoResult<(TlsStream, SocketAddr)>;

pub type BoxListenerStream<Socket> =
    BoxStream<'static, ListenerResult<(Socket, Scheme, SocketAddr)>>;

/// Either a simple, unencrypted [`TcpStream`] or a [`TlsStream`]. This allows an implementation
/// of [`ClientConnections`] and [`ServerConnections`] to expose an single socket type to support
/// both secure and unsecure connections.
#[pin_project(project = MaybeTlsProj)]
pub enum MaybeTlsStream {
    Plain(#[pin] TcpStream),
    Tls(#[pin] TlsStream),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_read(cx, buf),
            MaybeTlsProj::Tls(s) => s.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_write(cx, buf),
            MaybeTlsProj::Tls(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_flush(cx),
            MaybeTlsProj::Tls(s) => s.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            MaybeTlsProj::Plain(s) => s.poll_shutdown(cx),
            MaybeTlsProj::Tls(s) => s.poll_shutdown(cx),
        }
    }
}

impl Listener<TlsStream> for TlsListener {
    type AcceptStream = BoxListenerStream<TlsStream>;

    fn into_stream(self) -> Self::AcceptStream {
        let TlsListener { listener, acceptor } = self;
        tls_accept_stream(listener, acceptor).boxed()
    }
}

/// [`ClientConnections`] implementation that supports opening both secure and insecure connections.
#[derive(Clone)]
pub struct TokioTlsClientNetworking {
    resolver: Arc<Resolver>,
    connector: TlsConnector,
}

/// [`ServerConnections`] implementation that only supports secure connections.
#[derive(Clone)]
pub struct TokioTlsServerNetworking {
    acceptor: TlsAcceptor,
}

async fn accept_tls(
    acceptor: TlsAcceptor,
    addr: SocketAddr,
) -> ConnResult<(SocketAddr, TlsListener)> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    Ok((addr, TlsListener::new(listener, acceptor)))
}

impl TokioTlsServerNetworking {
    fn make_listener(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = ConnResult<(SocketAddr, TlsListener)>> + Send + 'static {
        let TokioTlsServerNetworking { acceptor } = self;
        let acc = acceptor.clone();
        accept_tls(acc, addr)
    }
}

impl TokioTlsClientNetworking {
    pub fn new(resolver: Arc<Resolver>, connector: TlsConnector) -> Self {
        TokioTlsClientNetworking {
            resolver,
            connector,
        }
    }

    pub fn with_root_certs(resolver: Arc<Resolver>, roots: Vec<Certificate>) -> TlsResult<Self> {
        let mut tls_conn_builder = NativeTlsConnector::builder();
        for cert in roots {
            tls_conn_builder.add_root_certificate(cert);
        }

        let connector = tls_conn_builder.build()?;

        Ok(Self::new(resolver, TlsConnector::from(connector)))
    }

    pub fn parse_certs(resolver: Arc<Resolver>, roots: &[RootCert<'_>]) -> TlsResult<Self> {
        let connector = connector_from_certs(roots)?;

        Ok(Self::new(resolver, connector))
    }
}

impl TokioTlsServerNetworking {
    pub fn new(acceptor: TlsAcceptor) -> Self {
        TokioTlsServerNetworking { acceptor }
    }

    pub fn from_identity(server_id: Identity) -> TlsResult<Self> {
        let acceptor = accepter_from_id(server_id)?;

        Ok(Self::new(acceptor))
    }

    pub fn parse_identity(id: IdentityCert<'_>) -> TlsResult<Self> {
        let acceptor = accepter_from_cert(id)?;

        Ok(Self::new(acceptor))
    }
}

impl ClientConnections for TokioTlsClientNetworking {
    type ClientSocket = MaybeTlsStream;

    fn try_open(
        &self,
        scheme: Scheme,
        _host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>> {
        async move {
            let host = addr.to_string();
            let socket = TcpStream::connect(addr).await?;
            match scheme {
                Scheme::Ws => Ok(MaybeTlsStream::Plain(socket)),
                Scheme::Wss => self
                    .connector
                    .connect(&host, socket)
                    .await
                    .map(MaybeTlsStream::Tls)
                    .map_err(|e| ConnectionError::NegotiationFailed(Box::new(e))),
            }
        }
        .boxed()
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        self.resolver.resolve(host, port)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.resolver.clone())
    }
}

impl ServerConnections for TokioTlsServerNetworking {
    type ServerSocket = TlsStream;

    type ListenerType = TlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        self.make_listener(addr).boxed()
    }
}

/// Combined implementation of [`ClientConnections`] and [`ServerConnections`] that wraps both
/// [`TokioTlsClientNetworking`] and [`TokioTlsServerNetworking`]. The server part is adapted to
/// produce [`MaybeTlsStream`] connections so that there is a unified client/server socket type,
/// inducing an implementation of [`super::ExternalConnections`].
#[derive(Clone)]
pub struct TokioTlsNetworking {
    client: TokioTlsClientNetworking,
    server: TokioTlsServerNetworking,
}

impl TokioTlsNetworking {
    pub fn new(
        resolver: Arc<Resolver>,
        connector: TlsConnector,
        acceptor: TlsAcceptor,
    ) -> TokioTlsNetworking {
        TokioTlsNetworking {
            client: TokioTlsClientNetworking::new(resolver, connector),
            server: TokioTlsServerNetworking::new(acceptor),
        }
    }

    pub fn from_identity(
        resolver: Arc<Resolver>,
        roots: Vec<Certificate>,
        server_id: Identity,
    ) -> TlsResult<Self> {
        Ok(TokioTlsNetworking {
            client: TokioTlsClientNetworking::with_root_certs(resolver, roots)?,
            server: TokioTlsServerNetworking::from_identity(server_id)?,
        })
    }

    pub fn parse_identity(
        resolver: Arc<Resolver>,
        id: IdentityCert<'_>,
        roots: &[RootCert<'_>],
    ) -> TlsResult<Self> {
        Ok(TokioTlsNetworking {
            client: TokioTlsClientNetworking::parse_certs(resolver, roots)?,
            server: TokioTlsServerNetworking::parse_identity(id)?,
        })
    }
}

impl ClientConnections for TokioTlsNetworking {
    type ClientSocket = MaybeTlsStream;

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>> {
        self.client.try_open(scheme, host, addr)
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        self.client.lookup(host, port)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        self.client.dns_resolver()
    }
}

impl ServerConnections for TokioTlsNetworking {
    type ServerSocket = MaybeTlsStream;

    type ListenerType = MaybeTlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        self.server
            .make_listener(addr)
            .map_ok(|(addr, listener)| (addr, listener.into()))
            .boxed()
    }
}

/// A listener that will listen for incoming TCP connections and attempt to negotiate a
/// TLS connection over them.
#[pin_project]
pub struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    fn new(listener: TcpListener, acceptor: TlsAcceptor) -> Self {
        TlsListener { listener, acceptor }
    }
}

/// Supported certificate formats for TLS connections.
#[derive(Clone, Copy)]
pub enum CertKind {
    DER,
    PEM,
}

/// An identity certificate for a TLS server.
pub struct IdentityCert<'a> {
    /// The format of the certificate.
    pub kind: CertKind,
    /// The key/password for the certificate (for DER this should be interpretable as a UTF8 string).
    pub key: &'a [u8],
    /// The body of the certificate as raw bytes.
    pub body: &'a [u8],
}

/// An X509 certificate to add as a trusted root for TLS clients.
pub struct RootCert<'a> {
    /// The format of the certificate.
    pub kind: CertKind,
    /// The body of the certificate as raw bytes.
    pub body: &'a [u8],
}

fn accepter_from_id(identity: Identity) -> TlsResult<TlsAcceptor> {
    let tls_acceptor = tokio_native_tls::native_tls::TlsAcceptor::builder(identity).build()?;
    Ok(TlsAcceptor::from(tls_acceptor))
}

fn accepter_from_cert(cert: IdentityCert<'_>) -> TlsResult<TlsAcceptor> {
    let IdentityCert { kind, key, body } = cert;
    let cert = match kind {
        CertKind::DER => {
            let password = std::str::from_utf8(key).unwrap();
            Identity::from_pkcs12(body, password)?
        }
        CertKind::PEM => Identity::from_pkcs8(body, key)?,
    };
    accepter_from_id(cert)
}

fn connector_from_certs<'a, I: IntoIterator<Item = &'a RootCert<'a>>>(
    roots: I,
) -> TlsResult<TlsConnector> {
    let mut tls_conn_builder = NativeTlsConnector::builder();
    for RootCert { kind, body } in roots {
        let cert = match kind {
            CertKind::DER => Certificate::from_der(body)?,
            CertKind::PEM => Certificate::from_pem(body)?,
        };
        tls_conn_builder.add_root_certificate(cert);
    }
    let connector = tls_conn_builder.build()?;

    Ok(TlsConnector::from(connector))
}

struct AcceptState<F> {
    listener: TcpListener,
    pending: FuturesUnordered<F>,
}

impl<F> AcceptState<F> {
    fn new(listener: TcpListener) -> Self {
        AcceptState {
            listener,
            pending: Default::default(),
        }
    }
}

type TlsError = tokio_native_tls::native_tls::Error;
type TlsResult<T> = Result<T, TlsError>;

impl<F: Future> AcceptState<F>
where
    F: Future<Output = Result<(TlsStream, SocketAddr), TlsError>>,
{
    fn push(&self, fut: F) {
        self.pending.push(fut);
    }

    async fn next_pending(
        &mut self,
    ) -> Option<Either<IoResult<(TcpStream, SocketAddr)>, TlsResult<(TlsStream, SocketAddr)>>> {
        let AcceptState { listener, pending } = self;
        tokio::select! {
            biased;
            handshake_result = pending.next(), if !pending.is_empty() => handshake_result.map(Either::Right),
            incoming_result = listener.accept() => Some(Either::Left(incoming_result)),
        }
    }
}

fn tls_accept_stream(
    listener: TcpListener,
    acceptor: TlsAcceptor,
) -> impl Stream<Item = ListenerResult<(TlsStream, Scheme, SocketAddr)>> + Send + 'static {
    let state = AcceptState::new(listener);
    unfold(
        Some((state, Arc::new(acceptor))),
        move |maybe_state| async move {
            if let Some((mut state, acceptor)) = maybe_state {
                loop {
                    match state.next_pending().await {
                        Some(Either::Left(Ok((tcp_stream, addr)))) => {
                            let acc = acceptor.clone();
                            let fut = async move {
                                let result = acc.accept(tcp_stream).await;
                                result.map(move |s| (s, addr))
                            };
                            state.push(fut);
                        }
                        Some(Either::Left(Err(e))) => {
                            let err = ListenerError::from(e);
                            if matches!(err, ListenerError::ListenerFailed(_)) {
                                break Some((Err(err), None));
                            } else {
                                break Some((Err(err), Some((state, acceptor))));
                            }
                        }
                        Some(Either::Right(Ok((tls_stream, addr)))) => {
                            break Some((
                                Ok((tls_stream, Scheme::Wss, addr)),
                                Some((state, acceptor)),
                            ));
                        }
                        Some(Either::Right(Err(e))) => {
                            error!(error = ?e, "TLS handshake failed.");
                            break Some((
                                Err(ListenerError::NegotiationFailed(Box::new(e))),
                                Some((state, acceptor)),
                            ));
                        }
                        None => break None,
                    }
                }
            } else {
                None
            }
        },
    )
}

/// Exposes a [`TcpListener`] as a [`Stream`].
struct TcpListenerWithPeer(TcpListener);

impl Stream for TcpListenerWithPeer {
    type Item = IoResult<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_accept(cx).map(Some)
    }
}

/// This wraps connections for a [`TlsListener`] as [`MaybeTlsStream`] to unify server and client
/// connection types.
pub struct MaybeTlsListener {
    inner: TlsListener,
}

impl From<TlsListener> for MaybeTlsListener {
    fn from(inner: TlsListener) -> Self {
        MaybeTlsListener { inner }
    }
}

impl Listener<MaybeTlsStream> for MaybeTlsListener {
    type AcceptStream = BoxListenerStream<MaybeTlsStream>;

    fn into_stream(self) -> Self::AcceptStream {
        let MaybeTlsListener {
            inner: TlsListener { listener, acceptor },
        } = self;
        tls_accept_stream(listener, acceptor)
            .map_ok(|(sock, scheme, addr)| (MaybeTlsStream::Tls(sock), scheme, addr))
            .boxed()
    }
}
