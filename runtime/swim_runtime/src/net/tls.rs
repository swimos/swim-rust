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
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use futures::future::BoxFuture;
use futures::future::Either;
use futures::stream::unfold;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::macros::support::Poll;
use tokio::net::{TcpListener, TcpStream};

use tokio_native_tls::native_tls::Certificate;
use tokio_native_tls::native_tls::Identity;
use tokio_native_tls::native_tls::TlsConnector as NativeTlsConnector;
use tokio_native_tls::{TlsAcceptor, TlsConnector};
use tracing::error;

use crate::net::dns::Resolver;
use crate::net::{IoResult, Listener};
use crate::net::{Scheme, SchemeSocketAddr};
use pin_project::pin_project;

use super::dns::BoxDnsResolver;
use super::dns::DnsResolver;
use super::ClientConnections;
use super::ListenerError;
use super::ListenerResult;
use super::ServerConnections;

pub type TlsStream = tokio_native_tls::TlsStream<TcpStream>;
pub type TlsHandshakeResult = IoResult<(TlsStream, SocketAddr)>;

type BoxListenerStream<Socket> = BoxStream<'static, ListenerResult<(Socket, SchemeSocketAddr)>>;

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

#[derive(Clone)]
pub struct TokioTlsClientNetworking {
    resolver: Arc<Resolver>,
    connector: TlsConnector,
}

#[derive(Clone)]
pub struct TokioTlsServerNetworking {
    acceptor: TlsAcceptor,
}

async fn accept_tls(
    acceptor: TlsAcceptor,
    addr: SocketAddr,
) -> IoResult<(SocketAddr, TlsListener)> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    Ok((addr, TlsListener::new(listener, acceptor)))
}

impl TokioTlsServerNetworking {
    fn make_listener(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = IoResult<(SocketAddr, TlsListener)>> + Send + 'static {
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

    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, IoResult<Self::ClientSocket>> {
        async move {
            let SchemeSocketAddr { scheme, addr } = addr;
            let host = addr.to_string();
            let socket = TcpStream::connect(addr).await?;
            match scheme {
                Scheme::Ws => Ok(MaybeTlsStream::Plain(socket)),
                Scheme::Wss => self
                    .connector
                    .connect(&host, socket)
                    .await
                    .map(MaybeTlsStream::Tls)
                    .map_err(|e| io::Error::new(ErrorKind::ConnectionRefused, e.to_string())),
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
    ) -> BoxFuture<'static, IoResult<(SocketAddr, Self::ListenerType)>> {
        self.make_listener(addr).boxed()
    }
}

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

    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, IoResult<Self::ClientSocket>> {
        self.client.try_open(addr)
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
    ) -> BoxFuture<'static, IoResult<(SocketAddr, Self::ListenerType)>> {
        self.server
            .make_listener(addr)
            .map_ok(|(addr, listener)| (addr, listener.into()))
            .boxed()
    }
}

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

#[derive(Clone, Copy)]
pub enum CertKind {
    DER,
    PEM,
}

pub struct IdentityCert<'a> {
    pub kind: CertKind,
    pub key: &'a [u8],
    pub body: &'a [u8],
}

pub struct RootCert<'a> {
    pub kind: CertKind,
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
) -> impl Stream<Item = ListenerResult<(TlsStream, SchemeSocketAddr)>> + Send + 'static {
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
                            let scheme_addr = SchemeSocketAddr::new(Scheme::Wss, addr);
                            break Some((Ok((tls_stream, scheme_addr)), Some((state, acceptor))));
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

struct TcpListenerWithPeer(TcpListener);

impl Stream for TcpListenerWithPeer {
    type Item = IoResult<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_accept(cx).map(Some)
    }
}

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
            .map_ok(|(sock, addr)| (MaybeTlsStream::Tls(sock), addr))
            .boxed()
    }
}
