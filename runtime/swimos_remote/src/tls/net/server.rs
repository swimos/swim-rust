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

use std::{net::SocketAddr, sync::Arc};

use crate::net::{
    ConnectionResult, Listener, ListenerError, ListenerResult, Scheme, ServerConnections,
};
use futures::{
    future::{BoxFuture, Either},
    stream::{unfold, BoxStream, FuturesUnordered},
    Future, FutureExt, Stream, StreamExt, TryStreamExt,
};
use rustls::pki_types::PrivateKeyDer;
use rustls::KeyLogFile;
use rustls_pemfile::Item;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsStream};

use crate::tls::{
    config::{CertChain, CertFormat, PrivateKey, ServerConfig},
    errors::TlsError,
    maybe::MaybeTlsStream,
};

/// [`ServerConnections`] implementation that only supports secure connections.
#[derive(Clone)]
pub struct RustlsServerNetworking {
    acceptor: TlsAcceptor,
}

async fn accept_tls(
    acceptor: TlsAcceptor,
    addr: SocketAddr,
) -> ConnectionResult<(SocketAddr, RustlsListener)> {
    let listener = TcpListener::bind(addr).await?;
    let bound_to = listener.local_addr()?;
    Ok((bound_to, RustlsListener { listener, acceptor }))
}

impl RustlsServerNetworking {
    pub fn make_listener(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = ConnectionResult<(SocketAddr, RustlsListener)>> + Send + 'static {
        let RustlsServerNetworking { acceptor } = self;
        let acc = acceptor.clone();
        accept_tls(acc, addr)
    }
}

impl RustlsServerNetworking {
    pub fn new(acceptor: TlsAcceptor) -> Self {
        RustlsServerNetworking { acceptor }
    }
}

impl TryFrom<ServerConfig> for RustlsServerNetworking {
    type Error = TlsError;

    fn try_from(config: ServerConfig) -> Result<Self, Self::Error> {
        let ServerConfig {
            chain: CertChain(certs),
            key,
            enable_log_file,
            provider,
        } = config;

        let mut chain = vec![];
        for cert in certs {
            chain.extend(super::load_cert_file(cert)?);
        }

        let PrivateKey { format, body } = key;
        let server_key = match format {
            CertFormat::Pem => {
                let mut body_ref = body.as_ref();
                match rustls_pemfile::read_one(&mut body_ref).map_err(TlsError::InvalidPem)? {
                    Some(Item::Sec1Key(body)) => PrivateKeyDer::from(body),
                    Some(Item::Pkcs8Key(body)) => PrivateKeyDer::from(body),
                    Some(Item::Pkcs1Key(body)) => PrivateKeyDer::from(body),
                    _ => return Err(TlsError::InvalidPrivateKey),
                }
            }
            CertFormat::Der => {
                PrivateKeyDer::try_from(body).map_err(|_| TlsError::InvalidPrivateKey)?
            }
        };

        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(chain, server_key)
            .expect("Invalid certs or private key.");

        if enable_log_file {
            config.key_log = Arc::new(KeyLogFile::new());
        }
        let acceptor = TlsAcceptor::from(Arc::new(config));
        Ok(RustlsServerNetworking::new(acceptor))
    }
}

impl ServerConnections for RustlsServerNetworking {
    type ServerSocket = TlsStream<TcpStream>;

    type ListenerType = RustlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnectionResult<(SocketAddr, Self::ListenerType)>> {
        self.make_listener(addr).boxed()
    }
}

/// A listener that will listen for incoming TCP connections and attempt to negotiate a
/// TLS connection over them.
pub struct RustlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

type BoxListenerStream<Socket> = BoxStream<'static, ListenerResult<(Socket, Scheme, SocketAddr)>>;

impl Listener<TlsStream<TcpStream>> for RustlsListener {
    type AcceptStream = BoxListenerStream<TlsStream<TcpStream>>;

    fn into_stream(self) -> Self::AcceptStream {
        let RustlsListener { listener, acceptor } = self;
        tls_accept_stream(listener, acceptor).boxed()
    }
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

type TlsResult<T> = Result<T, TlsError>;

impl<F: Future> AcceptState<F>
where
    F: Future<Output = Result<(TlsStream<TcpStream>, SocketAddr), TlsError>>,
{
    fn push(&self, fut: F) {
        self.pending.push(fut);
    }

    async fn next_pending(
        &mut self,
    ) -> Option<
        Either<
            std::io::Result<(TcpStream, SocketAddr)>,
            TlsResult<(TlsStream<TcpStream>, SocketAddr)>,
        >,
    > {
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
) -> impl Stream<Item = ListenerResult<(TlsStream<TcpStream>, Scheme, SocketAddr)>> + Send + 'static
{
    let state = AcceptState::new(listener);
    unfold(Some((state, acceptor)), move |maybe_state| async move {
        if let Some((mut state, acceptor)) = maybe_state {
            loop {
                match state.next_pending().await {
                    Some(Either::Left(Ok((tcp_stream, addr)))) => {
                        let acc = acceptor.clone();
                        let fut = async move {
                            let result = acc
                                .accept(tcp_stream)
                                .await
                                .map_err(TlsError::HandshakeFailed);
                            result.map(move |s| (TlsStream::Server(s), addr))
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
                        break Some((Ok((tls_stream, Scheme::Wss, addr)), Some((state, acceptor))));
                    }
                    Some(Either::Right(Err(e))) => {
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
    })
}

/// This wraps connections for [`TcpListener`] and [`RustlsListener`] as [`crate::maybe::MaybeTlsStream`] to unify server and client
/// connection types.
pub struct MaybeRustTlsListener {
    inner: Either<TcpListener, RustlsListener>,
}

impl From<TcpListener> for MaybeRustTlsListener {
    fn from(inner: TcpListener) -> Self {
        MaybeRustTlsListener {
            inner: Either::Left(inner),
        }
    }
}

impl From<RustlsListener> for MaybeRustTlsListener {
    fn from(inner: RustlsListener) -> Self {
        MaybeRustTlsListener {
            inner: Either::Right(inner),
        }
    }
}

impl Listener<MaybeTlsStream> for MaybeRustTlsListener {
    type AcceptStream = BoxListenerStream<MaybeTlsStream>;

    fn into_stream(self) -> Self::AcceptStream {
        let MaybeRustTlsListener { inner } = self;

        match inner {
            Either::Left(listener) => listener
                .into_stream()
                .map_ok(|(sock, scheme, addr)| (MaybeTlsStream::Plain(sock), scheme, addr))
                .boxed(),

            Either::Right(RustlsListener { listener, acceptor }) => {
                tls_accept_stream(listener, acceptor)
                    .map_ok(|(sock, scheme, addr)| (MaybeTlsStream::Tls(sock), scheme, addr))
                    .boxed()
            }
        }
    }
}
