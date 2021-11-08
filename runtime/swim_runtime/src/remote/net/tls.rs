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

use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;

use either::Either;
use futures::future::BoxFuture;
use futures::stream::Fuse;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use futures::{select, Stream};
use tokio::macros::support::Poll;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_native_tls::native_tls::{Identity, TlsConnector as NativeTlsConnector};
use tokio_native_tls::{TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

use crate::remote::net::dns::{DnsResolver, Resolver};
use crate::remote::net::plain::TokioPlainTextNetworking;
use crate::remote::net::{ExternalConnections, IoResult, Listener};
use crate::remote::table::SchemeHostPort;
use crate::remote::{Scheme, SchemeSocketAddr};
use im::HashMap;
use pin_project::pin_project;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

pub type TlsStream = tokio_native_tls::TlsStream<TcpStream>;
pub type TlsHandshakeResult = IoResult<(TlsStream, SocketAddr)>;
type TcpHandshakeResult = io::Result<(TcpStream, SocketAddr)>;

const DEFAULT_BUFFER: usize = 10;
const PENDING_ERR: &str = "TLS connection receiver buffer overflow";

/// HTTP protocol over TLS/SSL
const HTTPS_PORT: u16 = 443;

pub(crate) enum MaybeTlsListener {
    PlainText(TcpListener),
    Tls(TlsListener),
}

impl Listener for MaybeTlsListener {
    type Socket = Either<TcpStream, TlsStream>;
    type AcceptStream = Fuse<EitherStream>;

    fn into_stream(self) -> Self::AcceptStream {
        EitherStream(self).fuse()
    }
}

pub(crate) struct EitherStream(MaybeTlsListener);

impl Stream for EitherStream {
    type Item = IoResult<(Either<TcpStream, TlsStream>, SchemeSocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.0 {
            MaybeTlsListener::PlainText(listener) => match listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, addr))) => Poll::Ready(Some(Ok((
                    Either::Left(stream),
                    SchemeSocketAddr::new(Scheme::Ws, addr),
                )))),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            },
            MaybeTlsListener::Tls(ref mut listener) => match listener.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok((stream, addr)))) => {
                    Poll::Ready(Some(Ok((Either::Right(stream), addr))))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct TokioNetworking {
    resolver: Arc<Resolver>,
    plain: TokioPlainTextNetworking,
    tls: Arc<TokioTlsNetworking>,
}

impl TokioNetworking {
    #[allow(dead_code)]
    pub async fn new<I, A>(identities: I) -> Result<TokioNetworking, ()>
    where
        I: IntoIterator<Item = (A, Url)>,
        A: AsRef<PathBuf>,
    {
        let resolver = Arc::new(Resolver::new().await);
        let tls = TokioTlsNetworking::new(identities, resolver.clone());
        let plain = TokioPlainTextNetworking::new(resolver.clone());

        Ok(TokioNetworking {
            resolver,
            plain,
            tls: Arc::new(tls),
        })
    }
}

impl ExternalConnections for TokioNetworking {
    type Socket = Either<TcpStream, TlsStream>;
    type ListenerType = MaybeTlsListener;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::ListenerType>> {
        let this = self.clone();

        if addr.port() == HTTPS_PORT {
            Box::pin(async move { this.tls.bind(addr).await.map(MaybeTlsListener::Tls) })
        } else {
            Box::pin(async move { this.plain.bind(addr).await.map(MaybeTlsListener::PlainText) })
        }
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>> {
        let this = self.clone();

        if addr.port() == HTTPS_PORT {
            Box::pin(async move { this.tls.try_open(addr).await.map(Either::Right) })
        } else {
            Box::pin(async move { this.plain.try_open(addr).await.map(Either::Left) })
        }
    }

    fn lookup(
        &self,
        host: SchemeHostPort,
    ) -> BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>> {
        self.resolver.resolve(host).boxed()
    }
}

impl Listener for TlsListener {
    type Socket = TlsStream;
    type AcceptStream = Fuse<Self>;

    fn into_stream(self) -> Self::AcceptStream {
        self.fuse()
    }
}

#[derive(Clone)]
pub struct TokioTlsNetworking {
    resolver: Arc<Resolver>,
    identities: HashMap<Url, Identity>,
}

impl TokioTlsNetworking {
    pub fn new<I, A>(_identities: I, resolver: Arc<Resolver>) -> TokioTlsNetworking
    where
        I: IntoIterator<Item = (A, Url)>,
        A: AsRef<PathBuf>,
    {
        TokioTlsNetworking {
            resolver,
            identities: HashMap::new(),
        }
    }
}

impl ExternalConnections for TokioTlsNetworking {
    type Socket = TlsStream;
    type ListenerType = TlsListener;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::ListenerType>> {
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await?;
            Ok(TlsListener::new(listener))
        })
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>> {
        Box::pin(async move {
            let host = addr.to_string();
            let socket = TcpStream::connect(addr).await?;
            let tls_conn_builder = NativeTlsConnector::builder();

            let connector = tls_conn_builder
                .build()
                .map_err(|e| io::Error::new(ErrorKind::PermissionDenied, e.to_string()))?;
            let stream = TlsConnector::from(connector);

            stream
                .connect(&host, socket)
                .await
                .map_err(|e| io::Error::new(ErrorKind::ConnectionRefused, e.to_string()))
        })
    }

    fn lookup(&self, host: SchemeHostPort) -> BoxFuture<'static, IoResult<Vec<SchemeSocketAddr>>> {
        self.resolver.resolve(host)
    }
}

#[pin_project]
pub struct TlsListener {
    _jh: JoinHandle<()>,
    #[pin]
    receiver: ReceiverStream<TlsHandshakeResult>,
}

impl TlsListener {
    fn new(listener: TcpListener) -> TlsListener {
        // todo
        let cert = Identity::from_pkcs12(&[1, 2], "pw").unwrap();
        let tls_acceptor = tokio_native_tls::native_tls::TlsAcceptor::builder(cert)
            .build()
            .unwrap();
        let tls_acceptor = TlsAcceptor::from(tls_acceptor);

        let (jh, pending_rx) = PendingTlsConnections::accept(listener, tls_acceptor);

        TlsListener {
            _jh: jh,
            receiver: ReceiverStream::new(pending_rx),
        }
    }
}

impl Stream for TlsListener {
    type Item = IoResult<(TlsStream, SchemeSocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().receiver.poll_next(cx)?.map(|result| {
            result.map(|(stream, addr)| Ok((stream, SchemeSocketAddr::new(Scheme::Wss, addr))))
        })
    }
}

struct TcpListenerWithPeer(TcpListener);

impl Stream for TcpListenerWithPeer {
    type Item = IoResult<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_accept(cx).map(Some)
    }
}

pub struct PendingTlsConnections {
    listener: TcpListenerWithPeer,
    acceptor: TlsAcceptor,
    sender: mpsc::Sender<TlsHandshakeResult>,
}

impl PendingTlsConnections {
    pub fn accept(
        listener: TcpListener,
        acceptor: TlsAcceptor,
    ) -> (JoinHandle<()>, mpsc::Receiver<TlsHandshakeResult>) {
        let (tx, rx) = mpsc::channel(DEFAULT_BUFFER);

        let pending = PendingTlsConnections {
            listener: TcpListenerWithPeer(listener),
            acceptor,
            sender: tx,
        };

        let jh = tokio::spawn(pending.run());

        (jh, rx)
    }

    async fn run(self) {
        let PendingTlsConnections {
            listener,
            acceptor,
            sender,
        } = self;

        let mut fused_accept = listener.fuse();
        let mut pending = FuturesUnordered::new();

        loop {
            let next: Option<Either<TcpHandshakeResult, TlsHandshakeResult>> = select! {
                stream = fused_accept.next() => stream.map(Either::Left),
                result = pending.next() => result.map(Either::Right),
            };

            match next {
                Some(Either::Left(result)) => match result {
                    Ok((stream, addr)) => {
                        let accept_future = acceptor.accept(stream).map(move |r| match r {
                            Ok(stream) => Ok((stream, addr)),
                            Err(e) => Err(io::Error::new(ErrorKind::Other, e)),
                        });

                        pending.push(Box::pin(accept_future));
                    }
                    Err(e) => {
                        if sender.send(Err(e)).await.is_err() {
                            event!(Level::ERROR, PENDING_ERR)
                        }
                    }
                },
                Some(Either::Right(result)) => {
                    if sender.send(result).await.is_err() {
                        event!(Level::ERROR, PENDING_ERR)
                    }
                }
                None => {
                    return;
                }
            }
        }
    }
}
