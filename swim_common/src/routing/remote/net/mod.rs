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

use std::net::SocketAddr;
use std::pin::Pin;

use crate::routing::remote::net::dns::{DnsResolver, Resolver};
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::net::tls::{TlsListener, TlsStream, TokioTlsNetworking};
use crate::routing::remote::table::HostAndPort;
use crate::routing::remote::{ExternalConnections, IoResult, Listener};
use either::Either;
use futures::stream::{Fuse, StreamExt};
use futures::task::{Context, Poll};
use futures::FutureExt;
use futures::Stream;
use futures_util::future::BoxFuture;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use url::Url;

pub mod dns;
pub mod plain;
pub mod tls;

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
    type Item = IoResult<(Either<TcpStream, TlsStream>, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.0 {
            MaybeTlsListener::PlainText(listener) => match listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, addr))) => {
                    Poll::Ready(Some(Ok((Either::Left(stream), addr))))
                }
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

    fn lookup(&self, host: HostAndPort) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        self.resolver.resolve(host).boxed()
    }
}
