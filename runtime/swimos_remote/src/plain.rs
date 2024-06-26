// Copyright 2015-2024 Swim Inc.
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

use crate::dns::{DnsResolver, Resolver};
use crate::net::Listener;
use crate::net::{ConnectionError, Scheme};
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::FutureExt;
use futures::Stream;
use pin_project::pin_project;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

use crate::dns::BoxDnsResolver;
use crate::net::{ClientConnections, ConnectionResult, ListenerResult, ServerConnections};

/// Implementation of [`ServerConnections`] and [`ClientConnections`], using [`TcpListener`]
/// and [`TcpStream`] from Tokio.
#[derive(Debug, Clone)]
pub struct TokioPlainTextNetworking {
    resolver: Arc<Resolver>,
}

impl TokioPlainTextNetworking {
    pub fn new(resolver: Arc<Resolver>) -> TokioPlainTextNetworking {
        TokioPlainTextNetworking { resolver }
    }
}

async fn bind_to(addr: SocketAddr) -> ConnectionResult<(SocketAddr, TcpListener)> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    Ok((addr, listener))
}

const NO_TLS: &str = "TLS connections are not supported.";

impl ClientConnections for TokioPlainTextNetworking {
    type ClientSocket = TcpStream;

    fn try_open(
        &self,
        scheme: Scheme,
        _host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnectionResult<Self::ClientSocket>> {
        async move {
            match scheme {
                Scheme::Ws => Ok(TcpStream::connect(addr).await?),
                Scheme::Wss => Err(ConnectionError::BadParameter(NO_TLS.to_string())),
            }
        }
        .boxed()
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.resolver.clone())
    }

    fn lookup(
        &self,
        host: String,
        port: u16,
    ) -> BoxFuture<'static, std::io::Result<Vec<SocketAddr>>> {
        self.resolver.resolve(host, port)
    }
}

impl ServerConnections for TokioPlainTextNetworking {
    type ServerSocket = TcpStream;

    type ListenerType = TcpListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnectionResult<(SocketAddr, Self::ListenerType)>> {
        bind_to(addr).boxed()
    }
}

/// Wrapper around [`TcpListener`] that attaches the [`Scheme`] used for the incoming connection
/// to the stream and peer address.
#[pin_project]
#[derive(Debug)]
pub struct WithScheme(#[pin] TcpListener);

impl Stream for WithScheme {
    type Item = ListenerResult<(TcpStream, Scheme, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .0
            .poll_accept(cx)?
            .map(|(stream, addr)| Some(Ok((stream, Scheme::Ws, addr))))
    }
}

impl Listener<TcpStream> for TcpListener {
    type AcceptStream = WithScheme;

    fn into_stream(self) -> Self::AcceptStream {
        WithScheme(self)
    }
}
