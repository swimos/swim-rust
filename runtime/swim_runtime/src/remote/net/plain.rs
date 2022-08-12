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

use std::net::SocketAddr;
use std::pin::Pin;

use crate::remote::net::dns::{DnsResolver, Resolver};
use crate::remote::net::{ExternalConnections, IoResult, Listener};
use crate::remote::table::SchemeHostPort;
use crate::remote::{Scheme, SchemeSocketAddr};
use futures::future::BoxFuture;
use futures::stream::Fuse;
use futures::task::{Context, Poll};
use futures::FutureExt;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

/// Implementation of [`ExternalConnections`] using [`TcpListener`] and [`TcpStream`] from Tokio.
#[derive(Debug, Clone)]
pub struct TokioPlainTextNetworking {
    resolver: Arc<Resolver>,
}

impl TokioPlainTextNetworking {
    pub fn new(resolver: Arc<Resolver>) -> TokioPlainTextNetworking {
        TokioPlainTextNetworking { resolver }
    }
}

async fn bind_to(addr: SocketAddr) -> IoResult<(SocketAddr, TcpListener)> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    Ok((addr, listener))
}

impl ExternalConnections for TokioPlainTextNetworking {
    type Socket = TcpStream;
    type ListenerType = TcpListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, IoResult<(SocketAddr, Self::ListenerType)>> {
        bind_to(addr).boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>> {
        TcpStream::connect(addr).boxed()
    }

    fn lookup(&self, host: SchemeHostPort) -> BoxFuture<'static, IoResult<Vec<SchemeSocketAddr>>> {
        self.resolver.resolve(host)
    }
}

#[pin_project]
#[derive(Debug)]
pub struct WithPeer(#[pin] TcpListener);

impl Stream for WithPeer {
    type Item = IoResult<(TcpStream, SchemeSocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project()
            .0
            .poll_accept(cx)?
            .map(|(stream, addr)| Some(Ok((stream, SchemeSocketAddr::new(Scheme::Ws, addr)))))
    }
}

impl Listener for TcpListener {
    type Socket = TcpStream;
    type AcceptStream = Fuse<WithPeer>;

    fn into_stream(self) -> Self::AcceptStream {
        WithPeer(self).fuse()
    }
}
