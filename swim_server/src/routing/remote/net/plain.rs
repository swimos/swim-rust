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

use std::net::SocketAddr;
use std::pin::Pin;

use crate::routing::remote::net::dns::{DnsResolver, Resolver};
use crate::routing::remote::net::{ExternalConnections, IoResult, Listener};
use crate::routing::remote::table::HostAndPort;
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

impl ExternalConnections for TokioPlainTextNetworking {
    type Socket = TcpStream;
    type ListenerType = TcpListener;

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::ListenerType>> {
        TcpListener::bind(addr).boxed()
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>> {
        TcpStream::connect(addr).boxed()
    }

    fn lookup(&self, host: HostAndPort) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        self.resolver.resolve(host)
    }
}

#[pin_project]
#[derive(Debug)]
pub struct WithPeer(#[pin] TcpListener);

impl Stream for WithPeer {
    type Item = IoResult<(TcpStream, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().0.poll_accept(cx).map(Some)
    }
}

impl Listener for TcpListener {
    type Socket = TcpStream;
    type AcceptStream = Fuse<WithPeer>;

    fn into_stream(self) -> Self::AcceptStream {
        WithPeer(self).fuse()
    }
}
