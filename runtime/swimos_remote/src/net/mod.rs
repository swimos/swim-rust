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

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::Stream;
use std::io;
use swimos_net::Scheme;
use tokio::io::{AsyncRead, AsyncWrite};

use thiserror::Error;

use self::dns::BoxDnsResolver;

pub mod dns;
pub mod plain;

#[cfg(test)]
mod tests;

pub type IoResult<T> = io::Result<T>;
pub type ConnResult<T> = Result<T, ConnectionError>;

/// Error to indicate why attempting to open a new connection failed.
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// The connection could not be established at all.
    #[error("Opening a new connection failed: {0}")]
    ConnectionFailed(
        #[source]
        #[from]
        std::io::Error,
    ),
    /// A connection could not be negotiated (for example, a TLS handshake failed).
    #[error("Negotiating a new connection failed: {0}")]
    NegotiationFailed(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A connection parameter was invalid (for example, requesting a secure connection when TLS is not enabled).
    #[error("Negotiating a new connection failed: {0}")]
    BadParameter(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Errors that can be generated when listening for incoming connections. Particularly, this
/// distinguishes between an error in the listening process itself (after which we should stop
/// listening) and error with an incoming connection (after which we can attempt to carry on
/// listening).
#[derive(Debug, Error)]
pub enum ListenerError {
    /// Listening for new connections failed.
    #[error("Listening for incoming connections failed: {0}")]
    ListenerFailed(#[source] std::io::Error),
    /// A failure occurred trying to accept an incoming connection.
    #[error("Accepting a new connection failed: {0}")]
    AcceptFailed(#[source] std::io::Error),
    /// An incoming connection was established but the connection could not be negotiated (for example,
    /// a TLS handshake failed).
    #[error("Negotiating a new connection failed: {0}")]
    NegotiationFailed(#[source] Box<dyn std::error::Error + Send>),
}

impl From<std::io::Error> for ListenerError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::InvalidData
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof => ListenerError::AcceptFailed(err),
            _ => ListenerError::ListenerFailed(err),
        }
    }
}

pub type ListenerResult<T> = Result<T, ListenerError>;

/// Trait for servers that listen for incoming remote connections. This is primarily used to
/// abstract over [`std::net::TcpListener`] for testing purposes.
pub trait Listener<Socket>
where
    Socket: Unpin + Send + Sync + 'static,
{
    type AcceptStream: Stream<Item = ListenerResult<(Socket, Scheme, SocketAddr)>> + Send + Unpin;

    fn into_stream(self) -> Self::AcceptStream;
}

pub type BoxListenerStream<Socket> =
    BoxStream<'static, ListenerResult<(Socket, Scheme, SocketAddr)>>;

/// Provides all networking functionality required for a Warp client (DNS resolution and opening sockets).
pub trait ClientConnections: Clone + Send + Sync + 'static {
    type ClientSocket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>>;

    fn dns_resolver(&self) -> BoxDnsResolver;
    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>>;
}

impl<C> ClientConnections for Arc<C>
where
    C: ClientConnections,
{
    type ClientSocket = C::ClientSocket;

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>> {
        (**self).try_open(scheme, host, addr)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        (**self).dns_resolver()
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        (**self).lookup(host, port)
    }
}

/// Provides all networking functionality required for a Warp server (listening for incoming connections).
pub trait ServerConnections: Clone + Send + Sync + 'static {
    type ServerSocket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Self::ServerSocket> + Send + Sync;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>>;
}

impl<C> ServerConnections for Arc<C>
where
    C: ServerConnections,
{
    type ServerSocket = C::ServerSocket;

    type ListenerType = C::ListenerType;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        (**self).bind(addr)
    }
}

/// Trait for types that can create remote network connections asynchronously. This is primarily
/// used to abstract over ordinary sockets and sockets with TLS support.
pub trait ExternalConnections: Clone + Send + Sync + 'static {
    type Socket: Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Self::Socket> + Send + Sync;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>>;
    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::Socket>>;

    fn dns_resolver(&self) -> BoxDnsResolver;
    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>>;
}

impl<C> ExternalConnections for C
where
    C: ClientConnections + ServerConnections<ServerSocket = <C as ClientConnections>::ClientSocket>,
{
    type Socket = <C as ClientConnections>::ClientSocket;

    type ListenerType = <C as ServerConnections>::ListenerType;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        <Self as ServerConnections>::bind(self, addr)
    }

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::Socket>> {
        <Self as ClientConnections>::try_open(self, scheme, host, addr)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        <Self as ClientConnections>::dns_resolver(self)
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        <Self as ClientConnections>::lookup(self, host, port)
    }
}
