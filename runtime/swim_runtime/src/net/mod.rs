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
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};

use thiserror::Error;

use url::Url;

use self::dns::BoxDnsResolver;

pub mod dns;
pub mod plain;

#[derive(Debug, PartialEq, Eq, Error)]
pub enum BadUrl {
    #[error("{0} is not a valid Warp scheme.")]
    BadScheme(String),
    #[error("The URL did not contain a valid host.")]
    NoHost,
}

/// Supported websocket schemes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Ws,
    Wss,
}

impl TryFrom<&str> for Scheme {
    type Error = BadUrl;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ws" | "swim" | "warp" => Ok(Scheme::Ws),
            "wss" | "swims" | "warps" => Ok(Scheme::Wss),
            _ => Err(BadUrl::BadScheme(value.to_owned())),
        }
    }
}

impl Scheme {
    /// Get the default port for the schemes.
    fn get_default_port(&self) -> u16 {
        match self {
            Scheme::Ws => 80,
            Scheme::Wss => 443,
        }
    }

    /// Return if the scheme is secure.
    #[allow(dead_code)]
    fn is_secure(&self) -> bool {
        match self {
            Scheme::Ws => false,
            Scheme::Wss => true,
        }
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Ws => {
                write!(f, "ws")
            }
            Scheme::Wss => {
                write!(f, "wss")
            }
        }
    }
}

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
    NegotiationFailed(#[source] Box<dyn std::error::Error + Send>),
    /// A connection parameter was invalid (for example, requesting a secure connection when TLS is not enabled).
    #[error("Negotiating a new connection failed: {0}")]
    BadParameter(#[source] Box<dyn std::error::Error + Send>),
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

/// A combination of host name and port to be used as a key into the routing table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeHostPort(pub Scheme, pub String, pub u16);

impl SchemeHostPort {
    pub fn new(scheme: Scheme, host: String, port: u16) -> Self {
        SchemeHostPort(scheme, host, port)
    }

    pub fn scheme(&self) -> &Scheme {
        &self.0
    }

    pub fn host(&self) -> &String {
        &self.1
    }

    pub fn port(&self) -> u16 {
        self.2
    }
}

impl Display for SchemeHostPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let SchemeHostPort(scheme, host, port) = self;
        write!(f, "{}://{}:{}", scheme, host, port)
    }
}

impl<'a> TryFrom<&'a Url> for SchemeHostPort {
    type Error = BadUrl;

    fn try_from(url: &'a Url) -> Result<Self, Self::Error> {
        let scheme = Scheme::try_from(url.scheme())?;
        match (url.host_str(), url.port()) {
            (Some(host_str), Some(port)) => {
                Ok(SchemeHostPort::new(scheme, host_str.to_owned(), port))
            }
            (Some(host_str), _) => {
                let default_port = scheme.get_default_port();
                Ok(SchemeHostPort::new(
                    scheme,
                    host_str.to_owned(),
                    default_port,
                ))
            }
            _ => Err(BadUrl::NoHost),
        }
    }
}

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
