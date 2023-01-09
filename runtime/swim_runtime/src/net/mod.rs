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
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io;

use thiserror::Error;

use url::Url;

use self::dns::BoxDnsResolver;

pub mod dns;
pub mod plain;
#[cfg(feature = "tls")]
pub mod tls;

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

type IoResult<T> = io::Result<T>;
type ConnResult<T> = Result<T, ConnectionError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemeSocketAddr {
    pub scheme: Scheme,
    pub addr: SocketAddr,
}

impl SchemeSocketAddr {
    pub fn new(scheme: Scheme, addr: SocketAddr) -> SchemeSocketAddr {
        SchemeSocketAddr { scheme, addr }
    }
}

impl Display for SchemeSocketAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}/", self.scheme, self.addr)
    }
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Opening a new connection failed: {0}")]
    ConnectionFailed(#[source] #[from] std::io::Error),
    #[error("Negotiating a new connection failed: {0}")]
    NegotiationFailed(#[source] Box<dyn std::error::Error + Send>),
}

#[derive(Debug, Error)]
pub enum ListenerError {
    #[error("Listening for incoming connections failed: {0}")]
    ListenerFailed(#[source] std::io::Error),
    #[error("Accepting a new connection failed: {0}")]
    AcceptFailed(#[source] std::io::Error),
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
    type AcceptStream: Stream<Item = ListenerResult<(Socket, SchemeSocketAddr)>> + Send + Unpin;

    fn into_stream(self) -> Self::AcceptStream;
}

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

pub trait ClientConnections: Clone + Send + Sync + 'static {
    type ClientSocket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, ConnResult<Self::ClientSocket>>;

    fn dns_resolver(&self) -> BoxDnsResolver;
    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>>;
}

pub trait ServerConnections: Clone + Send + Sync + 'static {
    type ServerSocket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Self::ServerSocket> + Send + Sync;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>>;
}

/// Trait for types that can create remote network connections asynchronously. This is primarily
/// used to abstract over [`std::net::TcpListener`] and [`std::net::TcpStream`] for testing purposes.
pub trait ExternalConnections: Clone + Send + Sync + 'static {
    type Socket: Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Self::Socket> + Send + Sync;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>>;
    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, ConnResult<Self::Socket>>;

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

    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, ConnResult<Self::Socket>> {
        <Self as ClientConnections>::try_open(self, addr)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        <Self as ClientConnections>::dns_resolver(self)
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        <Self as ClientConnections>::lookup(self, host, port)
    }
}

impl<Conn> ExternalConnections for Arc<Conn>
where
    Conn: ExternalConnections,
{
    type Socket = Conn::Socket;

    type ListenerType = Conn::ListenerType;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        (**self).bind(addr)
    }

    fn try_open(&self, addr: SchemeSocketAddr) -> BoxFuture<'_, ConnResult<Self::Socket>> {
        (**self).try_open(addr)
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        (**self).lookup(host, port)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        (**self).dns_resolver()
    }
}
