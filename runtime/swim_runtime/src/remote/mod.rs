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

pub mod config;
pub mod net;

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::FusedStream;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io;

use thiserror::Error;

use url::Url;

use self::net::dns::BoxDnsResolver;

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

/// Trait for servers that listen for incoming remote connections. This is primarily used to
/// abstract over [`std::net::TcpListener`] for testing purposes.
pub trait Listener {
    type Socket: Unpin + Send + Sync + 'static;
    type AcceptStream: FusedStream<Item = IoResult<(Self::Socket, SchemeSocketAddr)>>
        + Send
        + Sync
        + Unpin;

    fn into_stream(self) -> Self::AcceptStream;
}

/// A combination of host name and port to be used as a key into the routing table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeHostPort(Scheme, String, u16);

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

    pub fn split(self) -> (Scheme, String, u16) {
        let SchemeHostPort(scheme, host, port) = self;
        (scheme, host, port)
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

/// Trait for types that can create remote network connections asynchronously. This is primarily
/// used to abstract over [`std::net::TcpListener`] and [`std::net::TcpStream`] for testing purposes.
pub trait ExternalConnections: Clone + Send + Sync + 'static {
    type Socket: Unpin + Send + Sync + 'static;
    type ListenerType: Listener<Socket = Self::Socket> + Send + Sync;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, IoResult<(SocketAddr, Self::ListenerType)>>;
    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>>;

    fn dns_resolver(&self) -> BoxDnsResolver;
    fn lookup(&self, host: SchemeHostPort) -> BoxFuture<'static, IoResult<Vec<SchemeSocketAddr>>>;
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
    ) -> BoxFuture<'static, IoResult<(SocketAddr, Self::ListenerType)>> {
        (**self).bind(addr)
    }

    fn try_open(&self, addr: SocketAddr) -> BoxFuture<'static, IoResult<Self::Socket>> {
        (**self).try_open(addr)
    }

    fn lookup(&self, host: SchemeHostPort) -> BoxFuture<'static, IoResult<Vec<SchemeSocketAddr>>> {
        (**self).lookup(host)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        (**self).dns_resolver()
    }
}
