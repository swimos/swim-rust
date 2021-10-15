use std::fmt::{Debug, Formatter};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use ratchet::{SplittableExtension, WebSocket};
pub use swim_ratchet::*;
pub use switcher::StreamSwitcher;

#[cfg(feature = "tls")]
use {
    crate::error::TlsError, crate::ws::tls::build_x509_certificate, std::path::Path,
    tokio_native_tls::native_tls::Certificate, tokio_native_tls::TlsStream,
};

use crate::error::ConnectionError;

pub mod ext;
mod swim_ratchet;
pub mod utils;

mod switcher;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "tls")]
pub type WebSocketDef<E> = WebSocket<StreamSwitcher<TcpStream, TlsStream<TcpStream>>, E>;
#[cfg(not(feature = "tls"))]
pub type WebSocketDef<E> = WebSocket<TcpStream, E>;

#[cfg(feature = "tls")]
pub type StreamDef = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;
#[cfg(not(feature = "tls"))]
pub type StreamDef = TcpStream;

pub trait WsConnections<Socket>
where
    Socket: Send + Sync + Unpin,
{
    type Ext: SplittableExtension + Send + 'static;
    type Error: Into<ConnectionError>;

    /// Negotiate a new client connection.
    fn open_connection(
        &self,
        socket: Socket,
        addr: String,
    ) -> BoxFuture<Result<WebSocket<Socket, Self::Ext>, Self::Error>>;

    /// Negotiate a new server connection.
    fn accept_connection(
        &self,
        socket: Socket,
    ) -> BoxFuture<Result<WebSocket<Socket, Self::Ext>, Self::Error>>;
}

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    type Sock: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static;
    type Ext: SplittableExtension + Send + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(
        &mut self,
        url: url::Url,
    ) -> BoxFuture<Result<WebSocket<Self::Sock, Self::Ext>, ConnectionError>>;
}

#[derive(Clone)]
pub enum Protocol {
    PlainText,
    #[cfg(feature = "tls")]
    Tls(Certificate),
}

impl PartialEq for Protocol {
    fn eq(&self, other: &Self) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (self, other) {
            (Protocol::PlainText, Protocol::PlainText) => true,
            #[cfg(feature = "tls")]
            (Protocol::Tls(_), Protocol::Tls(_)) => true,
            #[cfg(feature = "tls")]
            _ => false,
        }
    }
}

impl Protocol {
    #[cfg(feature = "tls")]
    pub fn tls(path: impl AsRef<Path>) -> Result<Protocol, TlsError> {
        let cert = build_x509_certificate(path)?;
        Ok(Protocol::Tls(cert))
    }
}

impl Debug for Protocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => write!(f, "Tls"),
        }
    }
}
