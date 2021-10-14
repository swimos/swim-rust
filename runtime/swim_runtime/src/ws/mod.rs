use std::fmt::{Debug, Formatter};

use futures::future::BoxFuture;
use tokio::net::TcpStream;
use tokio_native_tls::TlsStream;

use ratchet::{NoExt, SplittableExtension, WebSocket};
pub use swim_ratchet::*;
pub use switcher::StreamSwitcher;
#[cfg(feature = "tls")]
use {
    crate::error::TlsError, crate::ws::tls::build_x509_certificate, std::fmt, std::path::Path,
    tokio_native_tls::native_tls::Certificate,
};

use crate::error::ConnectionError;

pub mod ext;
mod swim_ratchet;
pub mod utils;

mod switcher;

#[cfg(feature = "tls")]
pub mod tls;

pub type WebSocketDef = WebSocket<StreamSwitcher<TcpStream, TlsStream<TcpStream>>, NoExt>;

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
    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> BoxFuture<Result<WebSocketDef, ConnectionError>>;
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlainText => write!(f, "PlainText"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => write!(f, "Tls"),
        }
    }
}
