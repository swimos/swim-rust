pub mod ext;
pub mod swim_ratchet;
pub mod utils;

#[cfg(feature = "tls")]
pub mod tls;

use futures::future::BoxFuture;
use ratchet::{Extension, WebSocket};
use std::error::Error;

use std::fmt::{Debug, Formatter};
#[cfg(feature = "tls")]
use {
    crate::error::TlsError, crate::ws::tls::build_x509_certificate, std::fmt, std::path::Path,
    tokio_native_tls::native_tls::Certificate,
};

pub trait WsConnections<Socket>
where
    Socket: Send + Sync + Unpin,
{
    type Ext: Extension;
    type Error: Error;

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
