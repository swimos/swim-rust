use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::{Bytes, BytesMut};
use futures::{ready, Future, FutureExt};
use http::{header::HeaderName, HeaderMap, HeaderValue, Method};
use httparse::Header;
use hyper::{
    upgrade::{OnUpgrade, Upgraded},
    Body, Request, Response,
};
use ratchet::{
    Extension, ExtensionProvider, NegotiatedExtension, Role, WebSocket, WebSocketConfig,
    WebSocketStream,
};
use sha1::{Digest, Sha1};
use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;

const UPGRADE_STR: &str = "Upgrade";
const WEBSOCKET_STR: &str = "websocket";
const WEBSOCKET_VERSION_STR: &str = "13";
const ACCEPT_KEY: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const FAILED_RESPONSE: &str = "Building response should not fail.";

/// Result of a successful websocket negotiation.
pub struct Negotiated<'a, Ext> {
    pub protocol: Option<&'a str>,
    pub extension: Option<(Ext, HeaderValue)>,
    pub key: Bytes,
}

/// Attempt to negotiate a websocket upgrade on a hyper request. If [`Ok(None)`] is returned,
/// no upgrade was requested. If an error is returned an upgrade was requested but it failed.
pub fn negotiate_upgrade<'a, T, E>(
    request: &Request<T>,
    protocols: &'a HashSet<&str>,
    extension_provider: &E,
) -> Result<Option<Negotiated<'a, E::Extension>>, UpgradeError<E::Error>>
where
    E: ExtensionProvider,
{
    let headers = request.headers();
    let has_conn = headers_contains(headers, http::header::CONNECTION, UPGRADE_STR);
    let has_upgrade = headers_contains(headers, http::header::UPGRADE, WEBSOCKET_STR);

    if request.method() == Method::GET && has_conn && has_upgrade {
        if !headers_contains(
            headers,
            http::header::SEC_WEBSOCKET_VERSION,
            WEBSOCKET_VERSION_STR,
        ) {
            return Err(UpgradeError::InvalidWebsocketVersion);
        }

        let key = if let Some(key) = headers
            .get(http::header::SEC_WEBSOCKET_KEY)
            .map(|v| Bytes::from(trim(v.as_bytes()).to_vec()))
        {
            key
        } else {
            return Err(UpgradeError::NoKey);
        };

        let protocol = headers
            .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
            .iter()
            .flat_map(|h| h.as_bytes().split(|c| *c == b' ' || *c == b','))
            .map(trim)
            .filter_map(|b| std::str::from_utf8(b).ok())
            .find_map(|p| protocols.get(p).copied());

        let ext_headers = extension_headers(headers);

        let extension = extension_provider.negotiate_server(&ext_headers)?;
        Ok(Some(Negotiated {
            protocol,
            extension,
            key,
        }))
    } else {
        Ok(None)
    }
}

/// Produce a bad request response for a bad websocket upgrade request.
pub fn fail_upgrade<ExtErr: std::error::Error>(error: UpgradeError<ExtErr>) -> Response<Body> {
    Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .body(Body::from(error.to_string()))
        .expect(FAILED_RESPONSE)
}

/// Upgrade a hyper request to a websocket, based on a successful negotiation.
///
/// #Arguments
/// * `request` - The hyper HTTP request.
/// * `negotiated` - Negotiated parameters for the websocket connection.
/// * `config` - Websocket configuration parameters.
/// * `unwrap_fn` - Used to unwrap the underlying socket type from the opaque [`Upgraded`] socket
/// provided by hyper.
pub fn upgrade<Ext, U>(
    request: Request<Body>,
    negotiated: Negotiated<'_, Ext>,
    config: Option<WebSocketConfig>,
    unwrap_fn: U,
) -> (Response<Body>, UpgradeFuture<Ext, U>)
where
    U: SockUnwrap,
    Ext: Extension + Send,
{
    let Negotiated {
        protocol,
        extension,
        key,
    } = negotiated;
    let mut digest = Sha1::new();
    Digest::update(&mut digest, key);
    Digest::update(&mut digest, ACCEPT_KEY);

    let sec_websocket_accept = STANDARD.encode(digest.finalize());
    let mut builder = Response::builder()
        .status(http::StatusCode::SWITCHING_PROTOCOLS)
        .header(http::header::SEC_WEBSOCKET_ACCEPT, sec_websocket_accept)
        .header(http::header::CONNECTION, UPGRADE_STR)
        .header(http::header::UPGRADE, WEBSOCKET_STR);

    if let Some(protocol) = protocol {
        builder = builder.header(http::header::SEC_WEBSOCKET_PROTOCOL, protocol);
    }
    let ext = match extension {
        Some((ext, header)) => {
            builder = builder.header(http::header::SEC_WEBSOCKET_EXTENSIONS, header);
            Some(ext)
        }
        None => None,
    };
    let fut = UpgradeFuture {
        upgrade: hyper::upgrade::on(request),
        config: config.unwrap_or_default(),
        extension: ext,
        unwrap_fn,
    };

    let response = builder.body(Body::empty()).expect(FAILED_RESPONSE);
    (response, fut)
}

fn extension_headers(headers: &HeaderMap) -> Vec<Header<'_>> {
    headers
        .iter()
        .map(|(name, value)| Header {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

fn headers_contains(headers: &HeaderMap, name: HeaderName, value: &str) -> bool {
    headers.get_all(name).iter().any(header_contains(value))
}

fn header_contains(content: &str) -> impl Fn(&HeaderValue) -> bool + '_ {
    |header| {
        header
            .as_bytes()
            .split(|c| *c == b' ' || *c == b',')
            .map(trim)
            .any(|s| s.eq_ignore_ascii_case(content.as_bytes()))
    }
}

fn trim(bytes: &[u8]) -> &[u8] {
    let not_ws = |b: &u8| !b.is_ascii_whitespace();
    let start = bytes.iter().position(not_ws);
    let end = bytes.iter().rposition(not_ws);
    match (start, end) {
        (Some(s), Some(e)) => &bytes[s..e + 1],
        _ => &[],
    }
}

/// Reasons that a websocket upgrade request could fail.
#[derive(Debug, Error, Clone, Copy)]
pub enum UpgradeError<ExtErr: std::error::Error> {
    #[error("Invalid websocket version specified.")]
    InvalidWebsocketVersion,
    #[error("No websocket key provided.")]
    NoKey,
    #[error("Invalid extension headers: {0}")]
    ExtensionError(ExtErr),
}

impl<ExtErr: std::error::Error> From<ExtErr> for UpgradeError<ExtErr> {
    fn from(err: ExtErr) -> Self {
        UpgradeError::ExtensionError(err)
    }
}
/// Trait for unwrapping the concrete type of an upgraded socket.
/// Upon a connection upgrade, hyper returns the upgraded socket indirected through a trait object.
/// The caller will generally know the real underlying type and this allows for that type to be
/// restored.
pub trait SockUnwrap {
    type Sock: WebSocketStream;

    /// Unwrap the socket (returning the underlying socket and a buffer containing any bytes
    /// that have already been read).
    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut);
}

/// Implementation of [`SockUnwrap`] that does not unwrap the socket.
pub struct NoUnwrap;

impl SockUnwrap for NoUnwrap {
    type Sock = Upgraded;

    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut) {
        (upgraded, BytesMut::new())
    }
}

/// A future that performs a websocket upgrade, unwraps the upgraded socket and
/// creates a ratchet websocket from form it.
#[derive(Debug)]
pub struct UpgradeFuture<Ext, U> {
    upgrade: OnUpgrade,
    config: WebSocketConfig,
    extension: Option<Ext>,
    unwrap_fn: U,
}

impl<Ext, U> Future for UpgradeFuture<Ext, U>
where
    Ext: Extension + Unpin,
    U: SockUnwrap + Unpin,
{
    type Output = Result<WebSocket<U::Sock, Ext>, hyper::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let UpgradeFuture {
            upgrade,
            config,
            extension,
            unwrap_fn,
        } = self.get_mut();
        let (upgraded, prefix) = unwrap_fn.unwrap_sock(ready!(upgrade.poll_unpin(cx))?);
        Poll::Ready(Ok(WebSocket::from_upgraded(
            std::mem::take(config),
            upgraded,
            NegotiatedExtension::from(extension.take()),
            prefix,
            Role::Server,
        )))
    }
}
