// Copyright 2015-2024 Swim Inc.
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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{ready, Future, FutureExt};
use http::request::Parts;
use http::{header::HeaderName, HeaderMap, HeaderValue, Method};
use http_body_util::Full;
use hyper::{
    upgrade::{OnUpgrade, Upgraded},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use ratchet::{
    Extension, ExtensionProvider, Role, SubprotocolRegistry, WebSocket, WebSocketConfig,
};
use ratchet_core::server::{build_response, parse_request_parts, UpgradeRequestParts};

const UPGRADE_STR: &str = "Upgrade";
const WEBSOCKET_STR: &str = "websocket";
const FAILED_RESPONSE: &str = "Building response should not fail.";

/// Represents the status of an upgrade attempt during a WebSocket negotiation.
///
/// The `UpgradeStatus` enum has two variants:
///
/// - `Upgradeable`: Indicates that an upgrade request has been initiated. It contains a `Result`
///   that wraps an `UpgradeRequest<E, T>` or an error from the `ratchet` module.
/// - `NotRequested`: Indicates that no upgrade was requested. It contains the original `Request<T>`
///   that was made.
pub enum UpgradeStatus<E, T> {
    /// Indicates that an upgrade request was made
    Upgradeable {
        /// The result of the upgrade operation. `Ok` if it was possible to upgrade the request and
        /// contains the upgraded parts which may be used to build a response and open the WebSocket
        /// connection or an `Err` if one was produced while parsing the request.
        result: Result<UpgradeRequestParts<E>, ratchet::Error>,
        /// The original `Request<T>` that was made.
        request: Request<T>,
    },
    /// Indicates that no upgrade was requested. Contains the original `Request<T>` that was made.
    NotRequested { request: Request<T> },
}

/// Attempt to negotiate a websocket upgrade on a hyper request. If [`Ok(None)`] is returned,
/// no upgrade was requested. If an error is returned an upgrade was requested but it failed.
///
/// # Arguments
/// * `request` - The HTTP request.
/// * `protocols` - The supported protocols for the negotiation.
/// * `extension_provider` - The extension provider (for example compression support).
pub fn negotiate_upgrade<T, E>(
    request: Request<T>,
    registry: &SubprotocolRegistry,
    extension_provider: &E,
) -> UpgradeStatus<E::Extension, T>
where
    E: ExtensionProvider,
{
    let headers = request.headers();
    let has_conn = headers_contains(headers, http::header::CONNECTION, UPGRADE_STR);
    let has_upgrade = headers_contains(headers, http::header::UPGRADE, WEBSOCKET_STR);

    if request.method() == Method::GET && has_conn && has_upgrade {
        let (parts, body) = request.into_parts();
        let Parts {
            method,
            version,
            headers,
            ..
        } = &parts;

        UpgradeStatus::Upgradeable {
            result: parse_request_parts(*version, method, headers, extension_provider, registry),
            request: Request::from_parts(parts, body),
        }
    } else {
        UpgradeStatus::NotRequested { request }
    }
}

/// Produce a bad request response for a bad websocket upgrade request.
pub fn fail_upgrade(error: ratchet::Error) -> Response<Full<Bytes>> {
    Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .body(Full::from(error.to_string()))
        .expect(FAILED_RESPONSE)
}

/// WebSocket upgrade parts used for initialising the connection.
pub struct Upgrade<Ext, U> {
    /// The negotiated response to be sent to the client.
    pub response: Response<Full<Bytes>>,
    /// A future that performs a websocket upgrade, unwraps the upgraded socket and creates a
    /// Ratchet WebSocket from it.
    pub future: UpgradeFuture<Ext, U>,
}

/// Upgrade a hyper request to a websocket, based on a successful negotiation.
///
/// # Arguments
/// * `parts` - The WebSocket upgrade parts.
/// * `request` - The upgrade request.
/// * `config` - Websocket configuration parameters.
/// * `unwrap_fn` - Used to unwrap the underlying socket type from the opaque [`Upgraded`] socket
///    provided by hyper.
pub fn upgrade<Ext, U, B>(
    parts: UpgradeRequestParts<Ext>,
    request: Request<B>,
    config: Option<WebSocketConfig>,
    unwrap_fn: U,
) -> Result<Upgrade<Ext, U>, ratchet::Error>
where
    Ext: Extension + Send,
{
    let UpgradeRequestParts {
        key,
        subprotocol,
        extension,
        extension_header,
        ..
    } = parts;
    let response = build_response(key, subprotocol, extension_header)?;
    let (parts, _body) = response.into_parts();

    Ok(Upgrade {
        response: Response::from_parts(parts, Full::default()),
        future: UpgradeFuture {
            upgrade: hyper::upgrade::on(request),
            config: config.unwrap_or_default(),
            extension,
            unwrap_fn,
        },
    })
}

fn headers_contains(headers: &HeaderMap, name: HeaderName, value: &str) -> bool {
    headers.get_all(name).iter().any(header_contains(value))
}

fn header_contains(content: &str) -> impl Fn(&HeaderValue) -> bool + '_ {
    move |header| {
        header
            .as_bytes()
            .split(|&c| c == b' ' || c == b',')
            .map(|s| std::str::from_utf8(s).unwrap_or("").trim())
            .any(|s| s.eq_ignore_ascii_case(content))
    }
}

/// Trait for unwrapping the concrete type of an upgraded socket.
/// Upon a connection upgrade, hyper returns the upgraded socket indirected through a trait object.
/// The caller will generally know the real underlying type and this allows for that type to be
/// restored.
pub trait SockUnwrap {
    type Sock;

    /// Unwrap the socket (returning the underlying socket and a buffer containing any bytes
    /// that have already been read).
    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut);
}

/// Implementation of [`SockUnwrap`] that does not unwrap the socket.
pub struct NoUnwrap;

impl SockUnwrap for NoUnwrap {
    type Sock = TokioIo<Upgraded>;

    fn unwrap_sock(&self, upgraded: Upgraded) -> (Self::Sock, BytesMut) {
        (TokioIo::new(upgraded), BytesMut::new())
    }
}

/// A future that performs a websocket upgrade, unwraps the upgraded socket and creates a Ratchet
/// WebSocket from it.
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
            extension.take(),
            prefix,
            Role::Server,
        )))
    }
}
