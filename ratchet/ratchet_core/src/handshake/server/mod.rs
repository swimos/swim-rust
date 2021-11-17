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

mod encoding;
#[cfg(test)]
mod tests;

use crate::ext::{NegotiatedExtension, NoExt};
use crate::handshake::io::BufferedIo;
use crate::handshake::server::encoding::{write_response, RequestParser};
use crate::handshake::{StreamingParser, ACCEPT_KEY};
use crate::handshake::{UPGRADE_STR, WEBSOCKET_STR};
use crate::protocol::Role;
use crate::{
    Error, HttpError, NoExtProvider, ProtocolRegistry, Request, WebSocket, WebSocketConfig,
    WebSocketStream,
};
use bytes::{Bytes, BytesMut};
use http::status::InvalidStatusCode;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use log::{error, trace};
use ratchet_ext::{Extension, ExtensionProvider};
use sha1::{Digest, Sha1};
use std::convert::TryFrom;
use std::iter::FromIterator;

const MSG_HANDSHAKE_COMPLETED: &str = "Server handshake completed";
const MSG_HANDSHAKE_FAILED: &str = "Server handshake failed";
const UPGRADED_MSG: &str = "Upgraded connection";
const REJECT_MSG: &str = "Rejected connection";

/// A structure representing an upgraded WebSocket session and an optional subprotocol that was
/// negotiated during the upgrade.
#[derive(Debug)]
pub struct UpgradedServer<S, E> {
    /// The original request that the peer sent.
    pub request: Request,
    /// The WebSocket connection.
    pub websocket: WebSocket<S, E>,
    /// An optional subprotocol that was negotiated during the upgrade.
    pub subprotocol: Option<String>,
}

impl<S, E> UpgradedServer<S, E> {
    /// Consume self and take the websocket
    pub fn into_websocket(self) -> WebSocket<S, E> {
        self.websocket
    }
}

/// Execute a server handshake on the provided stream.
///
/// Returns either a `WebSocketUpgrader` that may be used to either accept or reject the peer or an
/// error if the peer's request is malformatted or if an IO error occurs. If the peer is accepted,
/// then `config` will be used for building the `WebSocket`.
pub async fn accept<S, E>(
    stream: S,
    config: WebSocketConfig,
) -> Result<WebSocketUpgrader<S, NoExt>, Error>
where
    S: WebSocketStream,
{
    accept_with(stream, config, NoExtProvider, ProtocolRegistry::default()).await
}

/// Execute a server handshake on the provided stream. An attempt will be made to negotiate the
/// extension and subprotocols provided.
///
/// Returns either a `WebSocketUpgrader` that may be used to either accept or reject the peer or an
/// error if the peer's request is malformatted or if an IO error occurs. If the peer is accepted,
/// then `config`, `extension` and `subprotocols` will be used for building the `WebSocket`.
pub async fn accept_with<S, E>(
    mut stream: S,
    config: WebSocketConfig,
    extension: E,
    subprotocols: ProtocolRegistry,
) -> Result<WebSocketUpgrader<S, E::Extension>, Error>
where
    S: WebSocketStream,
    E: ExtensionProvider,
{
    let mut buf = BytesMut::new();
    let mut io = BufferedIo::new(&mut stream, &mut buf);
    let parser = StreamingParser::new(
        &mut io,
        RequestParser {
            subprotocols,
            extension,
        },
    );

    match parser.parse().await {
        Ok(result) => {
            let HandshakeResult {
                key,
                subprotocol,
                extension,
                request,
                extension_header,
            } = result;

            trace!(
                "{}for: {}. Selected subprotocol: {:?} and extension: {:?}",
                request.uri(),
                MSG_HANDSHAKE_COMPLETED,
                subprotocol,
                extension
            );

            Ok(WebSocketUpgrader {
                key,
                buf,
                stream,
                extension,
                request,
                subprotocol,
                extension_header,
                config,
            })
        }
        Err(e) => {
            error!("{}. Error: {:?}", MSG_HANDSHAKE_FAILED, e);

            match e.downcast_ref::<HttpError>() {
                Some(http_err) => {
                    write_response(
                        &mut stream,
                        &mut buf,
                        StatusCode::BAD_REQUEST,
                        HeaderMap::default(),
                        Some(http_err.to_string()),
                    )
                    .await?;
                    Err(e)
                }
                None => Err(e),
            }
        }
    }
}

/// A response to send to a client if the connection will not be upgraded.
#[derive(Debug)]
pub struct WebSocketResponse {
    status: StatusCode,
    headers: HeaderMap,
}

impl WebSocketResponse {
    /// Attempt to construct a new `WebSocketResponse` from `code`.
    ///
    /// # Errors
    /// Errors if the status code is invalid.
    pub fn new(code: u16) -> Result<WebSocketResponse, InvalidStatusCode> {
        StatusCode::from_u16(code).map(|status| WebSocketResponse {
            status,
            headers: HeaderMap::new(),
        })
    }

    /// Attempt to construct a new `WebSocketResponse` from `code` and `headers.
    ///
    /// # Errors
    /// Errors if the status code is invalid.
    pub fn with_headers<I>(code: u16, headers: I) -> Result<WebSocketResponse, InvalidStatusCode>
    where
        I: IntoIterator<Item = (http::header::HeaderName, http::header::HeaderValue)>,
    {
        Ok(WebSocketResponse {
            status: StatusCode::from_u16(code)?,
            headers: HeaderMap::from_iter(headers),
        })
    }
}

/// Represents a client connection that has been accepted and the upgrade request validated. This
/// may be used to validate the request by a user and opt to either continue the upgrade or reject
/// the connection - such as if the target path does not exist.
#[derive(Debug)]
pub struct WebSocketUpgrader<S, E> {
    request: Request,
    key: Bytes,
    subprotocol: Option<String>,
    buf: BytesMut,
    stream: S,
    extension: NegotiatedExtension<E>,
    extension_header: Option<HeaderValue>,
    config: WebSocketConfig,
}

impl<S, E> WebSocketUpgrader<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    /// The subprotocol that the client has requested.
    pub fn subprotocol(&self) -> Option<&String> {
        self.subprotocol.as_ref()
    }

    /// The URI that the client has requested.
    pub fn uri(&self) -> &Uri {
        self.request.uri()
    }

    /// The original request that the client sent.
    pub fn request(&self) -> &Request {
        &self.request
    }

    /// Attempt to upgrade this to a fully negotiated WebSocket connection.
    ///
    /// # Errors
    /// Errors if there is an IO error.
    pub async fn upgrade(self) -> Result<UpgradedServer<S, E>, Error> {
        self.upgrade_with(HeaderMap::default()).await
    }

    /// Insert `headers` into the response and attempt to upgrade this to a fully negotiated
    /// WebSocket connection.
    ///
    /// # Errors
    /// Errors if there is an IO error.
    pub async fn upgrade_with(self, mut headers: HeaderMap) -> Result<UpgradedServer<S, E>, Error> {
        let WebSocketUpgrader {
            request,
            key,
            subprotocol,
            mut buf,
            mut stream,
            extension,
            extension_header,
            config,
        } = self;

        let mut digest = Sha1::new();
        Digest::update(&mut digest, key);
        Digest::update(&mut digest, ACCEPT_KEY);

        let sec_websocket_accept = base64::encode(&digest.finalize());
        headers.insert(
            http::header::SEC_WEBSOCKET_ACCEPT,
            HeaderValue::try_from(sec_websocket_accept)?,
        );
        headers.insert(
            http::header::UPGRADE,
            HeaderValue::from_static(WEBSOCKET_STR),
        );
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static(UPGRADE_STR),
        );
        if let Some(subprotocol) = &subprotocol {
            headers.insert(
                http::header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::try_from(subprotocol)?,
            );
        }
        if let Some(extension_header) = extension_header {
            headers.insert(http::header::SEC_WEBSOCKET_EXTENSIONS, extension_header);
        }

        write_response(
            &mut stream,
            &mut buf,
            StatusCode::SWITCHING_PROTOCOLS,
            headers,
            None,
        )
        .await?;

        buf.clear();

        trace!("{} from {}", UPGRADED_MSG, request.uri());

        Ok(UpgradedServer {
            request,
            websocket: WebSocket::from_upgraded(config, stream, extension, buf, Role::Server),
            subprotocol,
        })
    }

    /// Reject this connection with the response provided.
    ///
    /// # Errors
    /// Errors if there is an IO error.
    pub async fn reject(self, response: WebSocketResponse) -> Result<(), Error> {
        let WebSocketResponse { status, headers } = response;
        let WebSocketUpgrader {
            mut stream,
            mut buf,
            request,
            ..
        } = self;

        trace!("{} from {}", REJECT_MSG, request.uri());

        write_response(&mut stream, &mut buf, status, headers, None).await
    }
}

#[derive(Debug)]
pub struct HandshakeResult<E> {
    key: Bytes,
    subprotocol: Option<String>,
    extension: NegotiatedExtension<E>,
    request: Request,
    extension_header: Option<HeaderValue>,
}
