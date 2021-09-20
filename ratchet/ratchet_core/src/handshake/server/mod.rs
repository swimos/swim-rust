// Copyright 2015-2021 SWIM.AI inc.
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

use crate::handshake::io::BufferedIo;
use crate::handshake::server::encoding::{write_response, RequestParser};
use crate::handshake::{StreamingParser, ACCEPT_KEY};
use crate::handshake::{UPGRADE_STR, WEBSOCKET_STR};
use crate::protocol::Role;
use crate::{
    Error, HttpError, NoExt, NoExtProvider, ProtocolRegistry, Request, Upgraded, WebSocket,
    WebSocketConfig, WebSocketStream,
};
use bytes::{Bytes, BytesMut};
use http::status::InvalidStatusCode;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use ratchet_ext::{Extension, ExtensionProvider};
use sha1::{Digest, Sha1};
use std::convert::TryFrom;
use std::iter::FromIterator;

pub async fn accept<S, E>(
    stream: S,
    config: WebSocketConfig,
) -> Result<WebSocketUpgrader<S, NoExt>, Error>
where
    S: WebSocketStream,
{
    accept_with(stream, config, NoExtProvider, ProtocolRegistry::default()).await
}

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
        Err(e) => match e.downcast_ref::<HttpError>() {
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
        },
    }
}

pub struct WebSocketResponse {
    status: StatusCode,
    headers: HeaderMap,
}

impl WebSocketResponse {
    pub fn new(code: u16) -> Result<WebSocketResponse, InvalidStatusCode> {
        StatusCode::from_u16(code).map(|status| WebSocketResponse {
            status,
            headers: HeaderMap::new(),
        })
    }

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

pub struct WebSocketUpgrader<S, E> {
    request: Request,
    key: Bytes,
    subprotocol: Option<String>,
    buf: BytesMut,
    stream: S,
    extension: E,
    extension_header: Option<HeaderValue>,
    config: WebSocketConfig,
}

impl<S, E> WebSocketUpgrader<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub fn subprotocol(&self) -> Option<&String> {
        self.subprotocol.as_ref()
    }

    pub fn uri(&self) -> &Uri {
        self.request.uri()
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub async fn upgrade(self) -> Result<Upgraded<S, E>, Error> {
        self.upgrade_with(HeaderMap::default()).await
    }

    pub async fn upgrade_with(self, mut headers: HeaderMap) -> Result<Upgraded<S, E>, Error> {
        let WebSocketUpgrader {
            key,
            subprotocol,
            mut buf,
            mut stream,
            extension,
            extension_header,
            config,
            ..
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

        Ok(Upgraded {
            socket: WebSocket::from_upgraded(config, stream, extension, buf, Role::Server),
            subprotocol,
        })
    }

    pub async fn reject(self, response: WebSocketResponse) -> Result<(), Error> {
        let WebSocketResponse { status, headers } = response;
        let WebSocketUpgrader {
            mut stream,
            mut buf,
            ..
        } = self;
        write_response(&mut stream, &mut buf, status, headers, None).await
    }
}

#[derive(Debug)]
pub struct HandshakeResult<E> {
    key: Bytes,
    subprotocol: Option<String>,
    extension: E,
    request: Request,
    extension_header: Option<HeaderValue>,
}
