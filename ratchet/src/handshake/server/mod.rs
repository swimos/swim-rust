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

#[cfg(test)]
mod tests;

use crate::handshake::io::BufferedIo;
use crate::handshake::{
    get_header, validate_header, validate_header_value, ParseResult, Parser, StreamingParser,
    ACCEPT_KEY, METHOD_GET, WEBSOCKET_VERSION_STR,
};
use crate::handshake::{TryMap, UPGRADE_STR, WEBSOCKET_STR};
use crate::protocol::Role;
use crate::{
    Error, ErrorKind, Extension, ExtensionProvider, HttpError, NoExt, NoExtProxy, ProtocolRegistry,
    Request, Upgraded, WebSocket, WebSocketConfig, WebSocketStream,
};
use bytes::{BufMut, Bytes, BytesMut};
use http::status::InvalidStatusCode;
use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use httparse::Status;
use sha1::{Digest, Sha1};
use std::convert::TryFrom;
use std::iter::FromIterator;
use tokio::io::AsyncWrite;

pub async fn accept<S>(
    stream: S,
    config: WebSocketConfig,
) -> Result<WebSocketUpgrader<S, NoExt>, Error>
where
    S: WebSocketStream,
{
    accept_with(stream, config, NoExtProxy, ProtocolRegistry::default()).await
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

async fn write_response<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    status: StatusCode,
    headers: HeaderMap,
    body: Option<String>,
) -> Result<(), Error>
where
    S: AsyncWrite + Unpin,
{
    buf.clear();

    let version_count = 9;
    let status_bytes = status.as_str().as_bytes();
    let reason_len = status.canonical_reason().map(|r| r.len() + 4).unwrap_or(2);
    let headers_len = headers.iter().fold(0, |count, (name, value)| {
        name.as_str().len() + value.len() + 2 + count
    });
    let terminator_len = if headers.is_empty() { 4 } else { 2 };
    let len = buf.len();

    buf.reserve(
        version_count + status_bytes.len() + reason_len + headers_len + terminator_len - len,
    );

    buf.put_slice(b"HTTP/1.1 ");
    buf.put_slice(status.as_str().as_bytes());

    match status.canonical_reason() {
        Some(reason) => buf.put_slice(format!(" {} \r\n", reason).as_bytes()),
        None => buf.put_slice(b"\r\n"),
    }

    for (name, value) in &headers {
        buf.put_slice(format!("{}: ", name).as_bytes());
        buf.put_slice(value.as_bytes());
        buf.put_slice(b"\r\n");
    }

    if let Some(body) = body {
        buf.put_slice(body.as_bytes());
    }

    if headers.is_empty() {
        buf.put_slice(b"\r\n\r\n");
    } else {
        buf.put_slice(b"\r\n");
    }

    let mut buffered = BufferedIo::new(stream, buf);
    buffered.write().await
}

#[derive(Debug)]
pub struct HandshakeResult<E> {
    key: Bytes,
    subprotocol: Option<String>,
    extension: E,
    request: Request,
    extension_header: Option<HeaderValue>,
}

struct RequestParser<E> {
    subprotocols: ProtocolRegistry,
    extension: E,
}

impl<E> Parser for RequestParser<E>
where
    E: ExtensionProvider,
{
    type Output = HandshakeResult<E::Extension>;

    fn parse(&mut self, buf: &[u8]) -> Result<ParseResult<Self::Output>, Error> {
        let RequestParser {
            subprotocols,
            extension,
        } = self;
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);

        match try_parse_request(buf, &mut request, extension, subprotocols)? {
            ParseResult::Complete(result, count) => Ok(ParseResult::Complete(result, count)),
            ParseResult::Partial => {
                check_partial_request(&request)?;
                Ok(ParseResult::Partial)
            }
        }
    }
}

fn try_parse_request<'l, E>(
    buffer: &'l [u8],
    request: &mut httparse::Request<'_, 'l>,
    extension: E,
    subprotocols: &mut ProtocolRegistry,
) -> Result<ParseResult<HandshakeResult<E::Extension>>, Error>
where
    E: ExtensionProvider,
{
    match request.parse(buffer) {
        Ok(Status::Complete(count)) => {
            parse_request(request, extension, subprotocols).map(|r| ParseResult::Complete(r, count))
        }
        Ok(Status::Partial) => Ok(ParseResult::Partial),
        Err(e) => Err(e.into()),
    }
}

fn check_partial_request(request: &httparse::Request) -> Result<(), Error> {
    match request.version {
        Some(1) | None => {}
        Some(v) => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpVersion(Some(v)),
            ))
        }
    }

    match request.method {
        Some(m) if m.eq_ignore_ascii_case(METHOD_GET) => {}
        None => {}
        m => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpMethod(m.map(ToString::to_string)),
            ));
        }
    }

    Ok(())
}

fn parse_request<E>(
    request: &mut httparse::Request<'_, '_>,
    extension: E,
    subprotocols: &mut ProtocolRegistry,
) -> Result<HandshakeResult<E::Extension>, Error>
where
    E: ExtensionProvider,
{
    match request.version {
        Some(1) => {}
        v => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpVersion(v),
            ))
        }
    }

    match request.method {
        Some(m) if m.eq_ignore_ascii_case(METHOD_GET) => {}
        m => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpMethod(m.map(ToString::to_string)),
            ));
        }
    }

    let headers = &request.headers;
    validate_header_value(headers, http::header::CONNECTION, UPGRADE_STR)?;
    validate_header_value(headers, http::header::UPGRADE, WEBSOCKET_STR)?;
    validate_header_value(
        headers,
        http::header::SEC_WEBSOCKET_VERSION,
        WEBSOCKET_VERSION_STR,
    )?;

    validate_header(headers, http::header::HOST, |_, _| Ok(()))?;

    let key = get_header(headers, http::header::SEC_WEBSOCKET_KEY)?;
    let (extension, extension_header) = extension
        .negotiate_server(request.headers)
        .map_err(Into::into)?;
    let subprotocol = subprotocols.negotiate_request(request)?;

    Ok(HandshakeResult {
        key,
        request: request.try_map()?,
        extension,
        subprotocol,
        extension_header,
    })
}
