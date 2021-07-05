mod encoding;
#[cfg(test)]
mod tests;

type Nonce = [u8; 24];

use crate::errors::{Error, ErrorKind, HttpError};
use crate::extensions::{ext::WebsocketExtension, Extension, ExtensionHandshake};
use crate::handshake::client::encoding::{build_request, encode_request};
use crate::handshake::io::BufferedIo;
use crate::handshake::{
    ExtensionRegistry, ProtocolRegistry, Registry, ACCEPT_KEY, BAD_STATUS_CODE, UPGRADE_STR,
    WEBSOCKET_STR,
};
use crate::{WebSocketConfig, WebSocketStream};
use bytes::{Buf, BytesMut};
use futures::future::ready;
use futures_util::FutureExt;
use http::header::HeaderName;
use http::{header, Request, StatusCode};
use httparse::{Response, Status};
use sha1::{Digest, Sha1};
use std::convert::TryFrom;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_native_tls::TlsConnector;
use tracing::{event, span, Level};
use tracing_futures::Instrument;

const MSG_HANDSHAKE_COMPLETED: &str = "Handshake completed";
const MSG_HANDSHAKE_FAILED: &str = "Handshake failed";
const MSG_HANDSHAKE_START: &str = "Executing client handshake";

pub async fn exec_client_handshake<S, E>(
    _config: &WebSocketConfig,
    stream: &mut S,
    _connector: Option<TlsConnector>,
    request: Request<()>,
    extension: E,
) -> Result<HandshakeResult<E::Extension>, Error>
where
    S: WebSocketStream,
    E: ExtensionHandshake,
{
    let machine = HandshakeMachine::new(stream, Vec::new(), extension);
    let uri = request.uri();
    let span = span!(Level::DEBUG, MSG_HANDSHAKE_START, ?uri);

    let exec = async move {
        let handshake_result = machine.exec(request).await;
        match &handshake_result {
            Ok(HandshakeResult {
                protocol,
                extension,
            }) => {
                event!(Level::DEBUG, MSG_HANDSHAKE_COMPLETED, ?protocol, ?extension)
            }
            Err(e) => {
                event!(Level::ERROR, MSG_HANDSHAKE_FAILED, error = ?e)
            }
        }

        handshake_result
    };

    exec.instrument(span).await
}

struct HandshakeMachine<'s, S, E> {
    buffered: BufferedIo<'s, S>,
    nonce: Nonce,
    subprotocols: Vec<&'static str>,
    extension: E,
}

impl<'s, S, E> HandshakeMachine<'s, S, E>
where
    S: WebSocketStream,
    E: ExtensionHandshake,
{
    pub fn new(
        socket: &'s mut S,
        subprotocols: Vec<&'static str>,
        extension: E,
    ) -> HandshakeMachine<'s, S, E> {
        HandshakeMachine {
            buffered: BufferedIo::new(socket, BytesMut::new()),
            nonce: [0; 24],
            subprotocols,
            extension,
        }
    }

    fn encode(&mut self, mut request: Request<()>) -> Result<(), Error> {
        let HandshakeMachine {
            buffered, nonce, ..
        } = self;

        build_request(&mut request)?;
        encode_request(&mut buffered.buffer, request, nonce);
        Ok(())
    }

    async fn write(&mut self) -> Result<(), Error> {
        self.buffered.write().await
    }

    fn clear_buffer(&mut self) {
        self.buffered.clear();
    }

    async fn read(&mut self) -> Result<HandshakeResult<E::Extension>, Error> {
        let HandshakeMachine {
            buffered,
            nonce,
            subprotocols,
            extension,
        } = self;

        loop {
            buffered.read().await?;

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut response = httparse::Response::new(&mut headers);

            match try_parse_response(&buffered.buffer, &mut response, nonce, extension)? {
                ParseResult::Complete(result, count) => {
                    buffered.advance(count);
                    break Ok(result);
                }
                ParseResult::Partial => check_partial_response(&response)?,
            }
        }
    }

    // This is split up on purpose so that the individual functions can be called in unit tests.
    pub async fn exec(
        mut self,
        request: Request<()>,
    ) -> Result<HandshakeResult<E::Extension>, Error> {
        self.encode(request)?;
        self.write().await?;
        self.clear_buffer();
        self.read().await
    }
}

#[derive(Debug, PartialEq)]
pub struct HandshakeResult<E> {
    protocol: &'static str,
    extension: E,
}

/// Quickly checks a partial response in the order of the expected HTTP response declaration to see
/// if it's valid.
fn check_partial_response(response: &Response) -> Result<(), Error> {
    match response.version {
        // httparse sets this to 0 for HTTP/1.0 or 1 for HTTP/1.1
        // rfc6455 § 4.2.1.1: must be HTTP/1.1 or higher
        Some(1) | None => {}
        Some(v) => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpVersion(Some(v)),
            ))
        }
    }

    match response.code {
        Some(code) if code == StatusCode::SWITCHING_PROTOCOLS => Ok(()),
        Some(code) if (300..400).contains(&code) => {
            // This keeps the response parsing going until the resource is found and then the
            // upgrade will fail with the redirection location
            Ok(())
        }
        Some(code) => match StatusCode::try_from(code) {
            Ok(code) => Err(Error::with_cause(ErrorKind::Http, HttpError::Status(code))),
            Err(_) => Err(Error::with_cause(ErrorKind::Http, BAD_STATUS_CODE)),
        },
        None => Ok(()),
    }
}

enum ParseResult<E> {
    Complete(HandshakeResult<E>, usize),
    Partial,
}

fn try_parse_response<'l, E>(
    buffer: &'l BytesMut,
    response: &mut Response<'_, 'l>,
    expected_nonce: &Nonce,
    extension: &E,
) -> Result<ParseResult<E::Extension>, Error>
where
    E: ExtensionHandshake,
{
    match response.parse(buffer.as_ref()) {
        Ok(Status::Complete(count)) => parse_response(response, expected_nonce, extension)
            .map(|r| ParseResult::Complete(r, count)),
        Ok(Status::Partial) => Ok(ParseResult::Partial),
        Err(e) => Err(e.into()),
    }
}

fn parse_response<E>(
    response: &Response,
    expected_nonce: &Nonce,
    extension: &E,
) -> Result<HandshakeResult<E::Extension>, Error>
where
    E: ExtensionHandshake,
{
    match response.version {
        // rfc6455 § 4.2.1.1: must be HTTP/1.1 or higher
        Some(1) => {}
        v => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::HttpVersion(v),
            ))
        }
    }

    let raw_status_code = response.code.ok_or(Error::new(ErrorKind::Http))?;
    let status_code = StatusCode::from_u16(raw_status_code)?;
    match status_code {
        c if c == StatusCode::SWITCHING_PROTOCOLS => {}
        c if c.is_redirection() => {
            match response.headers.iter().find(|h| h.name == header::LOCATION) {
                Some(header) => {
                    // the value _should_ be valid UTF-8
                    let location = String::from_utf8(header.value.to_vec())
                        .map_err(|_| Error::new(ErrorKind::Http))?;
                    return Err(Error::with_cause(
                        ErrorKind::Http,
                        HttpError::Redirected(location),
                    ));
                }
                None => return Err(Error::with_cause(ErrorKind::Http, HttpError::Status(c))),
            }
        }
        status_code => {
            return Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::Status(status_code),
            ))
        }
    }

    validate_header_value(response.headers, header::UPGRADE, WEBSOCKET_STR.as_bytes())?;
    validate_header_value(response.headers, header::CONNECTION, UPGRADE_STR.as_bytes())?;

    validate_header(
        response.headers,
        header::SEC_WEBSOCKET_ACCEPT,
        |_name, actual| {
            let mut digest = Sha1::new();
            digest.update(expected_nonce);
            digest.update(ACCEPT_KEY);

            let expected = base64::encode(&digest.finalize());
            if expected.as_bytes() != actual {
                return Err(Error::with_cause(ErrorKind::Http, HttpError::KeyMismatch));
            } else {
                Ok(())
            }
        },
    )?;

    // todo extensions
    let protocols = response.headers.iter().find(|h| {
        h.name
            .eq_ignore_ascii_case(header::SEC_WEBSOCKET_PROTOCOL.as_str())
    });

    if let Some(protocols) = protocols {
        println!("{:?}", protocols.value);
    }

    // todo protocol
    //
    Ok(HandshakeResult {
        protocol: "",
        extension: extension.negotiate(response)?,
    })
}

fn validate_header_value(
    headers: &[httparse::Header],
    name: HeaderName,
    expected: &[u8],
) -> Result<(), Error> {
    validate_header(headers, name, |name, actual| {
        if actual == expected {
            Ok(())
        } else {
            Err(Error::with_cause(
                ErrorKind::Http,
                HttpError::InvalidHeader(name),
            ))
        }
    })
}

fn validate_header<F>(headers: &[httparse::Header], name: HeaderName, f: F) -> Result<(), Error>
where
    F: Fn(HeaderName, &[u8]) -> Result<(), Error>,
{
    match headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name.as_str()))
    {
        Some(header) => f(name, header.value),
        None => Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::MissingHeader(name),
        )),
    }
}
