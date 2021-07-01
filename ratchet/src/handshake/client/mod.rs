mod encoding;
#[cfg(test)]
mod tests;

type Nonce = [u8; 24];

use crate::errors::{Error, ErrorKind, HttpError};
use crate::extensions::Extension;
use crate::handshake::client::encoding::{build_request, encode_request};
use crate::handshake::{
    ExtensionRegistry, ProtocolRegistry, Registry, ACCEPT_KEY, BAD_STATUS_CODE, UPGRADE_STR,
    WEBSOCKET_STR,
};
use crate::{WebSocketConfig, WebSocketStream};
use bytes::{Buf, BytesMut};
use http::header::HeaderName;
use http::{header, Request, StatusCode};
use httparse::{Response, Status};
use sha1::{Digest, Sha1};
use std::convert::TryFrom;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_native_tls::TlsConnector;

// todo error handling need to be tidied up

pub async fn exec_client_handshake<S>(
    _config: &WebSocketConfig,
    stream: &mut S,
    _connector: Option<TlsConnector>,
    request: Request<()>,
) -> Result<(), Error>
where
    S: WebSocketStream,
{
    let machine = HandshakeMachine::new(stream, Vec::new(), Vec::new());
    machine.exec(request).await
}

struct BufferedIo<'s, S> {
    socket: &'s mut S,
    buffer: BytesMut,
}

impl<'s, S> AsRef<BytesMut> for BufferedIo<'s, S> {
    fn as_ref(&self) -> &BytesMut {
        &self.buffer
    }
}

impl<'s, S> BufferedIo<'s, S>
where
    S: WebSocketStream,
{
    fn new(socket: &'s mut S, buffer: BytesMut) -> BufferedIo<'s, S> {
        BufferedIo { socket, buffer }
    }

    async fn write(&mut self) -> Result<(), Error> {
        let BufferedIo { socket, buffer } = self;

        socket.write_all(&buffer).await?;
        socket.flush().await?;

        Ok(())
    }

    async fn read(&mut self) -> Result<(), Error> {
        let BufferedIo { socket, buffer } = self;

        let len = buffer.len();
        buffer.resize(len + (8 * 1024), 0);
        let read_count = socket.read(&mut buffer[len..]).await?;
        buffer.truncate(len + read_count);

        Ok(())
    }

    fn advance(&mut self, count: usize) {
        self.buffer.advance(count);
    }

    fn clear(&mut self) {
        self.buffer.clear();
    }
}

struct HandshakeMachine<'s, S> {
    buffered: BufferedIo<'s, S>,
    nonce: Nonce,
    subprotocols: Vec<&'static str>,
    extensions: Vec<&'static str>,
}

impl<'s, S> HandshakeMachine<'s, S>
where
    S: WebSocketStream,
{
    pub fn new(
        socket: &'s mut S,
        subprotocols: Vec<&'static str>,
        extensions: Vec<&'static str>,
    ) -> HandshakeMachine<'s, S> {
        HandshakeMachine {
            buffered: BufferedIo::new(socket, BytesMut::new()),
            nonce: [0; 24],
            subprotocols,
            extensions,
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

    async fn read(&mut self) -> Result<(), Error> {
        let HandshakeMachine {
            buffered, nonce, ..
        } = self;

        loop {
            buffered.read().await?;

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut response = httparse::Response::new(&mut headers);

            match try_parse_response(buffered.as_ref(), &mut response, &nonce)? {
                ParseResult::Complete(count) => {
                    buffered.advance(count);
                    break Ok(());
                }
                ParseResult::Partial => check_partial_response(&response)?,
            }
        }
    }

    // This is split up on purpose so that the individual functions can be called in unit tests.
    pub async fn exec(mut self, request: Request<()>) -> Result<(), Error> {
        self.encode(request)?;
        self.write().await?;
        self.clear_buffer();
        self.read().await
    }
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

enum ParseResult {
    Complete(usize),
    Partial,
}

fn try_parse_response<'l>(
    buffer: &'l BytesMut,
    response: &mut Response<'_, 'l>,
    expected_nonce: &Nonce,
) -> Result<ParseResult, Error> {
    match response.parse(buffer.as_ref()) {
        Ok(Status::Complete(count)) => {
            parse_response(response, expected_nonce).map(|status| ParseResult::Complete(count))
        }
        Ok(Status::Partial) => Ok(ParseResult::Partial),
        Err(e) => Err(e.into()),
    }
}

fn parse_response(response: &Response, expected_nonce: &Nonce) -> Result<(), Error> {
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
            unimplemented!()
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
    // todo protocol

    Ok(())
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
    match headers.iter().find(|h| h.name == name) {
        Some(header) => f(name, header.value),
        None => Err(Error::with_cause(
            ErrorKind::Http,
            HttpError::MissingHeader(name),
        )),
    }
}
