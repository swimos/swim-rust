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

mod client;
mod io;
#[allow(warnings)]
mod server;

use crate::errors::{Error, ProtocolError};
use crate::errors::{ErrorKind, HttpError};
use crate::handshake::io::BufferedIo;
use crate::{Request, Response};
pub use client::{exec_client_handshake, HandshakeResult};
use fnv::FnvHashSet;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::Uri;
use http::{header, HeaderMap, HeaderValue};
use httparse::Header;
use tokio::io::AsyncRead;
use url::Url;

const WEBSOCKET_STR: &str = "websocket";
const UPGRADE_STR: &str = "upgrade";
const WEBSOCKET_VERSION_STR: &str = "13";
const BAD_STATUS_CODE: &str = "Invalid status code";
const ACCEPT_KEY: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Default)]
pub struct ProtocolRegistry {
    registrants: FnvHashSet<&'static str>,
}

enum Bias {
    Left,
    Right,
}

impl ProtocolRegistry {
    pub fn new<I>(i: I) -> ProtocolRegistry
    where
        I: IntoIterator<Item = &'static str>,
    {
        ProtocolRegistry {
            registrants: i.into_iter().collect(),
        }
    }

    fn negotiate<'h, I>(&self, mut headers: I, bias: Bias) -> Result<Option<String>, ProtocolError>
    where
        I: Iterator<Item = &'h Header<'h>>,
    {
        while let Some(header) = headers.next() {
            let value =
                String::from_utf8(header.value.to_vec()).map_err(|_| ProtocolError::Encoding)?;
            let protocols = value
                .split(',')
                .map(|s| s.trim())
                .collect::<FnvHashSet<_>>();

            let selected = match bias {
                Bias::Left => {
                    if !self.registrants.is_superset(&protocols) {
                        return Err(ProtocolError::UnknownProtocol);
                    }
                    protocols
                        .intersection(&self.registrants)
                        .next()
                        .map(|s| s.to_string())
                }
                Bias::Right => self
                    .registrants
                    .intersection(&protocols)
                    .next()
                    .map(|s| s.to_string()),
            };

            match selected {
                Some(selected) => return Ok(Some(selected)),
                None => continue,
            }
        }

        Ok(None)
    }

    pub fn negotiate_response(
        &self,
        response: &httparse::Response,
    ) -> Result<Option<String>, ProtocolError> {
        let it = response
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL.as_str()));

        self.negotiate(it, Bias::Left)
    }

    pub fn negotiate_request(
        &self,
        request: &httparse::Request,
    ) -> Result<Option<String>, ProtocolError> {
        let it = request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL.as_str()));

        self.negotiate(it, Bias::Right)
    }
}

trait SubprotocolApplicator<Target> {
    fn apply_to(&self, target: &mut Target) -> Result<(), crate::Error>;
}

impl SubprotocolApplicator<HeaderMap> for ProtocolRegistry {
    fn apply_to(&self, target: &mut HeaderMap) -> Result<(), crate::Error> {
        if self.registrants.is_empty() {
            return Ok(());
        }

        let out = self
            .registrants
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");

        let header_value = HeaderValue::from_str(&out).map_err(|_| {
            crate::Error::with_cause(ErrorKind::Http, HttpError::MalformattedHeader)
        })?;

        target.insert(header::SEC_WEBSOCKET_PROTOCOL, header_value);
        Ok(())
    }
}

pub struct StreamingParser<'i, 'buf, I, P> {
    io: &'i mut BufferedIo<'buf, I>,
    parser: P,
}

impl<'i, 'buf, I, P> StreamingParser<'i, 'buf, I, P>
where
    I: AsyncRead + Unpin,
    P: Parser,
{
    pub fn new(io: &'i mut BufferedIo<'buf, I>, parser: P) -> StreamingParser<'i, 'buf, I, P> {
        StreamingParser { io, parser }
    }

    pub async fn parse(self) -> Result<P::Output, Error> {
        let StreamingParser { io, mut parser } = self;

        loop {
            io.read().await?;

            match parser.parse(io.buffer) {
                Ok(ParseResult::Complete(out, count)) => {
                    io.advance(count);
                    return Ok(out);
                }
                Ok(ParseResult::Partial) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

pub trait Parser {
    type Output;

    fn parse(&mut self, buf: &[u8]) -> Result<ParseResult<Self::Output>, Error>;
}

pub enum ParseResult<O> {
    Complete(O, usize),
    Partial,
}

impl SubprotocolApplicator<Response> for ProtocolRegistry {
    fn apply_to(&self, _target: &mut Response) -> Result<(), crate::Error> {
        todo!()
    }
}

pub trait TryIntoRequest {
    fn try_into_request(self) -> Result<Request, Error>;
}

impl<'a> TryIntoRequest for &'a str {
    fn try_into_request(self) -> Result<Request, Error> {
        self.parse::<Uri>()?.try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a String {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for String {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a Uri {
    fn try_into_request(self) -> Result<Request, Error> {
        self.clone().try_into_request()
    }
}

impl TryIntoRequest for Uri {
    fn try_into_request(self) -> Result<Request, Error> {
        Ok(Request::get(self).body(())?)
    }
}

impl<'a> TryIntoRequest for &'a Url {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Url {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Request {
    fn try_into_request(self) -> Result<Request, Error> {
        Ok(self)
    }
}
