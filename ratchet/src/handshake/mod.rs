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

mod client;
mod io;
#[allow(warnings)]
mod server;

use crate::errors::{ErrorKind, HttpError};
use crate::protocol::frame::OpCodeParseErr;
use crate::{Request, Response};
pub use client::{exec_client_handshake, HandshakeResult};
use fnv::FnvHashSet;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::{header, HeaderValue};
use httparse::Header;
use thiserror::Error;

const WEBSOCKET_STR: &str = "websocket";
const UPGRADE_STR: &str = "upgrade";
const WEBSOCKET_VERSION_STR: &str = "13";
const WEBSOCKET_VERSION: u8 = 13;
const BAD_STATUS_CODE: &str = "Invalid status code";

const ACCEPT_KEY: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, PartialEq, Error)]
pub enum ProtocolError {
    #[error("Not valid UTF-8 encoding")]
    Encoding,
    #[error("Received an unknown subprotocol")]
    UnknownProtocol,
    #[error("Bad OpCode: `{0}`")]
    OpCode(OpCodeParseErr),
    #[error("Received an unexpected unmasked frame")]
    UnmaskedFrame,
    #[error("Received an unexpected masked frame")]
    MaskedFrame,
    #[error("Received a fragmented control frame")]
    FragmentedControl,
    #[error("A frame exceeded the maximum permitted size")]
    FrameOverflow,
    #[error("Attempted to use an extension that has not been negotiated")]
    UnknownExtension,
}

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

impl SubprotocolApplicator<Request> for ProtocolRegistry {
    fn apply_to(&self, target: &mut Request) -> Result<(), crate::Error> {
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

        target
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, header_value);
        Ok(())
    }
}

impl SubprotocolApplicator<Response> for ProtocolRegistry {
    fn apply_to(&self, _target: &mut Response) -> Result<(), crate::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::handshake::{ProtocolError, ProtocolRegistry};
    use http::header::SEC_WEBSOCKET_PROTOCOL;

    #[test]
    fn selects_protocol_ok() {
        let mut headers = [httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warp, warps",
        }];
        let request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["warps", "warp"]);
        assert_eq!(
            registry.negotiate_request(&request),
            Ok(Some("warp".to_string()))
        );
    }

    #[test]
    fn multiple_headers() {
        let mut headers = [
            httparse::Header {
                name: SEC_WEBSOCKET_PROTOCOL.as_str(),
                value: b"warp",
            },
            httparse::Header {
                name: SEC_WEBSOCKET_PROTOCOL.as_str(),
                value: b"warps",
            },
        ];
        let request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["warps", "warp"]);
        assert_eq!(
            registry.negotiate_request(&request),
            Ok(Some("warp".to_string()))
        );
    }

    #[test]
    fn mixed_headers() {
        let mut headers = [
            httparse::Header {
                name: SEC_WEBSOCKET_PROTOCOL.as_str(),
                value: b"warp1.0",
            },
            httparse::Header {
                name: SEC_WEBSOCKET_PROTOCOL.as_str(),
                value: b"warps2.0,warp3.0",
            },
            httparse::Header {
                name: SEC_WEBSOCKET_PROTOCOL.as_str(),
                value: b"warps4.0",
            },
        ];
        let request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["warps", "warp", "warps2.0"]);
        assert_eq!(
            registry.negotiate_request(&request),
            Ok(Some("warps2.0".to_string()))
        );
    }

    #[test]
    fn malformatted() {
        let mut headers = [httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: &[255, 255, 255, 255],
        }];
        let request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["warps", "warp", "warps2.0"]);
        assert_eq!(
            registry.negotiate_request(&request),
            Err(ProtocolError::Encoding)
        );
    }

    #[test]
    fn no_match() {
        let mut headers = [httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"a,b,c",
        }];
        let request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["d"]);
        assert_eq!(registry.negotiate_request(&request), Ok(None));
    }
}
