mod client;
mod io;
mod server;

use crate::extensions::Extension;
pub use client::exec_client_handshake;
use fnv::FnvHashSet;
use http::header::{HeaderName, SEC_WEBSOCKET_PROTOCOL};
use httparse::Header;
use std::collections::HashSet;
use std::error::Error;
use std::str::Utf8Error;
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
}

#[derive(Default)]
pub struct ProtocolRegistry {
    registrants: FnvHashSet<(&'static str)>,
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

            if !self.registrants.is_superset(&protocols) {
                return Err(ProtocolError::UnknownProtocol);
            }

            let selected = match bias {
                Bias::Left => protocols
                    .intersection(&self.registrants)
                    .next()
                    .map(|s| s.to_string()),
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
        let mut it = request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL.as_str()));

        self.negotiate(it, Bias::Right)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.registrants.contains(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::handshake::{ProtocolError, ProtocolRegistry};
    use http::header::SEC_WEBSOCKET_PROTOCOL;
    use httparse::Header;

    #[test]
    fn selects_protocol_ok() {
        let mut headers = [httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warp, warps",
        }];
        let mut request = httparse::Request::new(&mut headers);

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
        let mut request = httparse::Request::new(&mut headers);

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
        let mut request = httparse::Request::new(&mut headers);

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
        let mut request = httparse::Request::new(&mut headers);

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
        let mut request = httparse::Request::new(&mut headers);

        let registry = ProtocolRegistry::new(vec!["d"]);
        assert_eq!(registry.negotiate_request(&request), Ok(None));
    }
}
