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

use crate::{Error, ErrorKind, HttpError, ProtocolError};
use fnv::FnvHashSet;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::{HeaderMap, HeaderValue};
use httparse::Header;
use std::borrow::Cow;

/// A subprotocol registry that is used for negotiating a possible subprotocol to use for a
/// connection.
#[derive(Default, Debug, Clone)]
pub struct ProtocolRegistry {
    registrants: FnvHashSet<Cow<'static, str>>,
    header: Option<HeaderValue>,
}

impl ProtocolRegistry {
    /// Construct a new protocol registry that will allow the provided items.
    pub fn new<I>(i: I) -> Result<ProtocolRegistry, Error>
    where
        I: IntoIterator,
        I::Item: Into<Cow<'static, str>>,
    {
        let registrants = i
            .into_iter()
            .map(Into::into)
            .collect::<FnvHashSet<Cow<'static, str>>>();
        let header_str = registrants
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let header = HeaderValue::from_str(&header_str).map_err(|_| {
            crate::Error::with_cause(ErrorKind::Http, HttpError::MalformattedHeader(header_str))
        })?;

        Ok(ProtocolRegistry {
            registrants,
            header: Some(header),
        })
    }
}

enum Bias {
    Client,
    Server,
}

fn negotiate<'h, I>(
    registry: &ProtocolRegistry,
    headers: I,
    bias: Bias,
) -> Result<Option<String>, ProtocolError>
where
    I: Iterator<Item = &'h Header<'h>>,
{
    for header in headers {
        let value =
            String::from_utf8(header.value.to_vec()).map_err(|_| ProtocolError::Encoding)?;
        let protocols = value
            .split(',')
            .map(|s| s.trim().into())
            .collect::<FnvHashSet<_>>();

        let selected = match bias {
            Bias::Client => {
                if !registry.registrants.is_superset(&protocols) {
                    return Err(ProtocolError::UnknownProtocol);
                }
                protocols
                    .intersection(&registry.registrants)
                    .next()
                    .map(|s| s.to_string())
            }
            Bias::Server => registry
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
    registry: &ProtocolRegistry,
    response: &httparse::Response,
) -> Result<Option<String>, ProtocolError> {
    let it = response
        .headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL.as_str()));

    negotiate(registry, it, Bias::Client)
}

pub fn negotiate_request(
    registry: &ProtocolRegistry,
    request: &httparse::Request,
) -> Result<Option<String>, ProtocolError> {
    let it = request
        .headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case(SEC_WEBSOCKET_PROTOCOL.as_str()));

    negotiate(registry, it, Bias::Server)
}

pub fn apply_to(registry: &ProtocolRegistry, target: &mut HeaderMap) {
    if let Some(header) = &registry.header {
        target.insert(SEC_WEBSOCKET_PROTOCOL, header.clone());
    }
}
