// Copyright 2015-2023 Swim Inc.
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

use std::str::FromStr;

use mime::Mime;
use swimos_model::http::{Header, HeaderValue, StandardHeaderName};
use tracing::warn;

#[cfg(test)]
mod tests;

/// Utility to read header values from a [`swimos_model::http::HttpRequest`].
pub struct Headers<'a> {
    headers: &'a [Header],
}

impl<'a> Headers<'a> {
    pub fn new(headers: &'a [Header]) -> Self {
        Headers { headers }
    }
}

#[derive(Debug)]
pub struct InvalidHeader;

impl<'a> Headers<'a> {
    /// Get the content type specified for a request, if present.
    pub fn content_type(&self) -> Result<Option<Mime>, InvalidHeader> {
        let Headers { headers } = self;
        if let Some(header) = headers
            .iter()
            .find(|Header { name, .. }| name == &StandardHeaderName::ContentType)
        {
            if let Some(value) = header.value.as_str() {
                match Mime::from_str(value) {
                    Ok(mime) => Ok(Some(mime)),
                    Err(error) => {
                        warn!(header_value = %value, error = %error, "Invalid content type in request.");
                        Err(InvalidHeader)
                    }
                }
            } else {
                warn!("Non-string content type header in request.");
                Err(InvalidHeader)
            }
        } else {
            Ok(None)
        }
    }

    /// Get an iterator over content types from all accept headers. The iterator will
    /// produce an error for an invalid header value/content type but will not terminate
    /// until all accept headers have been exhausted.
    pub fn accept(&self) -> impl Iterator<Item = Result<Mime, InvalidHeader>> + '_ {
        let Headers { headers } = self;
        headers
            .iter()
            .filter(|Header { name, .. }| name == &StandardHeaderName::Accept)
            .map(|Header { value, .. }| value)
            .flat_map(extract_accept)
    }
}

#[derive(Default)]
enum AcceptState<'a, I> {
    Init(&'a HeaderValue),
    ConsumingPart(I),
    #[default]
    Done,
}

fn extract_accept(value: &HeaderValue) -> impl Iterator<Item = Result<Mime, InvalidHeader>> + '_ {
    let mut state = AcceptState::Init(value);
    std::iter::from_fn(move || loop {
        match std::mem::take(&mut state) {
            AcceptState::Init(value) => {
                if let Some(value_str) = value.as_str() {
                    let it = value_str
                        .split(';')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty());
                    state = AcceptState::ConsumingPart(it);
                } else {
                    state = AcceptState::Done;
                    break Some(Err(InvalidHeader));
                }
            }
            AcceptState::ConsumingPart(mut it) => match it.next() {
                Some(part) => {
                    state = AcceptState::ConsumingPart(it);
                    break Some(Mime::from_str(part).map_err(|_| InvalidHeader));
                }
                _ => break None,
            },
            AcceptState::Done => break None,
        }
    })
}

/// Create a content-type header for a [`swimos_model::http::HttpResponse`].
pub fn content_type_header(mime: &Mime) -> Header {
    Header {
        name: StandardHeaderName::ContentType.into(),
        value: HeaderValue::from(mime.as_ref()),
    }
}

///Add a content-length header to a [`swimos_model::http::HttpResponse`].
pub fn add_content_len_header<B: AsRef<[u8]>>(request: &mut swimos_model::http::HttpResponse<B>) {
    let len = request.payload.as_ref().len();
    let header = Header {
        name: StandardHeaderName::ContentLength.into(),
        value: HeaderValue::from(len.to_string()),
    };
    request.headers.push(header);
}
