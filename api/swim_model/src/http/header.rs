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

use bytes::Bytes;

use crate::BytesStr;


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderName(Name);


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderValue(HeaderValueInner);

#[derive(Debug, Clone, PartialEq, Eq)]
enum HeaderValueInner {
    StringHeader(BytesStr),
    BytesHeader(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: HeaderName,
    pub value: HeaderValue,
}

impl HeaderName {

    pub fn as_str(&self) -> &str {
        self.0.str_value()
    }

}

impl HeaderValue {

    pub fn new(bytes: Bytes) -> Self {
        HeaderValue(match BytesStr::new(bytes) {
            Ok(bytes_str) => HeaderValueInner::StringHeader(bytes_str),
            Err(bytes) => HeaderValueInner::BytesHeader(bytes),
        })
    }

    pub fn as_str(&self) -> Option<&str> {
        match &self.0 {
            HeaderValueInner::StringHeader(s) => Some(s.as_str()),
            HeaderValueInner::BytesHeader(_) => None,
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self.0 {
            HeaderValueInner::StringHeader(s) => s.into(),
            HeaderValueInner::BytesHeader(b) => b,
        }
    }

}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        HeaderValue(HeaderValueInner::StringHeader(BytesStr::from(value)))
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        HeaderValue(HeaderValueInner::StringHeader(BytesStr::from(value)))
    }
}

impl From<Bytes> for HeaderValue {
    fn from(value: Bytes) -> Self {
        HeaderValue::new(value)
    }
}

impl From<Vec<u8>> for HeaderValue {
    fn from(value: Vec<u8>) -> Self {
        HeaderValue::new(Bytes::from(value))
    }
}

impl From<&[u8]> for HeaderValue {
    fn from(value: &[u8]) -> Self {
        HeaderValue::new(Bytes::from(value.to_vec()))
    }
}

impl HeaderName {

    pub fn new(name: BytesStr) -> Self {
        if let Some(basic) = Name::try_from_basic(name.as_str()) {
            HeaderName(basic)
        } else {
            HeaderName(Name::Other(name))
        }
    }

}

impl From<String> for HeaderName {
    fn from(value: String) -> Self {
        HeaderName::new(BytesStr::from(value))
    }
}

impl From<&str> for HeaderName {
    fn from(value: &str) -> Self {
        if let Some(basic) = Name::try_from_basic(value) {
            HeaderName(basic)
        } else {
            HeaderName(Name::Other(BytesStr::from(value)))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Name {
    Accept,
    AcceptCharset,
    AcceptEncoding,
    AcceptLanguage,
    Allow,
    Connection,
    ContentEncoding,
    ContentLength,
    ContentType,
    Cookie,
    Expect,
    Host,
    Location,
    MaxForwards,
    Origin,
    SecWebSocketAccept,
    SecWebSocketExtensions,
    SecWebSocketKey,
    SecWebSocketProtocol,
    SecWebSocketVersion,
    Server,
    SetCookie,
    TransferEncoding,
    Upgrade,
    UserAgent,
    Other(BytesStr),
}

impl Name {

    fn try_from_basic(s: &str) -> Option<Self> {
        match s {
            "accept" => Some(Name::Accept),
            "accept-charset" => Some(Name::AcceptCharset),
            "accept-encoding" => Some(Name::AcceptEncoding),
            "accept-language" => Some(Name::AcceptLanguage),
            "allow" => Some(Name::Allow),
            "connection" => Some(Name::Connection),
            "content-encoding" => Some(Name::ContentEncoding),
            "content-length" => Some(Name::ContentLength),
            "content-type" => Some(Name::ContentType),
            "cookie" => Some(Name::Cookie),
            "expect" => Some(Name::Expect),
            "host" => Some(Name::Host),
            "location" => Some(Name::Location),
            "max-forwards" => Some(Name::MaxForwards),
            "origin" => Some(Name::Origin),
            "sec-websocket-accept" => Some(Name::SecWebSocketAccept),
            "sec-websocket-extensions" => Some(Name::SecWebSocketExtensions),
            "sec-websocket-key" => Some(Name::SecWebSocketKey),
            "sec-websocket-protocol" => Some(Name::SecWebSocketProtocol),
            "sec-websocket-version" => Some(Name::SecWebSocketVersion),
            "server" => Some(Name::Server),
            "set-cookie" => Some(Name::SetCookie),
            "transfer-encoding" => Some(Name::TransferEncoding),
            "upgrade" => Some(Name::Upgrade),
            "user-agent" => Some(Name::UserAgent),
            _ => None,
        }
    }

    fn str_value(&self) -> &str {
        match self {
            Name::Accept => "accept",
            Name::AcceptCharset => "accept-charset",
            Name::AcceptEncoding => "accept-encoding",
            Name::AcceptLanguage => "accept-language",
            Name::Allow => "allow",
            Name::Connection => "connection",
            Name::ContentEncoding => "content-encoding",
            Name::ContentLength => "content-length",
            Name::ContentType => "content-type",
            Name::Cookie => "cookie",
            Name::Expect => "expect",
            Name::Host => "host",
            Name::Location => "location",
            Name::MaxForwards => "max-forwards",
            Name::Origin => "origin",
            Name::SecWebSocketAccept => "sec-websocket-accept",
            Name::SecWebSocketExtensions => "sec-websocket-extensions",
            Name::SecWebSocketKey => "sec-websocket-key",
            Name::SecWebSocketProtocol => "sec-websocket-protocol",
            Name::SecWebSocketVersion => "sec-websocket-version",
            Name::Server => "server",
            Name::SetCookie => "set-cookie",
            Name::TransferEncoding => "transfer-encoding",
            Name::Upgrade => "upgrade",
            Name::UserAgent => "user-agent",
            Name::Other(name) => name.as_str(),
        }
    }

}

impl From<http::HeaderValue> for HeaderValue {
    fn from(value: http::HeaderValue) -> Self {
        HeaderValue::from(value.as_bytes())
    }
}

impl TryFrom<HeaderValue> for http::HeaderValue {
    type Error = http::header::InvalidHeaderValue;

    fn try_from(value: HeaderValue) -> Result<Self, Self::Error> {
        http::HeaderValue::from_maybe_shared(value.into_bytes())
    }
}

impl From<http::header::HeaderName> for HeaderName {
    fn from(value: http::header::HeaderName) -> Self {
        HeaderName::from(value.as_str())
    }
}

impl TryFrom<HeaderName> for http::header::HeaderName {
    type Error = http::header::InvalidHeaderName;

    fn try_from(value: HeaderName) -> Result<Self, Self::Error> {
        http::header::HeaderName::from_str(value.as_str())
    }
}

