// Copyright 2015-2024 Swim Inc.
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

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    str::FromStr,
};

use bytes::Bytes;
use swimos_utilities::encoding::BytesStr;

/// Model for the name of an HTTP header. The representation of this type will either be an enumeration
/// of the standard header names or a general ASCII string for custom headers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HeaderName(Name);

/// Model for the value of an HTTP header. The representation of this type is an array of bytes that
/// may, or may not, be a valid UTF8 string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderValue(HeaderValueInner);

#[derive(Debug, Clone, PartialEq, Eq)]
enum HeaderValueInner {
    StringHeader(BytesStr),
    BytesHeader(Bytes),
}

/// Mode of an HTTP header, used by [`super::HttpRequest`] and [`super::HttpResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: HeaderName,
    pub value: HeaderValue,
}

impl Header {
    /// Create a header from anything that can be converted into header names and header values.
    pub fn new<N, V>(name: N, value: V) -> Self
    where
        N: Into<HeaderName>,
        V: Into<HeaderValue>,
    {
        Header {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl HeaderName {
    pub fn as_str(&self) -> &str {
        self.0.str_value()
    }
}

impl HeaderValue {
    pub fn new(bytes: Bytes) -> Self {
        HeaderValue(match BytesStr::try_wrap(bytes) {
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

    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            HeaderValueInner::StringHeader(s) => s.as_str().as_bytes(),
            HeaderValueInner::BytesHeader(b) => b.as_ref(),
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

impl From<(String, String)> for Header {
    fn from(value: (String, String)) -> Self {
        let (k, v) = value;
        Header {
            name: HeaderName::from(k),
            value: HeaderValue::from(v),
        }
    }
}

impl From<(StandardHeaderName, String)> for Header {
    fn from(value: (StandardHeaderName, String)) -> Self {
        let (k, v) = value;
        Header {
            name: HeaderName(Name::Standard(k)),
            value: HeaderValue::from(v),
        }
    }
}

impl From<(&str, &str)> for Header {
    fn from(value: (&str, &str)) -> Self {
        let (k, v) = value;
        Header {
            name: HeaderName::from(k),
            value: HeaderValue::from(v),
        }
    }
}

impl From<(StandardHeaderName, &str)> for Header {
    fn from(value: (StandardHeaderName, &str)) -> Self {
        let (k, v) = value;
        Header {
            name: HeaderName(Name::Standard(k)),
            value: HeaderValue::from(v),
        }
    }
}

impl HeaderName {
    pub fn new(name: BytesStr) -> Self {
        if let Some(basic) = StandardHeaderName::try_from_basic(name.as_str()) {
            HeaderName(Name::Standard(basic))
        } else {
            HeaderName(Name::Other(name))
        }
    }

    pub fn standard_name(&self) -> Option<StandardHeaderName> {
        if let Name::Standard(s) = &self.0 {
            Some(*s)
        } else {
            None
        }
    }
}

impl From<StandardHeaderName> for HeaderName {
    fn from(value: StandardHeaderName) -> Self {
        HeaderName(Name::Standard(value))
    }
}

impl From<String> for HeaderName {
    fn from(value: String) -> Self {
        HeaderName::new(BytesStr::from(value))
    }
}

impl From<&str> for HeaderName {
    fn from(value: &str) -> Self {
        if let Some(basic) = StandardHeaderName::try_from_basic(value) {
            HeaderName(Name::Standard(basic))
        } else {
            HeaderName(Name::Other(BytesStr::from(value)))
        }
    }
}

/// An enumeration of standard header names.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandardHeaderName {
    Accept = 1,
    AcceptCharset = 2,
    AcceptEncoding = 3,
    AcceptLanguage = 4,
    Allow = 5,
    Connection = 6,
    ContentEncoding = 7,
    ContentLength = 8,
    ContentType = 9,
    Cookie = 10,
    Expect = 11,
    Host = 12,
    Location = 13,
    MaxForwards = 14,
    Origin = 15,
    SecWebSocketAccept = 16,
    SecWebSocketExtensions = 17,
    SecWebSocketKey = 18,
    SecWebSocketProtocol = 19,
    SecWebSocketVersion = 20,
    Server = 21,
    SetCookie = 22,
    TransferEncoding = 23,
    Upgrade = 24,
    UserAgent = 25,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Name {
    Standard(StandardHeaderName),
    Other(BytesStr),
}

impl From<StandardHeaderName> for Name {
    fn from(value: StandardHeaderName) -> Self {
        Name::Standard(value)
    }
}

impl Name {
    pub fn str_value(&self) -> &str {
        match self {
            Name::Standard(s) => s.str_value(),
            Name::Other(s) => s.as_str(),
        }
    }
}

impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Name {
    fn cmp(&self, other: &Self) -> Ordering {
        self.str_value().cmp(other.str_value())
    }
}

impl Hash for Name {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.str_value().hash(state)
    }
}

impl PartialEq<StandardHeaderName> for HeaderName {
    fn eq(&self, other: &StandardHeaderName) -> bool {
        match &self.0 {
            Name::Standard(h) => h == other,
            Name::Other(_) => false,
        }
    }
}

impl StandardHeaderName {
    fn try_from_basic(s: &str) -> Option<Self> {
        match s {
            "accept" => Some(StandardHeaderName::Accept),
            "accept-charset" => Some(StandardHeaderName::AcceptCharset),
            "accept-encoding" => Some(StandardHeaderName::AcceptEncoding),
            "accept-language" => Some(StandardHeaderName::AcceptLanguage),
            "allow" => Some(StandardHeaderName::Allow),
            "connection" => Some(StandardHeaderName::Connection),
            "content-encoding" => Some(StandardHeaderName::ContentEncoding),
            "content-length" => Some(StandardHeaderName::ContentLength),
            "content-type" => Some(StandardHeaderName::ContentType),
            "cookie" => Some(StandardHeaderName::Cookie),
            "expect" => Some(StandardHeaderName::Expect),
            "host" => Some(StandardHeaderName::Host),
            "location" => Some(StandardHeaderName::Location),
            "max-forwards" => Some(StandardHeaderName::MaxForwards),
            "origin" => Some(StandardHeaderName::Origin),
            "sec-websocket-accept" => Some(StandardHeaderName::SecWebSocketAccept),
            "sec-websocket-extensions" => Some(StandardHeaderName::SecWebSocketExtensions),
            "sec-websocket-key" => Some(StandardHeaderName::SecWebSocketKey),
            "sec-websocket-protocol" => Some(StandardHeaderName::SecWebSocketProtocol),
            "sec-websocket-version" => Some(StandardHeaderName::SecWebSocketVersion),
            "server" => Some(StandardHeaderName::Server),
            "set-cookie" => Some(StandardHeaderName::SetCookie),
            "transfer-encoding" => Some(StandardHeaderName::TransferEncoding),
            "upgrade" => Some(StandardHeaderName::Upgrade),
            "user-agent" => Some(StandardHeaderName::UserAgent),
            _ => None,
        }
    }

    fn str_value(&self) -> &'static str {
        match self {
            StandardHeaderName::Accept => "accept",
            StandardHeaderName::AcceptCharset => "accept-charset",
            StandardHeaderName::AcceptEncoding => "accept-encoding",
            StandardHeaderName::AcceptLanguage => "accept-language",
            StandardHeaderName::Allow => "allow",
            StandardHeaderName::Connection => "connection",
            StandardHeaderName::ContentEncoding => "content-encoding",
            StandardHeaderName::ContentLength => "content-length",
            StandardHeaderName::ContentType => "content-type",
            StandardHeaderName::Cookie => "cookie",
            StandardHeaderName::Expect => "expect",
            StandardHeaderName::Host => "host",
            StandardHeaderName::Location => "location",
            StandardHeaderName::MaxForwards => "max-forwards",
            StandardHeaderName::Origin => "origin",
            StandardHeaderName::SecWebSocketAccept => "sec-websocket-accept",
            StandardHeaderName::SecWebSocketExtensions => "sec-websocket-extensions",
            StandardHeaderName::SecWebSocketKey => "sec-websocket-key",
            StandardHeaderName::SecWebSocketProtocol => "sec-websocket-protocol",
            StandardHeaderName::SecWebSocketVersion => "sec-websocket-version",
            StandardHeaderName::Server => "server",
            StandardHeaderName::SetCookie => "set-cookie",
            StandardHeaderName::TransferEncoding => "transfer-encoding",
            StandardHeaderName::Upgrade => "upgrade",
            StandardHeaderName::UserAgent => "user-agent",
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
