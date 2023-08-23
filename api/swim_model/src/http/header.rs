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

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    str::FromStr,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::BytesStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl Header {

    pub fn new<N, V>(name: N, value: V) -> Self
    where
        N: Into<HeaderName>,
        V: Into<HeaderValue>, {
        Header { name: name.into(), value: value.into() }
    }

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

impl StandardHeaderName {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Accept),
            2 => Some(Self::AcceptCharset),
            3 => Some(Self::AcceptEncoding),
            4 => Some(Self::AcceptLanguage),
            5 => Some(Self::Allow),
            6 => Some(Self::Connection),
            7 => Some(Self::ContentEncoding),
            8 => Some(Self::ContentLength),
            9 => Some(Self::ContentType),
            10 => Some(Self::Cookie),
            11 => Some(Self::Expect),
            12 => Some(Self::Host),
            13 => Some(Self::Location),
            14 => Some(Self::MaxForwards),
            15 => Some(Self::Origin),
            16 => Some(Self::SecWebSocketAccept),
            17 => Some(Self::SecWebSocketExtensions),
            18 => Some(Self::SecWebSocketKey),
            19 => Some(Self::SecWebSocketProtocol),
            20 => Some(Self::SecWebSocketVersion),
            21 => Some(Self::Server),
            22 => Some(Self::SetCookie),
            23 => Some(Self::TransferEncoding),
            24 => Some(Self::Upgrade),
            25 => Some(Self::UserAgent),
            _ => None,
        }
    }
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
        self.str_value().partial_cmp(other.str_value())
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

#[derive(Debug, Error)]
pub enum HeaderNameDecodeError {
    #[error("{0} does not encode a valid standard header name.")]
    InvalidHeaderNameCode(u8),
    #[error("Header name contained invalid UTF8")]
    InvalidUtf8,
}

const TAG_LEN: usize = 1;
const NAME_LEN_SIZE: usize = 2;
const VALUE_LEN_SIZE: usize = 4;

impl Header {
    pub fn encoded_len(&self) -> usize {
        let Header { name, value } = self;
        let value_bytes = value.as_bytes();
        let value_len = VALUE_LEN_SIZE + value_bytes.len();
        let name_len = match &name.0 {
            Name::Standard(_) => TAG_LEN,
            Name::Other(s) => TAG_LEN + NAME_LEN_SIZE + s.as_str().len(),
        };
        name_len + value_len
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        let Header { name, value } = self;
        let value_bytes = value.as_bytes();
        dst.put_u32(value_bytes.len() as u32);
        match &name.0 {
            Name::Standard(s) => {
                dst.put_u8(*s as u8);
            }
            Name::Other(s) => {
                dst.put_u8(0);
                dst.put_u16(s.as_str().len() as u16);
                dst.put(s.as_str().as_bytes());
            }
        }
        dst.put(value_bytes);
    }

    pub fn decode(src: &mut BytesMut) -> Result<Option<Header>, HeaderNameDecodeError> {
        let mut data = src.as_ref();
        if data.remaining() < VALUE_LEN_SIZE {
            return Ok(None);
        }
        let value_len = data.get_u32() as usize;
        if data.remaining() < TAG_LEN {
            return Ok(None);
        }
        let code = data.get_u8();
        let name = if code == 0 {
            if data.remaining() < NAME_LEN_SIZE {
                return Ok(None);
            }
            let name_len = data.get_u16() as usize;
            if data.remaining() < name_len + value_len {
                return Ok(None);
            }
            src.advance(VALUE_LEN_SIZE + TAG_LEN + NAME_LEN_SIZE);
            if let Ok(bytes_str) = BytesStr::new(src.split_to(name_len).freeze()) {
                HeaderName(Name::Other(bytes_str))
            } else {
                return Err(HeaderNameDecodeError::InvalidUtf8);
            }
        } else {
            if data.remaining() < value_len {
                return Ok(None);
            }
            src.advance(VALUE_LEN_SIZE + TAG_LEN);
            if let Some(s) = StandardHeaderName::from_byte(code) {
                HeaderName(Name::Standard(s))
            } else {
                return Err(HeaderNameDecodeError::InvalidHeaderNameCode(code));
            }
        };
        let value_bytes = src.split_to(value_len).freeze();
        let value = HeaderValue::new(value_bytes);
        Ok(Some(Header { name, value }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::{Header, HeaderName, HeaderValue, StandardHeaderName};

    fn roundtrip_header(header: Header) {
        let mut buffer = BytesMut::new();

        header.encode(&mut buffer);
        assert_eq!(buffer.len(), header.encoded_len());

        let restored = Header::decode(&mut buffer)
            .expect("Decode failed.")
            .expect("Incomplete.");
        assert_eq!(restored, header);
        assert!(buffer.is_empty());
    }

    #[test]
    fn header_encoding() {
        roundtrip_header((StandardHeaderName::Cookie, "kjhsa8h8hasd8f").into());
        roundtrip_header(("custom_header_name", "I'm a header").into());
        let header = Header {
            name: HeaderName::from(StandardHeaderName::Allow),
            value: HeaderValue::new(Bytes::from([1, 2, 3].as_slice())),
        };
        roundtrip_header(header);
    }
}
