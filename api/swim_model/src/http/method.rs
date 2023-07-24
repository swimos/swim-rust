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

use bytes::{Buf, BufMut, BytesMut};
use lazy_static::lazy_static;
use std::{collections::HashMap, fmt::Formatter};
use thiserror::Error;

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Method(MethodInner);

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash)]
enum MethodInner {
    #[default]
    Get,
    Head,
    Post,
    Put,
    Delete,
    Connect,
    Options,
    Trace,
}

impl Method {
    pub const GET: Method = Method(MethodInner::Get);
    pub const HEAD: Method = Method(MethodInner::Head);
    pub const POST: Method = Method(MethodInner::Post);
    pub const PUT: Method = Method(MethodInner::Put);
    pub const DELETE: Method = Method(MethodInner::Delete);
    pub const CONNECT: Method = Method(MethodInner::Connect);
    pub const OPTIONS: Method = Method(MethodInner::Options);
    pub const TRACE: Method = Method(MethodInner::Trace);
}

impl From<Method> for http::Method {
    fn from(value: Method) -> Self {
        match value.0 {
            MethodInner::Get => http::Method::GET,
            MethodInner::Head => http::Method::HEAD,
            MethodInner::Post => http::Method::POST,
            MethodInner::Put => http::Method::PUT,
            MethodInner::Delete => http::Method::DELETE,
            MethodInner::Connect => http::Method::CONNECT,
            MethodInner::Options => http::Method::OPTIONS,
            MethodInner::Trace => http::Method::TRACE,
        }
    }
}

lazy_static! {
    static ref METHODS: HashMap<http::Method, Method> = {
        let mut m = HashMap::new();
        m.insert(http::Method::GET, Method::GET);
        m.insert(http::Method::HEAD, Method::HEAD);
        m.insert(http::Method::POST, Method::POST);
        m.insert(http::Method::DELETE, Method::DELETE);
        m.insert(http::Method::CONNECT, Method::CONNECT);
        m.insert(http::Method::OPTIONS, Method::OPTIONS);
        m.insert(http::Method::TRACE, Method::TRACE);
        m
    };
}

impl TryFrom<http::Method> for Method {
    type Error = UnsupportedMethod;

    fn try_from(value: http::Method) -> Result<Self, Self::Error> {
        METHODS
            .get(&value)
            .copied()
            .ok_or_else(|| UnsupportedMethod(value.to_string()))
    }
}

#[derive(Debug, Error)]
#[error("HTTP method '{0}' is not supported.")]
pub struct UnsupportedMethod(String);

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            MethodInner::Get => write!(f, "GET"),
            MethodInner::Head => write!(f, "HEAD"),
            MethodInner::Post => write!(f, "POST"),
            MethodInner::Put => write!(f, "PUT"),
            MethodInner::Delete => write!(f, "DELETE"),
            MethodInner::Connect => write!(f, "CONNECT"),
            MethodInner::Options => write!(f, "OPTIONS"),
            MethodInner::Trace => write!(f, "TRACE"),
        }
    }
}

impl std::fmt::Debug for Method {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl Method {
    pub const ENCODED_LENGTH: usize = 1;
}

impl Method {
    pub fn encode(&self, dst: &mut BytesMut) {
        let code = match &self.0 {
            MethodInner::Get => 0u8,
            MethodInner::Head => 1u8,
            MethodInner::Post => 2u8,
            MethodInner::Put => 3u8,
            MethodInner::Delete => 4u8,
            MethodInner::Connect => 5u8,
            MethodInner::Options => 6u8,
            MethodInner::Trace => 7u8,
        };
        dst.put_u8(code);
    }

    pub fn decode(src: &mut impl Buf) -> Result<Option<Self>, MethodDecodeError> {
        if src.remaining() >= Self::ENCODED_LENGTH {
            match src.get_u8() {
                0 => Ok(Some(Method::GET)),
                1 => Ok(Some(Method::HEAD)),
                2 => Ok(Some(Method::POST)),
                3 => Ok(Some(Method::PUT)),
                4 => Ok(Some(Method::DELETE)),
                5 => Ok(Some(Method::CONNECT)),
                6 => Ok(Some(Method::OPTIONS)),
                7 => Ok(Some(Method::TRACE)),
                ow => Err(MethodDecodeError(ow)),
            }
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Error)]
#[error("{0} does not encode a valid method.")]
pub struct MethodDecodeError(pub u8);

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::Method;

    fn round_trip_method(method: Method) {
        let mut buffer = BytesMut::new();
        method.encode(&mut buffer);

        let restored = Method::decode(&mut buffer)
            .expect("Decoding failed.")
            .expect("Incomplete.");
        assert_eq!(restored, method);
        assert!(buffer.is_empty());
    }

    #[test]
    fn method_encoding() {
        round_trip_method(Method::GET);
        round_trip_method(Method::POST);
        round_trip_method(Method::PUT);
        round_trip_method(Method::DELETE);
        round_trip_method(Method::HEAD);
        round_trip_method(Method::OPTIONS);
        round_trip_method(Method::TRACE);
        round_trip_method(Method::CONNECT);
    }

    #[test]
    fn invalid_methods() {
        let mut buffer = BytesMut::new();
        assert!(matches!(Method::decode(&mut buffer), Ok(None)));

        buffer.put_u8(u8::MAX);
        assert!(matches!(Method::decode(&mut buffer), Err(_)));
    }
}
