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

use bytes::{BytesMut, BufMut, Buf};
use frunk::{prelude::HList, HCons, HNil, hlist, HList};
use mime::Mime;
use std::fmt::Debug;
use swim_api::protocol::write_recon;
use swim_form::Form;
use swim_recon::parser::parse_into;
use thiserror::Error;

use super::content_type::{recon, RECON_SUBTYPE};

#[cfg(feature = "json")]
mod json;

#[cfg(feature = "json")]
pub use json::Json;

#[cfg(test)]
mod tests;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("The codec does not support the content type: {0}")]
    UnsupportedContentType(Mime),
    #[error("Encoding or decoding of a payload failed: {0}")]
    EncodingError(Box<dyn std::error::Error + Send>),
}

pub trait HttpLaneCodecSupport: Default + Debug + Clone {
    fn supports(&self, content_type: &Mime) -> bool;

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime>;
}

pub trait HttpLaneCodec<T>: HttpLaneCodecSupport {
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError>;

    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError>;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct Recon;

impl HttpLaneCodecSupport for Recon {
    fn supports(&self, content_type: &Mime) -> bool {
        content_type == recon()
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        if accepts.is_empty() || accepts.iter().any(accepts_recon) {
            Some(recon())
        } else {
            None
        }
    }
}

fn accepts_recon(mime: &Mime) -> bool {
    (mime.type_() == mime::APPLICATION || mime.type_() == mime::STAR)
        && (mime.subtype() == RECON_SUBTYPE || mime.subtype() == mime::STAR)
}

impl<T: Form> HttpLaneCodec<T> for Recon {
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if !self.supports(content_type) {
            return Err(CodecError::UnsupportedContentType(content_type.clone()));
        }
        write_recon(buffer, value);
        Ok(())
    }

    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError> {
        if !self.supports(content_type) {
            return Err(CodecError::UnsupportedContentType(content_type.clone()));
        }
        let body = match std::str::from_utf8(buffer) {
            Ok(body) => body,
            Err(e) => return Err(CodecError::EncodingError(Box::new(e))),
        };
        parse_into(body, true).map_err(|e| CodecError::EncodingError(Box::new(e)))
    }
}

impl HttpLaneCodecSupport for HNil {
    fn supports(&self, _content_type: &Mime) -> bool {
        false
    }

    fn select_codec<'a>(&self, _accepts: &'a [Mime]) -> Option<&'a Mime> {
        None
    }
}

impl<Head, Tail> HttpLaneCodecSupport for HCons<Head, Tail>
where
    Head: HttpLaneCodecSupport,
    Tail: HList + HttpLaneCodecSupport,
{
    fn supports(&self, content_type: &Mime) -> bool {
        let HCons { head, tail } = self;
        head.supports(content_type) || tail.supports(content_type)
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        let HCons { head, tail } = self;
        head.select_codec(accepts)
            .or_else(|| tail.select_codec(accepts))
    }
}

impl<T> HttpLaneCodec<T> for HNil {
    fn encode(
        &self,
        content_type: &Mime,
        _value: &T,
        _buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        Err(CodecError::UnsupportedContentType(content_type.clone()))
    }

    fn decode(&self, content_type: &Mime, _buffer: &[u8]) -> Result<T, CodecError> {
        Err(CodecError::UnsupportedContentType(content_type.clone()))
    }
}

impl<T, Head, Tail> HttpLaneCodec<T> for HCons<Head, Tail>
where
    Head: HttpLaneCodec<T>,
    Tail: HList + HttpLaneCodec<T>,
{
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        let HCons { head, tail } = self;
        if head.supports(content_type) {
            head.encode(content_type, value, buffer)
        } else {
            tail.encode(content_type, value, buffer)
        }
    }

    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError> {
        let HCons { head, tail } = self;
        if head.supports(content_type) {
            head.decode(content_type, buffer)
        } else {
            tail.decode(content_type, buffer)
        }
    }
}

#[cfg(not(feature = "json"))]
type DefaultCodecInner = Recon;

#[cfg(feature = "json")]
type DefaultCodecInner = HList!(Recon, Json);

#[derive(Default, Debug, Clone, Copy)]
pub struct DefaultCodec {
    inner: DefaultCodecInner,
}

impl HttpLaneCodecSupport for DefaultCodec {
    fn supports(&self, content_type: &Mime) -> bool {
        self.inner.supports(content_type)
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        self.inner.select_codec(accepts)
    }
}

impl<T> HttpLaneCodec<T> for DefaultCodec
where
    DefaultCodecInner: HttpLaneCodec<T>,
{
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        self.inner.encode(content_type, value, buffer)
    }

    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError> {
        self.inner.decode(content_type, buffer)
    }
}

fn test_ct1() -> mime::Mime {
    "example/a".parse().unwrap()
}

fn test_ct2() -> mime::Mime {
    "example/b".parse().unwrap()
}

#[derive(Clone, Debug)]
struct CodecA(mime::Mime);

impl Default for CodecA {
    fn default() -> Self {
        Self(test_ct1())
    }
}

#[derive(Clone, Debug)]
struct CodecB(mime::Mime);

impl Default for CodecB {
    fn default() -> Self {
        Self(test_ct2())
    }
}

impl HttpLaneCodecSupport for CodecA {
    fn supports(&self, content_type: &Mime) -> bool {
        content_type == &self.0
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        accepts.iter().find(|ct| **ct == self.0)
    }
}

#[derive(Debug, Error)]
#[error("Bad data.")]
struct BadData;

impl HttpLaneCodec<i32> for CodecA {
    fn encode(
        &self,
        content_type: &Mime,
        value: &i32,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if self.supports(content_type) {
            buffer.reserve(5);
            buffer.put_u8(0);
            buffer.put_i32(*value);
            Ok(())
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }

    fn decode(&self, content_type: &Mime, mut buffer: &[u8]) -> Result<i32, CodecError> {
        if self.supports(content_type) {
            if buffer.len() == 5 {
                if buffer.get_u8() == 0 {
                    Ok(buffer.get_i32())
                } else {
                    Err(CodecError::EncodingError(Box::new(BadData)))
                }
            } else {
                Err(CodecError::EncodingError(Box::new(BadData)))
            }
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }
}

impl HttpLaneCodecSupport for CodecB {
    fn supports(&self, content_type: &Mime) -> bool {
        content_type == &self.0
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        accepts.iter().find(|ct| **ct == self.0)
    }
}

impl HttpLaneCodec<i32> for CodecB {
    fn encode(
        &self,
        content_type: &Mime,
        value: &i32,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if self.supports(content_type) {
            buffer.reserve(5);
            buffer.put_u8(1);
            buffer.put_i32(*value);
            Ok(())
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }

    fn decode(&self, content_type: &Mime, mut buffer: &[u8]) -> Result<i32, CodecError> {
        if self.supports(content_type) {
            if buffer.len() == 5 {
                if buffer.get_u8() == 1 {
                    Ok(buffer.get_i32())
                } else {
                    Err(CodecError::EncodingError(Box::new(BadData)))
                }
            } else {
                Err(CodecError::EncodingError(Box::new(BadData)))
            }
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }
}

#[test]
fn hnil_codec() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = HNil;
    assert!(!codec.supports(&ct1));
    assert!(!codec.supports(&ct2));

    let mut buffer = BytesMut::new();
    assert!(matches!(codec.encode(&ct1, &1, &mut buffer), Err(CodecError::UnsupportedContentType(_))));
    assert!(matches!(codec.encode(&ct2, &1, &mut buffer), Err(CodecError::UnsupportedContentType(_))));
}

#[test]
fn hcons_codec_support() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = hlist![CodecA::default(), CodecB::default()];

    assert!(codec.supports(&ct1));
    assert!(codec.supports(&ct2));
    assert!(!codec.supports(recon()));
}

#[test]
fn hcons_codec_encoding() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = hlist![CodecA::default(), CodecB::default()];

    let mut buffer = BytesMut::new();
    
    assert!(codec.encode(&ct1, &5, &mut buffer).is_ok());
    let bytes = buffer.as_ref();
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes[0], 0);
    let restored = codec.decode(&ct1, bytes).expect("Decode failed.");
    assert_eq!(restored, 5);
    
    buffer.clear();

    assert!(codec.encode(&ct2, &7, &mut buffer).is_ok());
    let bytes = buffer.as_ref();
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes[0], 1);
    let restored = codec.decode(&ct2, bytes).expect("Decode failed.");
    assert_eq!(restored, 7);
}