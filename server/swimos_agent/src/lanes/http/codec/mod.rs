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

use bytes::BytesMut;
use frunk::{prelude::HList, HCons, HList, HNil};
use mime::Mime;
use std::fmt::Debug;
use swimos_form::Form;
use swimos_recon::{parser::parse_recognize, write_recon};
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
    /// Indicates whether the codec can serialize to an deserialize from the provided content type.
    fn supports(&self, content_type: &Mime) -> bool;

    /// Selects a content type based on the types that the client can accept.
    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime>;
}

/// Codec for encoding and decoding the payloads of HTTP requests/responses.
pub trait HttpLaneCodec<T>: HttpLaneCodecSupport {
    /// Attempt to encode the payload for an HTTP response/request.
    ///
    /// # Arguments
    /// * `content_type` - The content type to use.
    /// * `value` - The payload to encode.
    /// * `buffer` - Buffer into which to encode the payload.
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError>;

    /// Attempt to decode a payload from an HTTP request/response.
    ///
    /// # Arguments
    /// * `content_type` - The content type inferred from the headers.
    /// * `buffer` - The raw payload bytes.
    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError>;
}

/// Codec for the Recon format.
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
        parse_recognize(body, true).map_err(|e| CodecError::EncodingError(Box::new(e)))
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

/// The default codec used by HTTP lanes. The formats supported depend on what features are enabled
/// for the crate. Recon is always supported.
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
