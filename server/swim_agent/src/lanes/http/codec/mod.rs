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
use frunk::{prelude::HList, HCons, HNil};
use mime::Mime;
use std::fmt::{Debug, Write};
use swim_form::Form;
use swim_recon::{parser::parse_into, printer::print_recon_compact};
use thiserror::Error;

use super::content_type::recon;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("The codec does not support the content type: {0}")]
    UnsupportedContentType(Mime),
    #[error("Encoding or decoding of a payload failed: {0}")]
    EncodingError(Box<dyn std::error::Error + Send>),
}

pub trait HttpLaneCodecSupport {
    fn supports(&self, content_type: &Mime) -> bool;
}

pub trait HttpLaneCodec<T>: HttpLaneCodecSupport + Default + Debug {
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
        write!(buffer, "{}", print_recon_compact(value))
            .expect("Encoding to Recon should be infallible.");
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
