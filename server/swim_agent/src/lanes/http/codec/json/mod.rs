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

use bytes::{BufMut, BytesMut};
use mime::Mime;
use serde::{Deserialize, Serialize};

use super::{CodecError, HttpLaneCodec, HttpLaneCodecSupport};

#[cfg(test)]
mod tests;

#[derive(Default, Debug, Clone, Copy)]
pub struct Json;

impl HttpLaneCodecSupport for Json {
    fn supports(&self, content_type: &mime::Mime) -> bool {
        content_type.type_() == mime::APPLICATION_JSON.type_()
            && content_type.subtype() == mime::APPLICATION_JSON.subtype()
    }

    fn select_codec<'a>(&self, accepts: &'a [mime::Mime]) -> Option<&'a mime::Mime> {
        if accepts.is_empty() || accepts.iter().any(accepts_json) {
            Some(&mime::APPLICATION_JSON)
        } else {
            None
        }
    }
}

fn accepts_json(mime: &Mime) -> bool {
    (mime.type_() == mime::APPLICATION || mime.type_() == mime::STAR)
        && (mime.subtype() == mime::JSON || mime.subtype() == mime::STAR)
}

impl<T> HttpLaneCodec<T> for Json
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    fn encode(
        &self,
        content_type: &Mime,
        value: &T,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if !self.supports(content_type) {
            return Err(CodecError::UnsupportedContentType(content_type.clone()));
        }
        match serde_json::to_vec(value) {
            Ok(v) => {
                buffer.reserve(v.len());
                buffer.put(v.as_ref());
                Ok(())
            }
            Err(err) => Err(CodecError::EncodingError(Box::new(err))),
        }
    }

    fn decode(&self, content_type: &Mime, buffer: &[u8]) -> Result<T, CodecError> {
        if !self.supports(content_type) {
            return Err(CodecError::UnsupportedContentType(content_type.clone()));
        }
        serde_json::from_slice(buffer).map_err(|err| CodecError::EncodingError(Box::new(err)))
    }
}
