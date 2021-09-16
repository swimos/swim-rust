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

use http::{HeaderMap, HeaderValue};
use httparse::Header;
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, ExtensionProvider, SplittableExtension,
};
use std::convert::Infallible;

#[derive(Debug, Default, Clone)]
pub struct NoExt;
impl Extension for NoExt {
    fn encode(&mut self) {}

    fn decode(&mut self) {}
}

pub struct NoExtProxy;
impl ExtensionProvider for NoExtProxy {
    type Extension = NoExt;
    type Error = Infallible;

    fn apply_headers(&self, _headers: &mut HeaderMap) {}

    fn negotiate_client(&self, _headers: &[Header]) -> Result<Self::Extension, Self::Error> {
        Ok(NoExt)
    }

    fn negotiate_server(
        &self,
        _headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error> {
        Ok((NoExt, None))
    }
}

impl SplittableExtension for NoExt {
    type Encoder = NoExtEncoder;
    type Decoder = NoExtDecoder;

    fn split(self) -> (Self::Encoder, Self::Decoder) {
        (NoExtEncoder, NoExtDecoder)
    }

    fn reunite(_encoder: Self::Encoder, _decoder: Self::Decoder) -> Self {
        NoExt
    }
}

#[derive(Debug)]
pub struct NoExtEncoder;
impl ExtensionEncoder for NoExtEncoder {
    type United = NoExt;
    fn encode(&mut self) {}
}

#[derive(Debug)]
pub struct NoExtDecoder;
impl ExtensionDecoder for NoExtDecoder {
    fn decode(&mut self) {}
}
