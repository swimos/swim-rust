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

use flate2::{Compress, Decompress};
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, ExtensionProvider, Header, HeaderMap,
    HeaderValue, SplittableExtension,
};
use thiserror::Error;

pub struct DeflateExtProvider {}

#[derive(Error, Debug)]
#[error("Err")]
pub struct DeflateError;

impl ExtensionProvider for DeflateExtProvider {
    type Extension = Deflate;
    type Error = DeflateError;

    fn apply_headers(&self, _headers: &mut HeaderMap) {
        todo!()
    }

    fn negotiate_client(&self, _headers: &[Header]) -> Result<Self::Extension, Self::Error> {
        unimplemented!()
    }

    fn negotiate_server(
        &self,
        _headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Deflate {
    encoder: DeflateEncoder,
    decoder: DeflateDecoder,
}

impl Extension for Deflate {
    fn encode(&mut self) {
        self.encoder.encode()
    }

    fn decode(&mut self) {
        self.decoder.decode()
    }
}

impl SplittableExtension for Deflate {
    type Encoder = DeflateEncoder;
    type Decoder = DeflateDecoder;

    fn split(self) -> (Self::Encoder, Self::Decoder) {
        let Deflate { encoder, decoder } = self;
        (encoder, decoder)
    }

    fn reunite(encoder: Self::Encoder, decoder: Self::Decoder) -> Self {
        Deflate { encoder, decoder }
    }
}

#[derive(Debug)]
pub struct DeflateEncoder {
    compress: Compress,
}

impl ExtensionEncoder for DeflateEncoder {
    type United = Deflate;

    fn encode(&mut self) {
        todo!()
    }
}

#[derive(Debug)]
pub struct DeflateDecoder {
    decompress: Decompress,
}

impl ExtensionDecoder for DeflateDecoder {
    fn decode(&mut self) {
        todo!()
    }
}
