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

use crate::Error;
use http::{HeaderMap, HeaderValue};
use httparse::Header;
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, ExtensionProvider, FrameHeader,
    ReunitableExtension, SplittableExtension,
};
use std::convert::Infallible;

#[derive(Debug, Default, Clone)]
pub struct NoExt;
impl Extension for NoExt {
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> &mut Self::Encoder {
        self
    }

    fn decoder(&mut self) -> &mut Self::Decoder {
        self
    }
}

impl ExtensionEncoder for NoExt {
    type Error = Infallible;

    fn encode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        Ok(())
    }
}

impl ExtensionDecoder for NoExt {
    type Error = Infallible;

    fn decode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        Ok(())
    }
}

pub struct NoExtProvider;
impl ExtensionProvider for NoExtProvider {
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

impl From<Infallible> for Error {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

impl SplittableExtension for NoExt {
    type SplitEncoder = NoExtEncoder;
    type SplitDecoder = NoExtDecoder;

    fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder) {
        (NoExtEncoder, NoExtDecoder)
    }
}

impl ReunitableExtension for NoExt {
    fn reunite(_encoder: Self::Encoder, _decoder: Self::Decoder) -> Self {
        NoExt
    }
}

#[derive(Debug)]
pub struct NoExtEncoder;
impl ExtensionEncoder for NoExtEncoder {
    type Error = Infallible;

    fn encode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        Ok(())
    }
}

#[derive(Debug)]
pub struct NoExtDecoder;
impl ExtensionDecoder for NoExtDecoder {
    type Error = Infallible;

    fn decode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        Ok(())
    }
}
