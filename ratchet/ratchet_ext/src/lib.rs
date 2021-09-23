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

pub use http::{HeaderMap, HeaderValue};
pub use httparse::Header;
use std::error::Error;
use std::fmt::Debug;

pub trait ExtensionProvider {
    type Extension: Extension;
    type Error: Error + Sync + Send + 'static;

    fn apply_headers(&self, headers: &mut HeaderMap);

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error>;

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<Option<(Self::Extension, Option<HeaderValue>)>, Self::Error>;
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum OpCode {
    Continuation,
    Text,
    Binary,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FrameHeader {
    pub fin: bool,
    pub rsv1: bool,
    pub rsv2: bool,
    pub rsv3: bool,
    pub opcode: OpCode,
}

pub trait Extension: ExtensionEncoder + ExtensionDecoder + Debug {}
impl<E> Extension for E where E: ExtensionEncoder + ExtensionDecoder + Debug {}

pub trait SplittableExtension: Extension {
    type SplitEncoder: ExtensionEncoder;
    type SplitDecoder: ExtensionDecoder;

    fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder);
}

pub trait ReunitableExtension: SplittableExtension {
    fn reunite(encoder: Self::SplitEncoder, decoder: Self::SplitDecoder) -> Self;
}

pub trait ExtensionEncoder {
    type Error: Error + Send + Sync + 'static;

    fn encode<A>(&mut self, payload: A, header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>;
}

pub trait ExtensionDecoder {
    type Error: Error + Send + Sync + 'static;

    fn decode<A>(&mut self, payload: A, header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>;
}

impl<'r, E> ExtensionProvider for &'r mut E
where
    E: ExtensionProvider,
{
    type Extension = E::Extension;
    type Error = E::Error;

    fn apply_headers(&self, headers: &mut HeaderMap) {
        E::apply_headers(self, headers)
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        E::negotiate_client(self, headers)
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<Option<(Self::Extension, Option<HeaderValue>)>, Self::Error> {
        E::negotiate_server(self, headers)
    }
}

impl<'r, E> ExtensionProvider for &'r E
where
    E: ExtensionProvider,
{
    type Extension = E::Extension;
    type Error = E::Error;

    fn apply_headers(&self, headers: &mut HeaderMap) {
        E::apply_headers(self, headers)
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        E::negotiate_client(self, headers)
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<Option<(Self::Extension, Option<HeaderValue>)>, Self::Error> {
        E::negotiate_server(self, headers)
    }
}
