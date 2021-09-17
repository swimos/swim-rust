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
use std::fmt::Debug;

pub mod deflate;
pub mod ext;

pub trait Extension: Debug {
    fn encode(&mut self);

    fn decode(&mut self);
}

pub trait ExtensionProvider {
    type Extension: Extension;
    type Error: Into<Error> + 'static;

    fn apply_headers(&self, headers: &mut HeaderMap);

    fn negotiate_client(&self, headers: &[Header]) -> Result<Self::Extension, Self::Error>;

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error>;
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

    fn negotiate_client(&self, headers: &[Header]) -> Result<Self::Extension, Self::Error> {
        E::negotiate_client(self, headers)
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error> {
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

    fn negotiate_client(&self, headers: &[Header]) -> Result<Self::Extension, Self::Error> {
        E::negotiate_client(self, headers)
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<(Self::Extension, Option<HeaderValue>), Self::Error> {
        E::negotiate_server(self, headers)
    }
}
