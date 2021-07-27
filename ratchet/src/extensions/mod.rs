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

use crate::errors::BoxError;
use crate::extensions::ext::NoExt;
use crate::Request;
use std::fmt::Debug;

pub mod deflate;
pub mod ext;

// todo
pub struct ExtHandshakeErr(pub(crate) BoxError);

pub trait Extension: Debug + Clone {
    fn encode(&mut self);

    fn decode(&mut self);
}

pub trait ExtensionHandshake {
    type Extension: Extension;

    fn apply_headers(&self, request: &mut Request);

    fn negotiate(
        &self,
        response: &httparse::Response,
    ) -> Result<Option<Self::Extension>, ExtHandshakeErr>;
}

#[derive(Clone, Debug)]
pub enum NegotiatedExtension<E> {
    None(NoExt),
    Negotiated(E),
}

impl<E> Default for NegotiatedExtension<E> {
    fn default() -> Self {
        NegotiatedExtension::None(NoExt::default())
    }
}

impl<E> Extension for NegotiatedExtension<E>
where
    E: Extension,
{
    fn encode(&mut self) {
        match self {
            NegotiatedExtension::None(ext) => ext.encode(),
            NegotiatedExtension::Negotiated(ext) => ext.encode(),
        }
    }

    fn decode(&mut self) {
        match self {
            NegotiatedExtension::None(ext) => ext.decode(),
            NegotiatedExtension::Negotiated(ext) => ext.decode(),
        }
    }
}
