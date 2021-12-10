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

mod error;
mod models;
mod server;

pub use error::*;
pub use models::*;
pub use server::*;
use swim_utilities::routing::uri::RelativeUri;
use url::Url;

pub enum RemoteRequest {}

#[derive(Debug)]
pub enum Address {
    Local(RelativeUri),
    Remote(Url, RelativeUri),
}

impl From<(Option<Url>, RelativeUri)> for Address {
    fn from(p: (Option<Url>, RelativeUri)) -> Self {
        match p {
            (Some(url), uri) => Address::Remote(url, uri),
            (None, uri) => Address::Local(uri),
        }
    }
}

impl From<RelativeUri> for Address {
    fn from(uri: RelativeUri) -> Self {
        Address::Local(uri)
    }
}
