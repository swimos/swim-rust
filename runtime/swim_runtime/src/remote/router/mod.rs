// Copyright 2015-2021 Swim Inc.
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

#[cfg(test)]
mod tests;

mod error;
mod models;
mod replacement;

pub use error::*;
pub use models::*;
pub use replacement::*;
use std::convert::identity;
use std::future::Future;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use url::Url;

#[derive(Debug)]
pub enum Address {
    Local(RelativeUri),
    Remote(Url, RelativeUri),
}

impl Address {
    pub fn uri(&self) -> &RelativeUri {
        match self {
            Address::Local(uri) => uri,
            Address::Remote(_, uri) => uri,
        }
    }

    pub fn url(&self) -> Option<&Url> {
        match self {
            Address::Local(_) => None,
            Address::Remote(url, _) => Some(url),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Address::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Address::Remote(_, _))
    }
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

async fn callback<Func, Fut, E, T>(op: Func) -> Result<T, RoutingError>
where
    Func: FnOnce(oneshot::Sender<Result<T, RoutingError>>) -> Fut,
    Fut: Future<Output = Result<(), SendError<E>>>,
{
    let (callback_tx, callback_rx) = oneshot::channel();

    op(callback_tx)
        .await
        .map_err(|_| RoutingError::RouterDropped)?;
    callback_rx
        .await
        .map_err(|_| RoutingError::RouterDropped)
        .and_then(identity)
}
