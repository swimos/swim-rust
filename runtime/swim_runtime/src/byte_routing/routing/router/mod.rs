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
mod task;

use crate::byte_routing::routing::router::task::RouterTask;
use crate::byte_routing::routing::RawRoute;
use crate::error::{ResolutionError, RouterError};
use crate::routing::RoutingAddr;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use url::Url;

type Callback<O, E> = oneshot::Sender<Result<O, E>>;

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

#[derive(Debug)]
struct Inner {
    tx: mpsc::Sender<RoutingRequest>,
    _task: JoinHandle<()>,
}

#[derive(Debug, Clone)]
pub struct Router {
    inner: Arc<Inner>,
}

impl Router {
    pub fn new(buffer_size: NonZeroUsize) -> Router {
        let (tx, rx) = mpsc::channel(buffer_size.get());
        let task = tokio::spawn(RouterTask::new(rx).run());

        Router {
            inner: Arc::new(Inner { tx, _task: task }),
        }
    }

    async fn exec<'f, Func, Fut, T>(&'f self, fun: Func) -> T
    where
        Func: FnOnce(&'f mpsc::Sender<RoutingRequest>, oneshot::Sender<T>) -> Fut,
        Fut: Future<Output = Result<(), SendError<RoutingRequest>>> + 'f,
    {
        let Router { inner, .. } = self;
        let (callback_tx, callback_rx) = oneshot::channel();

        fun(&inner.tx, callback_tx).await.expect("Router dropped");
        callback_rx.await.expect("No response")
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<RawRoute, ResolutionError> {
        self.exec(|tx, callback| tx.send(RoutingRequest::ResolveSender { addr, callback }))
            .await
    }

    pub async fn lookup<I>(&mut self, address: I) -> Result<RoutingAddr, RouterError>
    where
        I: Into<Address>,
    {
        self.exec(|tx, callback| {
            tx.send(RoutingRequest::Lookup {
                address: address.into(),
                callback,
            })
        })
        .await
    }
}

#[derive(Debug)]
pub enum RoutingRequest {
    ResolveSender {
        addr: RoutingAddr,
        callback: Callback<RawRoute, ResolutionError>,
    },
    Lookup {
        address: Address,
        callback: Callback<RoutingAddr, RouterError>,
    },
}
