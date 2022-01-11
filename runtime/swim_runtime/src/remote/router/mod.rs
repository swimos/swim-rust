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

// todo router
// #[cfg(test)]
// mod tests;

// mod error;
pub mod fixture;
mod models;

pub use models::*;

use std::convert::identity;
use std::future::Future;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;

use crate::error::RoutingError;
use crate::remote::table::BidirectionalRegistrator;
use crate::remote::RawRoute;
use crate::routing::{BidirectionalRoute, Route, RoutingAddr, RoutingAddrKind, TaggedSender};
use swim_model::path::Addressable;
use swim_utilities::future::request::Request;
use tokio::sync::mpsc;
use url::Url;

#[derive(Clone, Debug)]
pub struct Router<Path> {
    client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    plane: Option<mpsc::Sender<PlaneRoutingRequest>>,
    remote: mpsc::Sender<RemoteRoutingRequest>,
}

impl<Path> Router<Path> {
    pub fn client(
        client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Router<Path> {
        Router {
            client,
            plane: None,
            remote,
        }
    }

    pub fn server(
        client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        plane: mpsc::Sender<PlaneRoutingRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Router<Path> {
        Router {
            client,
            plane: Some(plane),
            remote,
        }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<RawRoute, RoutingError> {
        let Router {
            client,
            plane,
            remote,
        } = self;

        match addr.discriminate() {
            RoutingAddrKind::Remote => {
                callback(|callback| {
                    remote.send(RemoteRoutingRequest::Endpoint {
                        addr,
                        request: Request::new(callback),
                    })
                })
                .await
            }
            RoutingAddrKind::Plane => match plane {
                Some(tx) => {
                    callback(|callback| {
                        tx.send(PlaneRoutingRequest::Endpoint {
                            addr,
                            request: Request::new(callback),
                        })
                    })
                    .await
                }
                None => Err(RoutingError::Unresolvable(addr.to_string())),
            },
            RoutingAddrKind::Client => {
                // todo: implement once #457 has been merged
                let _ = client;
                unimplemented!()
            }
        }
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RoutingError>
    where
        A: Into<Address>,
    {
        let address = address.into();

        match address.url() {
            Some(url) => {
                callback(|callback| {
                    let tx = &self.remote;
                    tx.send(RemoteRoutingRequest::ResolveUrl {
                        host: url.clone(),
                        request: Request::new(callback),
                    })
                })
                .await
            }
            None => match &self.plane {
                Some(tx) => {
                    callback(|callback| {
                        tx.send(PlaneRoutingRequest::Resolve {
                            host: None,
                            route: address.uri().clone(),
                            request: Request::new(callback),
                        })
                    })
                    .await
                }
                None => Err(RoutingError::Unresolvable(address.into_string())),
            },
        }
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRegistrator, RoutingError> {
        let tx = &self.remote;
        callback(|callback| {
            tx.send(RemoteRoutingRequest::Bidirectional {
                host: host.clone(),
                request: Request::new(callback),
            })
        })
        .await
    }

    /// Creates a router that will tag senders with `tag`.
    pub fn tagged(&self, tag: RoutingAddr) -> TaggedRouter<Path>
    where
        Path: Addressable,
    {
        TaggedRouter::new(Some(tag), self.clone())
    }

    /// Creates a router that will tag senders with the provided routing address during sender
    /// resolution.
    pub fn untagged(&self) -> TaggedRouter<Path>
    where
        Path: Addressable,
    {
        TaggedRouter::new(None, self.clone())
    }
}

/// A wrapper around a raw router that attaches a tag (RoutingAddr) to routes that are resolved.
#[derive(Clone, Debug)]
pub struct TaggedRouter<Path> {
    tag: Option<RoutingAddr>,
    inner: Router<Path>,
}

impl<Path> TaggedRouter<Path> {
    fn new(tag: Option<RoutingAddr>, inner: Router<Path>) -> TaggedRouter<Path> {
        TaggedRouter { tag, inner }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<Route, RoutingError> {
        let TaggedRouter { tag, inner } = self;
        let RawRoute { sender, on_drop } = inner.resolve_sender(addr).await?;

        let tag = match tag {
            Some(tag) => *tag,
            None => addr,
        };

        Ok(Route::new(TaggedSender::new(tag, sender), on_drop))
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RoutingError>
    where
        A: Into<Address>,
    {
        self.inner.lookup(address).await
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRoute, RoutingError> {
        let handle = self.inner.resolve_bidirectional(host).await?;
        handle.register().await.map_err(Into::into)
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
