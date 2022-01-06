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

use crate::remote::table::BidirectionalRegistrator;
use crate::remote::RawRoute;
use crate::router2::{
    callback, Address, DownlinkRoutingRequest, NewRoutingError, PlaneRoutingRequest,
    RemoteRoutingRequest,
};
use crate::routing::{BidirectionalRoute, Route, RoutingAddr, RoutingAddrKind, TaggedSender};
use swim_model::path::Addressable;
use swim_utilities::future::request::Request;
use tokio::sync::mpsc;
use url::Url;

#[derive(Clone, Debug)]
pub struct ReplacementRouter<Path> {
    client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    plane: Option<mpsc::Sender<PlaneRoutingRequest>>,
    remote: mpsc::Sender<RemoteRoutingRequest>,
}

impl<Path> ReplacementRouter<Path> {
    pub fn client(
        client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> ReplacementRouter<Path> {
        ReplacementRouter {
            client,
            plane: None,
            remote,
        }
    }

    pub fn server(
        client: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        plane: mpsc::Sender<PlaneRoutingRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> ReplacementRouter<Path> {
        ReplacementRouter {
            client,
            plane: Some(plane),
            remote,
        }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<RawRoute, NewRoutingError> {
        let ReplacementRouter {
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
                None => Err(NewRoutingError::Resolution(None)),
            },
            RoutingAddrKind::Client => {
                let _ = client;
                unimplemented!()
            }
        }
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, NewRoutingError>
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
                            name: address.uri().clone(),
                            request: Request::new(callback),
                        })
                    })
                    .await
                }
                None => Err(NewRoutingError::Resolution(None)),
            },
        }
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRegistrator, NewRoutingError> {
        let tx = &self.remote;
        callback(|callback| {
            tx.send(RemoteRoutingRequest::Bidirectional {
                host,
                request: Request::new(callback),
            })
        })
        .await
    }

    /// Creates a router that will tag senders with `tag`.
    pub fn tagged(&self, tag: RoutingAddr) -> TaggedReplacementRouter<Path>
    where
        Path: Addressable,
    {
        TaggedReplacementRouter::new(tag, self.clone())
    }
}

/// A wrapper around a raw router that attaches a tag (RoutingAddr) to routes that are resolved.
#[derive(Clone, Debug)]
pub struct TaggedReplacementRouter<Path> {
    tag: RoutingAddr,
    inner: ReplacementRouter<Path>,
}

impl<Path> TaggedReplacementRouter<Path> {
    fn new(tag: RoutingAddr, inner: ReplacementRouter<Path>) -> TaggedReplacementRouter<Path> {
        TaggedReplacementRouter { tag, inner }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<Route, NewRoutingError> {
        let TaggedReplacementRouter { tag, inner } = self;
        let RawRoute { sender, on_drop } = inner.resolve_sender(addr).await?;
        Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, NewRoutingError>
    where
        A: Into<Address>,
    {
        self.inner.lookup(address).await
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRoute, NewRoutingError> {
        let handle = self.inner.resolve_bidirectional(host).await?;
        handle.register().await.map_err(Into::into)
    }
}
