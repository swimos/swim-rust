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
    callback, Address, ReplacementDownlinkRoutingRequest, ReplacementPlaneRoutingRequest,
    ReplacementRemoteRoutingRequest, RouterError, RouterErrorKind,
};
use crate::routing::{BidirectionalRoute, Route, RoutingAddr, RoutingAddrKind, TaggedSender};
use swim_model::path::Addressable;
use swim_utilities::future::request::Request;
use tokio::sync::mpsc;
use url::Url;

/// A router which resolves raw routes that have no origin associated with them.
///
/// This hands out raw byte channels for sending messages to the destination and it is the callees
/// responsibility to ensure that the correct encoder is attached to the route.
#[derive(Clone, Debug)]
pub struct ReplacementRouter<Path> {
    client: mpsc::Sender<ReplacementDownlinkRoutingRequest<Path>>,
    plane: Option<mpsc::Sender<ReplacementPlaneRoutingRequest>>,
    remote: mpsc::Sender<ReplacementRemoteRoutingRequest>,
}

impl<Path> ReplacementRouter<Path> {
    pub fn client(
        client: mpsc::Sender<ReplacementDownlinkRoutingRequest<Path>>,
        remote: mpsc::Sender<ReplacementRemoteRoutingRequest>,
    ) -> ReplacementRouter<Path> {
        ReplacementRouter {
            client,
            plane: None,
            remote,
        }
    }

    pub fn server(
        client: mpsc::Sender<ReplacementDownlinkRoutingRequest<Path>>,
        plane: mpsc::Sender<ReplacementPlaneRoutingRequest>,
        remote: mpsc::Sender<ReplacementRemoteRoutingRequest>,
    ) -> ReplacementRouter<Path> {
        ReplacementRouter {
            client,
            plane: Some(plane),
            remote,
        }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<RawRoute, RouterError> {
        let ReplacementRouter {
            client,
            plane,
            remote,
        } = self;

        match addr.discriminate() {
            RoutingAddrKind::Remote => {
                callback(|callback| {
                    remote.send(ReplacementRemoteRoutingRequest::Endpoint {
                        addr,
                        request: Request::new(callback),
                    })
                })
                .await
            }
            RoutingAddrKind::Plane => match plane {
                Some(tx) => {
                    callback(|callback| {
                        tx.send(ReplacementPlaneRoutingRequest::Endpoint {
                            addr,
                            request: Request::new(callback),
                        })
                    })
                    .await
                }
                None => Err(RouterError::new(RouterErrorKind::Resolution)),
            },
            RoutingAddrKind::Client => {
                let _ = client;
                unimplemented!()
            }
        }
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RouterError>
    where
        A: Into<Address>,
    {
        let address = address.into();

        match address.url() {
            Some(url) => {
                callback(|callback| {
                    let tx = &self.remote;
                    tx.send(ReplacementRemoteRoutingRequest::ResolveUrl {
                        host: url.clone(),
                        request: Request::new(callback),
                    })
                })
                .await
            }
            None => Err(RouterError::new(RouterErrorKind::Resolution)),
        }
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRegistrator, RouterError> {
        let tx = &self.remote;
        callback(|callback| {
            tx.send(ReplacementRemoteRoutingRequest::Bidirectional {
                host,
                request: Request::new(callback),
            })
        })
        .await
    }

    /// Creates a router that will tag senders with `tag`.
    pub fn create_for(&self, tag: RoutingAddr) -> TaggedReplacementRouter<Path>
    where
        Path: Addressable,
    {
        TaggedReplacementRouter::new(tag, self.clone())
    }
}

/// A wrapper around a raw router that attaches a tag (RoutingAddr) to routes that are resolved.
#[derive(Debug)]
pub struct TaggedReplacementRouter<Path> {
    tag: RoutingAddr,
    inner: ReplacementRouter<Path>,
}

impl<Path> TaggedReplacementRouter<Path> {
    fn new(tag: RoutingAddr, inner: ReplacementRouter<Path>) -> TaggedReplacementRouter<Path> {
        TaggedReplacementRouter { tag, inner }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<Route, RouterError> {
        let TaggedReplacementRouter { tag, inner } = self;
        let RawRoute { sender, on_drop } = inner.resolve_sender(addr).await?;
        Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RouterError>
    where
        A: Into<Address>,
    {
        self.inner.lookup(address).await
    }

    pub async fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> Result<BidirectionalRoute, RouterError> {
        let handle = self.inner.resolve_bidirectional(host).await?;
        handle.register().await.map_err(Into::into)
    }
}
