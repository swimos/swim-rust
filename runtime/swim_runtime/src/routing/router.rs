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

use std::convert::identity;
use std::future::Future;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;

use crate::error::{ResolutionError, RoutingError};
use crate::remote::RawOutRoute;
use crate::routing::{
    Address, ClientEndpointRequest, PlaneRoutingRequest, RemoteRoutingRequest, Route, RoutingAddr,
    RoutingAddrKind, TaggedSender,
};
use swim_utilities::future::request::Request;
use tokio::sync::mpsc;
use tokio::sync::oneshot::error::RecvError;

#[derive(Clone, Debug)]
pub struct Router {
    client: mpsc::Sender<ClientEndpointRequest>,
    plane: Option<mpsc::Sender<PlaneRoutingRequest>>,
    remote: mpsc::Sender<RemoteRoutingRequest>,
}

impl Router {
    pub fn client(
        client: mpsc::Sender<ClientEndpointRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Router {
        Router {
            client,
            plane: None,
            remote,
        }
    }

    pub fn server(
        client: mpsc::Sender<ClientEndpointRequest>,
        plane: mpsc::Sender<PlaneRoutingRequest>,
        remote: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Router {
        Router {
            client,
            plane: Some(plane),
            remote,
        }
    }

    pub async fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> Result<RawOutRoute, ResolutionError> {
        let Router {
            client,
            plane,
            remote,
        } = self;

        match addr.discriminate() {
            RoutingAddrKind::Remote => {
                callback(|callback| {
                    remote.send(RemoteRoutingRequest::EndpointOut {
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
                None => Err(ResolutionError::Addr(addr)),
            },
            RoutingAddrKind::Client => {
                callback(|callback| {
                    client.send(ClientEndpointRequest::Get(addr, Request::new(callback)))
                })
                .await
            }
        }
    }

    pub async fn lookup<A>(&mut self, address: A) -> Result<RoutingAddr, RoutingError>
    where
        A: Into<Address>,
    {
        let address = address.into();

        match address.url() {
            Some(url) => callback(|callback| {
                let tx = &self.remote;
                tx.send(RemoteRoutingRequest::ResolveUrl {
                    host: url.clone(),
                    request: Request::new(callback),
                })
            })
            .await
            .map_err(Into::into),
            None => match &self.plane {
                Some(tx) => callback(|callback| {
                    tx.send(PlaneRoutingRequest::Resolve {
                        name: address.uri().clone(),
                        request: Request::new(callback),
                    })
                })
                .await
                .map_err(Into::into),
                None => Err(RoutingError::Resolution(ResolutionError::Agent(
                    address.uri().clone(),
                ))),
            },
        }
    }

    /// Creates a router that will tag senders with `tag`.
    pub fn tagged(&self, tag: RoutingAddr) -> TaggedRouter {
        TaggedRouter::new(Some(tag), self.clone())
    }

    /// Creates a router that will tag senders with the provided routing address during sender
    /// resolution.
    pub fn untagged(&self) -> TaggedRouter {
        TaggedRouter::new(None, self.clone())
    }
}

/// A wrapper around a raw router that attaches a tag (RoutingAddr) to routes that are resolved.
#[derive(Clone, Debug)]
pub struct TaggedRouter {
    tag: Option<RoutingAddr>,
    inner: Router,
}

impl TaggedRouter {
    fn new(tag: Option<RoutingAddr>, inner: Router) -> TaggedRouter {
        TaggedRouter { tag, inner }
    }

    pub async fn resolve_sender(&mut self, addr: RoutingAddr) -> Result<Route, ResolutionError> {
        let TaggedRouter { tag, inner } = self;
        let RawOutRoute { sender, on_drop } = inner.resolve_sender(addr).await?;

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
}

async fn callback<Func, Fut, E, T, P>(op: Func) -> Result<T, E>
where
    Func: FnOnce(oneshot::Sender<Result<T, E>>) -> Fut,
    Fut: Future<Output = Result<(), SendError<P>>>,
    E: From<RecvError> + From<SendError<P>>,
{
    let (callback_tx, callback_rx) = oneshot::channel();

    op(callback_tx).await.map_err::<E, _>(Into::into)?;
    callback_rx.await.map_err(Into::into).and_then(identity)
}
