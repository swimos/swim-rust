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

use futures::future::BoxFuture;
use futures::FutureExt;
use std::any::Any;
use std::collections::HashSet;

use std::sync::Arc;

use swim_model::path::Path;
use swim_runtime::error::{NoAgentAtRoute, RouterError, Unresolvable};
use swim_runtime::remote::RawRoute;
use swim_runtime::routing::{
    BidirectionalRoute, BidirectionalRouter, Route, Router, RouterFactory, RoutingAddr,
    TaggedSender,
};

use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;

use swim_runtime::router2::{
    DownlinkRoutingRequest, NewRoutingError, PlaneRoutingRequest, RemoteRoutingRequest,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use url::Url;

type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
type EndpointRequest = Request<Result<RawRoute, Unresolvable>>;
type RoutesRequest = Request<HashSet<RelativeUri>>;
type ResolutionRequest = Request<Result<RoutingAddr, RouterError>>;

// todo remove
// /// Requests that can be serviced by the plane event loop.
// #[derive(Debug)]
// pub enum PlaneRoutingRequest {
//     /// Get a handle to an agent (starting it where necessary).
//     Agent {
//         name: RelativeUri,
//         request: AgentRequest,
//     },
//     /// Get channel to route messages to a specified routing address.
//     Endpoint {
//         id: RoutingAddr,
//         request: EndpointRequest,
//     },
//     /// Resolve the routing address for an agent.
//     Resolve {
//         host: Option<Url>,
//         name: RelativeUri,
//         request: ResolutionRequest,
//     },
//     /// Get all of the active routes for the plane.
//     Routes(RoutesRequest),
// }

#[derive(Debug, Clone)]
pub(crate) struct TopLevelServerRouterFactory {
    plane_sender: mpsc::Sender<PlaneRoutingRequest>,
    client_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelServerRouterFactory {
    pub(in crate) fn new(
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelServerRouterFactory {
            plane_sender,
            client_sender,
            remote_sender,
        }
    }
}

impl RouterFactory for TopLevelServerRouterFactory {
    type Router = TopLevelServerRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        TopLevelServerRouter::new(
            addr,
            self.plane_sender.clone(),
            self.client_sender.clone(),
            self.remote_sender.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct TopLevelServerRouter {
    addr: RoutingAddr,
    plane_sender: mpsc::Sender<PlaneRoutingRequest>,
    client_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelServerRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelServerRouter {
            addr,
            plane_sender,
            client_sender,
            remote_sender,
        }
    }
}

impl Router for TopLevelServerRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, NewRoutingError>> {
        async move {
            let TopLevelServerRouter {
                plane_sender,
                remote_sender,
                client_sender,
                addr: tag,
            } = self;

            let (tx, rx) = oneshot::channel();
            let request = Request::new(tx);
            if addr.is_remote() {
                if remote_sender
                    .send(RemoteRoutingRequest::Endpoint { addr, request })
                    .await
                    .is_err()
                {
                    Err(NewRoutingError::RouterDropped)
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(err)) => Err(err),
                        Err(_) => Err(NewRoutingError::RouterDropped),
                    }
                }
            } else if addr.is_plane() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if plane_sender
                    .send(PlaneRoutingRequest::Endpoint { addr, request })
                    .await
                    .is_err()
                {
                    Err(NewRoutingError::RouterDropped)
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(err)) => Err(err),
                        Err(_) => Err(NewRoutingError::RouterDropped),
                    }
                }
            } else {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if client_sender
                    .send(DownlinkRoutingRequest::Endpoint { addr, request })
                    .await
                    .is_err()
                {
                    Err(NewRoutingError::RouterDropped)
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(err)) => Err(err),
                        Err(_) => Err(NewRoutingError::RouterDropped),
                    }
                }
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, NewRoutingError>> {
        async move {
            let TopLevelServerRouter { plane_sender, .. } = self;

            let (tx, rx) = oneshot::channel();
            if plane_sender
                .send(PlaneRoutingRequest::Resolve {
                    host,
                    name: route.clone(),
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(NewRoutingError::Resolution(Some(route.to_string())))
            } else {
                match rx.await {
                    Ok(Ok(addr)) => Ok(addr),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(NewRoutingError::RouterDropped),
                }
            }
        }
        .boxed()
    }
}

impl BidirectionalRouter for TopLevelServerRouter {
    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, NewRoutingError>> {
        async move {
            let TopLevelServerRouter { remote_sender, .. } = self;

            let (tx, rx) = oneshot::channel();
            if remote_sender
                .send(RemoteRoutingRequest::Bidirectional {
                    host: host.clone(),
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(NewRoutingError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(registrator)) => registrator.register().await.map_err(Into::into),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(NewRoutingError::RouterDropped),
                }
            }
        }
        .boxed()
    }
}
