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

use crate::routing::PlaneRoutingRequest;
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_runtime::error::{ResolutionError, RouterError};
use swim_runtime::remote::RawRoute;
use swim_runtime::routing::{Route, Router, RouterFactory, RoutingAddr, TaggedSender};
use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::{mpsc, oneshot};
use url::Url;

#[cfg(test)]
mod tests;

/// Creates [`PlaneRouter`] instances by cloning a channel back to the plane.
#[derive(Debug)]
pub struct PlaneRouterFactory<DelegateFac: RouterFactory> {
    request_sender: mpsc::Sender<PlaneRoutingRequest>,
    delegate_fac: DelegateFac,
}

impl<DelegateFac: RouterFactory> PlaneRouterFactory<DelegateFac> {
    /// Create a factory from a channel back to the owning plane.
    pub fn new(
        request_sender: mpsc::Sender<PlaneRoutingRequest>,
        delegate_fac: DelegateFac,
    ) -> Self {
        PlaneRouterFactory {
            request_sender,
            delegate_fac,
        }
    }
}

impl<DelegateFac: RouterFactory> RouterFactory for PlaneRouterFactory<DelegateFac> {
    type Router = PlaneRouter<DelegateFac::Router>;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        PlaneRouter::new(
            addr,
            self.delegate_fac.create_for(addr),
            self.request_sender.clone(),
        )
    }
}

/// An implementation of [`Router`] tied to a plane.
#[derive(Debug, Clone)]
pub struct PlaneRouter<Delegate> {
    tag: RoutingAddr,
    delegate_router: Delegate,
    request_sender: mpsc::Sender<PlaneRoutingRequest>,
}

impl<Delegate> PlaneRouter<Delegate> {
    pub fn new(
        tag: RoutingAddr,
        delegate_router: Delegate,
        request_sender: mpsc::Sender<PlaneRoutingRequest>,
    ) -> Self {
        PlaneRouter {
            tag,
            delegate_router,
            request_sender,
        }
    }
}

impl<Delegate: Router> Router for PlaneRouter<Delegate> {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move {
            let PlaneRouter {
                tag,
                delegate_router,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if addr.is_plane() {
                if request_sender
                    .send(PlaneRoutingRequest::Endpoint {
                        id: addr,
                        request: Request::new(tx),
                    })
                    .await
                    .is_err()
                {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(err)) => Err(ResolutionError::unresolvable(err.to_string())),
                        Err(_) => Err(ResolutionError::router_dropped()),
                    }
                }
            } else {
                delegate_router.resolve_sender(addr).await
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async move {
            let PlaneRouter { request_sender, .. } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(PlaneRoutingRequest::Resolve {
                    host,
                    name: route,
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(RouterError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(addr)) => Ok(addr),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(RouterError::RouterDropped),
                }
            }
        }
        .boxed()
    }
}
