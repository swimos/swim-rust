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

use crate::error::{ResolutionError, RouterError, Unresolvable};
use crate::remote::{RawOutRoute, RemoteRoutingRequest};
use crate::routing::{Route, Router, RouterFactory, RoutingAddr, TaggedSender};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::{mpsc, oneshot};
use url::Url;

#[cfg(test)]
mod tests;

/// Router implementation that will route to running [`ConnectionTask`]s for remote addresses and
/// will delegate to another router instance for local addresses.
#[derive(Debug, Clone)]
pub struct RemoteRouter<DelegateRouter> {
    tag: RoutingAddr,
    delegate_router: DelegateRouter,
    request_tx: mpsc::Sender<RemoteRoutingRequest>,
}

impl<DelegateRouter> RemoteRouter<DelegateRouter> {
    pub fn new(
        tag: RoutingAddr,
        delegate_router: DelegateRouter,
        request_tx: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        RemoteRouter {
            tag,
            delegate_router,
            request_tx,
        }
    }
}

impl<DelegateRouter: Router> Router for RemoteRouter<DelegateRouter> {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let RemoteRouter {
                tag,
                delegate_router,
                request_tx,
            } = self;
            if addr.is_remote() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                let routing_req = RemoteRoutingRequest::EndpointOut { addr, request };
                if request_tx.send(routing_req).await.is_err() {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawOutRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(Unresolvable(addr))) => Err(ResolutionError::unresolvable(addr)),
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
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let RemoteRouter {
                request_tx,
                delegate_router,
                ..
            } = self;
            if let Some(url) = host {
                lookup_inner(request_tx, url).await
            } else {
                delegate_router.lookup(host, route).await
            }
        }
        .boxed()
    }
}

pub struct RemoteRouterFactory<F> {
    delegate_router_factory: F,
    request_tx: mpsc::Sender<RemoteRoutingRequest>,
}

impl<F> RemoteRouterFactory<F> {
    pub fn new(delegate_router_factory: F, request_tx: mpsc::Sender<RemoteRoutingRequest>) -> Self {
        RemoteRouterFactory {
            delegate_router_factory,
            request_tx,
        }
    }
}

async fn lookup_inner(
    request_tx: &mpsc::Sender<RemoteRoutingRequest>,
    url: Url,
) -> Result<RoutingAddr, RouterError> {
    let (tx, rx) = oneshot::channel();
    let request = Request::new(tx);
    let routing_req = RemoteRoutingRequest::ResolveUrl { host: url, request };
    if request_tx.send(routing_req).await.is_err() {
        Err(RouterError::RouterDropped)
    } else {
        match rx.await {
            Ok(Ok(addr)) => Ok(addr),
            Ok(Err(err)) => Err(RouterError::ConnectionFailure(err)),
            Err(_) => Err(RouterError::RouterDropped),
        }
    }
}

impl<F: RouterFactory> RouterFactory for RemoteRouterFactory<F> {
    type Router = RemoteRouter<F::Router>;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        let RemoteRouterFactory {
            delegate_router_factory,
            request_tx,
        } = self;
        RemoteRouter::new(
            addr,
            delegate_router_factory.create_for(addr),
            request_tx.clone(),
        )
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async move {
            let RemoteRouterFactory {
                request_tx,
                delegate_router_factory,
                ..
            } = self;
            if let Some(url) = host {
                lookup_inner(request_tx, url).await
            } else {
                delegate_router_factory.lookup(host, route).await
            }
        }
        .boxed()
    }
}
