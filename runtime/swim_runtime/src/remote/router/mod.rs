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

use crate::error::{ConnectionDropped, ResolutionError, RouterError};
use crate::remote::{RawOutRoute, RemoteRoutingRequest};
use crate::routing::{
    BidirectionalRoute, BidirectionalRouter, Route, Router, RoutingAddr,
    TaggedSender,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use swim_utilities::trigger::promise::Receiver;

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

    fn register_interest(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Receiver<ConnectionDropped>, ResolutionError>> {
        async move {
            let RemoteRouter {
                delegate_router,
                request_tx, ..
            } = self;
            if addr.is_remote() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                let routing_req = RemoteRoutingRequest::EndpointIn { addr, request };
                if request_tx.send(routing_req).await.is_err() {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(on_drop)) => {
                            Ok(on_drop)
                        }
                        Ok(Err(err)) => Err(ResolutionError::unresolvable(err.to_string())),
                        Err(_) => Err(ResolutionError::router_dropped()),
                    }
                }
            } else {
                delegate_router.register_interest(addr).await
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
            } else {
                delegate_router.lookup(host, route).await
            }
        }
        .boxed()
    }
}

impl<DelegateRouter: Router> BidirectionalRouter for RemoteRouter<DelegateRouter> {
    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        let RemoteRouter { request_tx, .. } = self;
        async move {
            let (tx, rx) = oneshot::channel();
            let routing_req = RemoteRoutingRequest::Bidirectional {
                host: host.clone(),
                request: Request::new(tx),
            };
            if request_tx.send(routing_req).await.is_err() {
                Err(ResolutionError::router_dropped())
            } else {
                match rx.await {
                    Ok(Ok(registrator)) => registrator.register().await,
                    Ok(Err(_)) => Err(ResolutionError::unresolvable(host.to_string())),
                    Err(_) => Err(ResolutionError::router_dropped()),
                }
            }
        }
        .boxed()
    }
}
