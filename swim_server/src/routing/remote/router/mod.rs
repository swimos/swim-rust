// Copyright 2015-2020 SWIM.AI inc.
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

use crate::routing::error::RouterError;
use crate::routing::remote::{RawRoute, RoutingRequest};
use crate::routing::{Route, RoutingAddr, ServerRouter, TaggedSender};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_common::request::Request;
use swim_common::routing::ResolutionError;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Router implementation that will route to running [`ConnectionTask`]s for remote addresses and
/// will delegate to another router instance for local addresses.
#[derive(Debug)]
pub struct RemoteRouter<Delegate> {
    tag: RoutingAddr,
    delegate_router: Delegate,
    request_tx: mpsc::Sender<RoutingRequest>,
}

impl<Delegate> RemoteRouter<Delegate> {
    pub fn new(
        tag: RoutingAddr,
        delegate_router: Delegate,
        request_tx: mpsc::Sender<RoutingRequest>,
    ) -> Self {
        RemoteRouter {
            tag,
            delegate_router,
            request_tx,
        }
    }
}

impl<Delegate: ServerRouter> ServerRouter for RemoteRouter<Delegate> {
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
                let routing_req = RoutingRequest::Endpoint { addr, request };
                if request_tx.send(routing_req).await.is_err() {
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
                let routing_req = RoutingRequest::ResolveUrl { host: url, request };
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
