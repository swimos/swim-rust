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

use crate::plane::PlaneRequest;
use crate::routing::error::{ResolutionError, RouterError};
use crate::routing::{Route, RoutingAddr, ServerRouter, ServerRouterFactory, TaggedSender};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_common::request::Request;
use swim_common::sink::item::either::EitherSink;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// Creates [`PlaneRouter`] instances by cloning a channel back to the plane.
#[derive(Debug)]
pub struct PlaneRouterFactory<DelegateFac: ServerRouterFactory> {
    request_sender: mpsc::Sender<PlaneRequest>,
    delegate_fac: DelegateFac,
}

impl<DelegateFac: ServerRouterFactory> PlaneRouterFactory<DelegateFac> {
    /// Create a factory from a channel back to the owning plane.
    pub(in crate) fn new(
        request_sender: mpsc::Sender<PlaneRequest>,
        delegate_fac: DelegateFac,
    ) -> Self {
        PlaneRouterFactory {
            request_sender,
            delegate_fac,
        }
    }
}

impl<DelegateFac: ServerRouterFactory> ServerRouterFactory for PlaneRouterFactory<DelegateFac> {
    type Router = PlaneRouter<DelegateFac::Router>;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        PlaneRouter::new(
            addr,
            self.delegate_fac.create_for(addr),
            self.request_sender.clone(),
        )
    }
}

/// An implementation of [`ServerRouter`] tied to a plane.
#[derive(Debug, Clone)]
pub struct PlaneRouter<Delegate> {
    tag: RoutingAddr,
    delegate_router: Delegate,
    request_sender: mpsc::Sender<PlaneRequest>,
}

impl<Delegate> PlaneRouter<Delegate> {
    pub(in crate) fn new(
        tag: RoutingAddr,
        delegate_router: Delegate,
        request_sender: mpsc::Sender<PlaneRequest>,
    ) -> Self {
        PlaneRouter {
            tag,
            delegate_router,
            request_sender,
        }
    }
}

impl<Delegate: ServerRouter> ServerRouter for PlaneRouter<Delegate> {
    type Sender = EitherSink<TaggedSender, Delegate::Sender>;

    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<Result<Route<Self::Sender>, ResolutionError>> {
        async move {
            let PlaneRouter {
                tag,
                delegate_router,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if addr.is_local() {
                if request_sender
                    .send(PlaneRequest::Endpoint {
                        id: addr,
                        request: Request::new(tx),
                    })
                    .await
                    .is_err()
                {
                    Err(ResolutionError::RouterDropped)
                } else {
                    match rx.await {
                        Ok(Ok(Route { sender, on_drop })) => Ok(Route::new(
                            EitherSink::left(TaggedSender::new(*tag, sender)),
                            on_drop,
                        )),
                        Ok(Err(err)) => Err(ResolutionError::Unresolvable(err)),
                        Err(_) => Err(ResolutionError::RouterDropped),
                    }
                }
            } else {
                delegate_router
                    .resolve_sender(addr)
                    .await
                    .map(|Route { sender, on_drop }| Route::new(EitherSink::right(sender), on_drop))
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
                .send(PlaneRequest::Resolve {
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
