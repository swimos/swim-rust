use crate::plane::PlaneRequest;

use futures::future::BoxFuture;
use futures::FutureExt;
use std::net::SocketAddr;
use swim_common::request::Request;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::error::RouterError;
use swim_common::routing::remote::{RawRoute, RoutingRequest};
use swim_common::routing::{Route, RoutingAddr, Router, RouterFactory, TaggedSender};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use url::Url;
use utilities::uri::RelativeUri;

#[derive(Debug, Clone)]
pub(crate) struct TopLevelRouterFactory {
    plane_sender: mpsc::Sender<PlaneRequest>,
    remote_sender: mpsc::Sender<RoutingRequest>,
}

impl TopLevelRouterFactory {
    pub(in crate) fn new(
        plane_sender: mpsc::Sender<PlaneRequest>,
        remote_sender: mpsc::Sender<RoutingRequest>,
    ) -> Self {
        TopLevelRouterFactory {
            plane_sender,
            remote_sender,
        }
    }
}

impl RouterFactory for TopLevelRouterFactory {
    type Router = TopLevelRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        TopLevelRouter::new(addr, self.plane_sender.clone(), self.remote_sender.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TopLevelRouter {
    addr: RoutingAddr,
    plane_sender: mpsc::Sender<PlaneRequest>,
    remote_sender: mpsc::Sender<RoutingRequest>,
}

impl TopLevelRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        plane_sender: mpsc::Sender<PlaneRequest>,
        remote_sender: mpsc::Sender<RoutingRequest>,
    ) -> Self {
        TopLevelRouter {
            addr,
            plane_sender,
            remote_sender,
        }
    }
}

impl Router for TopLevelRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
        _origin: Option<SocketAddr>,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let TopLevelRouter {
                plane_sender,
                remote_sender,
                addr: tag,
            } = self;

            if addr.is_remote() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if remote_sender
                    .send(RoutingRequest::Endpoint { addr, request })
                    .await
                    .is_err()
                {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(_)) => Err(ResolutionError::unresolvable(addr.to_string())),
                        Err(_) => Err(ResolutionError::router_dropped()),
                    }
                }
            } else {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if plane_sender
                    .send(PlaneRequest::Endpoint { id: addr, request })
                    .await
                    .is_err()
                {
                    Err(ResolutionError::router_dropped())
                } else {
                    match rx.await {
                        Ok(Ok(RawRoute { sender, on_drop })) => {
                            Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                        }
                        Ok(Err(_)) => Err(ResolutionError::unresolvable(addr.to_string())),
                        Err(_) => Err(ResolutionError::router_dropped()),
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
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let TopLevelRouter { plane_sender, .. } = self;

            let (tx, rx) = oneshot::channel();
            if plane_sender
                .send(PlaneRequest::Resolve {
                    host,
                    name: route.clone(),
                    request: Request::new(tx),
                })
                .await
                .is_err()
            {
                Err(RouterError::NoAgentAtRoute(route))
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
