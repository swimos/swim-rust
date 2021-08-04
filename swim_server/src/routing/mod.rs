use futures::future::BoxFuture;
use futures::FutureExt;
use swim_client::router::ClientRoutingRequest;
use swim_common::request::Request;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::error::RouterError;
use swim_common::routing::remote::{RawRoute, RemoteRoutingRequest};
use swim_common::routing::{BidirectionalRoute, Origin, PlaneRoutingRequest};
use swim_common::routing::{Route, Router, RouterFactory, RoutingAddr, TaggedSender};
use swim_common::warp::path::Path;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use url::Url;
use utilities::uri::RelativeUri;

#[derive(Debug, Clone)]
pub(crate) struct TopLevelServerRouterFactory {
    plane_sender: mpsc::Sender<PlaneRoutingRequest>,
    client_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelServerRouterFactory {
    pub(in crate) fn new(
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
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
    client_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelServerRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<ClientRoutingRequest<Path>>,
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
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let TopLevelServerRouter {
                plane_sender,
                remote_sender,
                client_sender,
                addr: tag,
            } = self;

            if addr.is_remote() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if remote_sender
                    .send(RemoteRoutingRequest::Endpoint { addr, request })
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
            } else if addr.is_local() {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                if plane_sender
                    .send(PlaneRoutingRequest::Endpoint { id: addr, request })
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
                if client_sender
                    .send(ClientRoutingRequest::Endpoint { addr, request })
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

    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
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
                Err(ResolutionError::router_dropped())
            } else {
                match rx.await {
                    Ok(Ok(addr)) => Ok(addr),
                    Ok(Err(_)) => Err((ResolutionError::unresolvable(host.to_string()))),
                    Err(_) => Err(ResolutionError::router_dropped()),
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
