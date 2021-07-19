use futures::future::BoxFuture;
use futures::FutureExt;
use swim_client::router::ClientRequest;
use swim_common::request::Request;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::error::RouterError;
use swim_common::routing::remote::{RawRoute, RemoteRoutingRequest};
use swim_common::routing::{Duplex, Origin, PlaneRoutingRequest};
use swim_common::routing::{Route, Router, RouterFactory, RoutingAddr, TaggedSender};
use swim_common::warp::path::Path;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use url::Url;
use utilities::uri::RelativeUri;

#[derive(Debug, Clone)]
pub(crate) struct TopLevelRouterFactory {
    plane_sender: mpsc::Sender<PlaneRoutingRequest>,
    client_sender: mpsc::Sender<ClientRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelRouterFactory {
    pub(in crate) fn new(
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<ClientRequest<Path>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelRouterFactory {
            plane_sender,
            client_sender,
            remote_sender,
        }
    }
}

impl RouterFactory for TopLevelRouterFactory {
    type Router = TopLevelRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        TopLevelRouter::new(
            addr,
            self.plane_sender.clone(),
            self.client_sender.clone(),
            self.remote_sender.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct TopLevelRouter {
    addr: RoutingAddr,
    plane_sender: mpsc::Sender<PlaneRoutingRequest>,
    client_sender: mpsc::Sender<ClientRequest<Path>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        plane_sender: mpsc::Sender<PlaneRoutingRequest>,
        client_sender: mpsc::Sender<ClientRequest<Path>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelRouter {
            addr,
            plane_sender,
            client_sender,
            remote_sender,
        }
    }
}

impl Router for TopLevelRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let TopLevelRouter {
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
            } else {
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
            }
        }
        .boxed()
    }

    //Todo dm
    // fn register_duplex_connection(
    //     &mut self,
    //     addr: RoutingAddr,
    // ) -> BoxFuture<'_, Result<Duplex, ResolutionError>> {
    //     async move {
    //         if addr.is_local(){
    //
    //         }else {
    //
    //         }
    //     }.boxed()
    // }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let TopLevelRouter { plane_sender, .. } = self;

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
