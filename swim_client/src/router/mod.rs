// Copyright 2015-2021 SWIM.AI inc.
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

use crate::connections::ConnectionChannel;
use crate::connections::ConnectionRegistrator;
use crate::connections::ConnectionType;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::trace_span;
use tracing::{span, Level};
use tracing_futures::Instrument;
use url::Url;

use swim_async_runtime::task::*;
use swim_model::path::{AbsolutePath, Addressable, RelativePath};
use swim_utilities::future::request::request_future::RequestError;
use swim_warp::envelope::{Envelope, IncomingLinkMessage};

use crate::connections::{ConnectionPool, ConnectionSender};
use swim_runtime::error::{ConnectionError, ResolutionError, RouterError, RoutingError, Unresolvable};
use swim_runtime::remote::{BadUrl, RawRoute, RemoteRoutingRequest};
use swim_runtime::remote::table::SchemeHostPort;
use swim_runtime::routing::{BidirectionalRoute, BidirectionalRouter, Route, Router, RouterFactory, RoutingAddr, TaggedSender};
use swim_utilities::errors::Recoverable;
use swim_utilities::future::request::Request;
use swim_utilities::trigger::promise;

#[cfg(test)]
pub(crate) mod tests;

#[derive(Debug, Clone)]
pub(crate) struct TopLevelClientRouterFactory {
    client_sender: mpsc::Sender<DownlinkRoutingRequest<AbsolutePath>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelClientRouterFactory {
    pub(in crate) fn new(
        client_sender: mpsc::Sender<DownlinkRoutingRequest<AbsolutePath>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelClientRouterFactory {
            client_sender,
            remote_sender,
        }
    }
}

impl RouterFactory for TopLevelClientRouterFactory {
    type Router = TopLevelClientRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        TopLevelClientRouter::new(addr, self.client_sender.clone(), self.remote_sender.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TopLevelClientRouter {
    addr: RoutingAddr,
    client_sender: mpsc::Sender<DownlinkRoutingRequest<AbsolutePath>>,
    remote_sender: mpsc::Sender<RemoteRoutingRequest>,
}

impl TopLevelClientRouter {
    pub(crate) fn new(
        addr: RoutingAddr,
        client_sender: mpsc::Sender<DownlinkRoutingRequest<AbsolutePath>>,
        remote_sender: mpsc::Sender<RemoteRoutingRequest>,
    ) -> Self {
        TopLevelClientRouter {
            addr,
            client_sender,
            remote_sender,
        }
    }
}

impl Router for TopLevelClientRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let TopLevelClientRouter {
                remote_sender,
                addr: tag,
                ..
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
                Err(ResolutionError::unresolvable(addr.to_string()))
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
            let TopLevelClientRouter { remote_sender, .. } = self;

            match host {
                Some(host) => {
                    let (tx, rx) = oneshot::channel();
                    if remote_sender
                        .send(RemoteRoutingRequest::ResolveUrl {
                            host,
                            request: Request::new(tx),
                        })
                        .await
                        .is_err()
                    {
                        Err(RouterError::RouterDropped)
                    } else {
                        match rx.await {
                            Ok(Ok(addr)) => Ok(addr),
                            Ok(Err(err)) => Err(RouterError::ConnectionFailure(err)),
                            Err(_) => Err(RouterError::RouterDropped),
                        }
                    }
                }
                None => Err(RouterError::ConnectionFailure(ConnectionError::Resolution(
                    ResolutionError::unresolvable(route.to_string()),
                ))),
            }
        }
        .boxed()
    }
}

impl BidirectionalRouter for TopLevelClientRouter {
    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        async move {
            let TopLevelClientRouter { remote_sender, .. } = self;

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
                    Ok(Ok(registrator)) => registrator.register().await,
                    Ok(Err(_)) => Err(ResolutionError::unresolvable(host.to_string())),
                    Err(_) => Err(ResolutionError::router_dropped()),
                }
            }
        }
        .boxed()
    }
}

pub(crate) type Node = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RoutingPath {
    Remote(SchemeHostPort),
    Local(Node),
}

pub struct AddressableWrapper<T: Addressable>(pub T);

impl<T: Addressable> TryFrom<AddressableWrapper<T>> for RoutingPath {
    type Error = BadUrl;

    fn try_from(path: AddressableWrapper<T>) -> Result<Self, Self::Error> {
        match path.0.host() {
            Some(host) => Ok(RoutingPath::Remote(SchemeHostPort::try_from(host)?)),
            None => Ok(RoutingPath::Local(path.0.node().to_string())),
        }
    }
}

pub(crate) struct RoutingTable<Path: Addressable> {
    addresses: HashMap<RoutingPath, RoutingAddr>,
    endpoints: HashMap<RoutingAddr, ConnectionRegistrator<Path>>,
}

impl<Path: Addressable> RoutingTable<Path> {
    pub(crate) fn new() -> Self {
        RoutingTable {
            addresses: HashMap::new(),
            endpoints: HashMap::new(),
        }
    }

    pub(crate) fn try_resolve_addr(
        &self,
        routing_path: &RoutingPath,
    ) -> Option<(RoutingAddr, ConnectionRegistrator<Path>)> {
        let routing_addr = self.addresses.get(routing_path).copied()?;
        let endpoint = self.try_resolve_endpoint(&routing_addr)?;
        Some((routing_addr, endpoint))
    }

    pub(crate) fn try_resolve_endpoint(
        &self,
        routing_addr: &RoutingAddr,
    ) -> Option<ConnectionRegistrator<Path>> {
        self.endpoints.get(routing_addr).cloned()
    }

    pub(crate) fn add_registrator(
        &mut self,
        routing_path: RoutingPath,
        routing_addr: RoutingAddr,
        connection_registrator: ConnectionRegistrator<Path>,
    ) {
        self.addresses.insert(routing_path, routing_addr);
        self.endpoints.insert(routing_addr, connection_registrator);
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouterFactory<Path: Addressable, DelegateFac: RouterFactory> {
    client_request_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    delegate_fac: DelegateFac,
}

impl<Path: Addressable, DelegateFac: RouterFactory> ClientRouterFactory<Path, DelegateFac> {
    pub fn new(
        request_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
        delegate_fac: DelegateFac,
    ) -> Self {
        ClientRouterFactory {
            client_request_sender: request_sender,
            delegate_fac,
        }
    }
}

impl<Path: Addressable, DelegateFac: RouterFactory> RouterFactory
    for ClientRouterFactory<Path, DelegateFac>
where
    <DelegateFac as RouterFactory>::Router: BidirectionalRouter,
{
    type Router = ClientRouter<Path, DelegateFac::Router>;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        ClientRouter {
            tag: addr,
            request_sender: self.client_request_sender.clone(),
            delegate_router: self.delegate_fac.create_for(addr),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouter<Path: Addressable, DelegateRouter: BidirectionalRouter> {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<DownlinkRoutingRequest<Path>>,
    delegate_router: DelegateRouter,
}

impl<Path: Addressable, DelegateRouter: BidirectionalRouter> Router
    for ClientRouter<Path, DelegateRouter>
{
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let ClientRouter {
                delegate_router, ..
            } = self;

            delegate_router.resolve_sender(addr).await
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let ClientRouter {
                delegate_router, ..
            } = self;

            delegate_router.lookup(host, route).await
        }
        .boxed()
    }
}

impl<Path: Addressable, DelegateRouter: BidirectionalRouter> BidirectionalRouter
    for ClientRouter<Path, DelegateRouter>
{
    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<'_, Result<BidirectionalRoute, ResolutionError>> {
        async move {
            let ClientRouter {
                delegate_router, ..
            } = self;

            delegate_router.resolve_bidirectional(host).await
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum DownlinkRoutingRequest<Path: Addressable> {
    /// Obtain a connection.
    Connect {
        target: Path,
        request: Request<Result<ConnectionChannel, ConnectionError>>,
        conn_type: ConnectionType,
    },
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: Request<Result<RawRoute, Unresolvable>>,
    },
}

/// The Router events are emitted by the connection streams of the router and indicate
/// messages or errors from the remote host.
#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    // Incoming message from a remote host.
    Message(IncomingLinkMessage),
    // There was an error in the connection. If a retry strategy exists this will trigger it.
    ConnectionClosed,
    /// The remote host is unreachable. This will not trigger the retry system.
    Unreachable(String),
    // The router is stopping.
    Stopping,
}
