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

use crate::error::{
    ConnectionDropped, ConnectionError, ResolutionError, RouterError, RoutingError,
};
use std::convert::TryFrom;

use crate::remote::RawOutRoute;
use bytes::Buf;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::fmt::{Display, Formatter};
use swim_utilities::future::item_sink::{ItemSink, SendError};
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, RequestEnvelope};
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

pub type CloseReceiver = promise::Receiver<mpsc::Sender<Result<(), RoutingError>>>;
pub type CloseSender = promise::Sender<mpsc::Sender<Result<(), RoutingError>>>;

#[cfg(test)]
mod tests;

/// A key into the server routing table specifying an endpoint to which [`Envelope`]s can be sent.
/// This is deliberately non-descriptive to allow it to be [`Copy`] and so very cheap to use as a
/// key.
type Location = Uuid;

/// An opaque routing address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingAddr(Location);

const REMOTE: u8 = 0;
const PLANE: u8 = 1;
const CLIENT: u8 = 2;

impl RoutingAddr {
    const fn new(tag: u8, id: u32) -> Self {
        let mut uuid_as_int = id as u128;
        uuid_as_int |= (tag as u128) << 120;
        RoutingAddr(Uuid::from_u128(uuid_as_int))
    }

    pub const fn remote(id: u32) -> Self {
        RoutingAddr::new(REMOTE, id)
    }

    pub const fn plane(id: u32) -> Self {
        RoutingAddr::new(PLANE, id)
    }

    pub const fn client(id: u32) -> Self {
        RoutingAddr::new(CLIENT, id)
    }

    pub fn is_plane(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == PLANE
    }

    pub fn is_remote(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == REMOTE
    }

    pub fn is_client(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == CLIENT
    }

    pub fn uuid(&self) -> &Uuid {
        &self.0
    }

    fn get_location(&self) -> u32 {
        let mut slice = &self.0.as_bytes()[12..];
        slice.get_u32()
    }
}

impl Display for RoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let location = self.get_location();
        match self.0.as_bytes()[0] {
            REMOTE => write!(f, "Remote({})", location),
            PLANE => write!(f, "Plane({})", location),
            _ => write!(f, "Client({})", location),
        }
    }
}

impl From<RoutingAddr> for Uuid {
    fn from(addr: RoutingAddr) -> Self {
        addr.0
    }
}

#[derive(Debug)]
pub struct InvalidRoutingAddr(Uuid);

impl Display for InvalidRoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is not a valid routing address.", &self.0)
    }
}

impl std::error::Error for InvalidRoutingAddr {}

impl TryFrom<Uuid> for RoutingAddr {
    type Error = InvalidRoutingAddr;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.as_bytes()[0] <= CLIENT {
            Ok(RoutingAddr(value))
        } else {
            Err(InvalidRoutingAddr(value))
        }
    }
}

/// An [`Envelope`] tagged with the key of the endpoint into routing table from which it originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedEnvelope(pub RoutingAddr, pub Envelope);

/// An [`RequestEnvelope`] tagged with the key of the endpoint into routing table from which it
/// originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedClientEnvelope(pub RoutingAddr, pub RequestEnvelope);

impl TaggedClientEnvelope {
    pub fn lane(&self) -> &str {
        self.1.path().lane.as_str()
    }
}

/// A single entry in the router consisting of a sender that will push envelopes to the endpoint
/// and a promise that will be satisfied when the endpoint closes.
#[derive(Clone, Debug)]
pub struct Route {
    sender: TaggedSender,
    on_drop: promise::Receiver<ConnectionDropped>,
}

#[derive(Debug)]
pub struct ClientRoute {
    pub tag: RoutingAddr,
    pub route: Route,
    pub receiver: mpsc::Receiver<TaggedEnvelope>,
    pub rx_on_dropped: promise::Receiver<ConnectionDropped>,
    pub handle_drop: ClientRouteMonitor,
}

#[derive(Debug)]
pub struct UnroutableClient {
    route: RawOutRoute,
    receiver: mpsc::Receiver<TaggedEnvelope>,
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    handle_drop: ClientRouteMonitor,
}

impl ClientRoute {
    pub fn new(
        tag: RoutingAddr,
        route: Route,
        receiver: mpsc::Receiver<TaggedEnvelope>,
        rx_on_dropped: promise::Receiver<ConnectionDropped>,
        handle_drop: ClientRouteMonitor,
    ) -> Self {
        ClientRoute {
            tag,
            route,
            receiver,
            rx_on_dropped,
            handle_drop,
        }
    }
}

#[derive(Debug)]
pub struct ClientRouteMonitor(Option<promise::Sender<ConnectionDropped>>);

impl ClientRouteMonitor {
    pub fn new(sender: promise::Sender<ConnectionDropped>) -> Self {
        ClientRouteMonitor(Some(sender))
    }
}

impl Drop for ClientRouteMonitor {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.provide(ConnectionDropped::Closed);
        }
    }
}

impl UnroutableClient {
    pub fn new(
        route: RawOutRoute,
        receiver: mpsc::Receiver<TaggedEnvelope>,
        rx_on_dropped: promise::Receiver<ConnectionDropped>,
        handle_drop: promise::Sender<ConnectionDropped>,
    ) -> Self {
        UnroutableClient {
            route,
            receiver,
            rx_on_dropped,
            handle_drop: ClientRouteMonitor(Some(handle_drop)),
        }
    }

    pub fn make_client(self, addr: RoutingAddr) -> ClientRoute {
        let UnroutableClient {
            route: RawOutRoute { sender, on_drop },
            receiver,
            rx_on_dropped,
            handle_drop,
        } = self;
        ClientRoute::new(
            addr,
            Route::new(TaggedSender::new(addr, sender), on_drop),
            receiver,
            rx_on_dropped,
            handle_drop,
        )
    }
}

impl Route {
    pub fn new(sender: TaggedSender, on_drop: promise::Receiver<ConnectionDropped>) -> Self {
        Route { sender, on_drop }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub async fn send_item(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
        self.sender.send_item(envelope).await
    }

    pub async fn terminated(self) -> ConnectionDropped {
        let Route { on_drop, ..} = self;
        on_drop
            .await
            .map(|reason| (*reason).clone())
            .unwrap_or(ConnectionDropped::Unknown)
    }
}

/// Trait for routers capable of resolving addresses and returning connections to them.
/// The connections can only be used to send [`Envelope`]s to the corresponding addresses.
pub trait Router: Send + Sync {
    /// Given a routing address, resolve the corresponding router entry
    /// consisting of a sender that will push envelopes to the endpoint.
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>>;

    /// Find and return the corresponding routing address of an endpoint for a given route.
    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Create router instances bound to particular routing addresses.
pub trait RouterFactory: Send + Sync {
    type Router: Router + 'static;

    /// Create a new router for a given routing address.
    fn create_for(&self, addr: RoutingAddr) -> Self::Router;

    /// Find and return the corresponding routing address of an endpoint for a given route.
    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Sender that attaches a [`RoutingAddr`] to received envelopes before sending them over a channel.
#[derive(Debug, Clone)]
pub struct TaggedSender {
    tag: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl TaggedSender {
    pub fn new(tag: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> Self {
        TaggedSender { tag, inner }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn send_item(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
        Ok(self
            .inner
            .send(TaggedEnvelope(self.tag, envelope))
            .await
            .map_err(|e| {
                let TaggedEnvelope(_addr, env) = e.0;
                SendError(env)
            })?)
    }
}

impl<'a> ItemSink<'a, Envelope> for TaggedSender {
    type Error = SendError<Envelope>;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        self.send_item(value).boxed()
    }
}

impl<'a> ItemSink<'a, Envelope> for Route {
    type Error = SendError<Envelope>;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        self.sender.send_item(value).boxed()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NoRoutes;

impl Router for NoRoutes {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move { Err(ResolutionError::Unresolvable(addr)) }.boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async move {
            if let Some(url) = host {
                Err(RouterError::ConnectionFailure(ConnectionError::Resolution(
                    url.to_string(),
                )))
            } else {
                Err(RouterError::NoAgentAtRoute(route))
            }
        }
        .boxed()
    }
}

impl RouterFactory for NoRoutes {
    type Router = NoRoutes;

    fn create_for(&self, _addr: RoutingAddr) -> Self::Router {
        NoRoutes
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        Router::lookup(self, host, route)
    }
}
