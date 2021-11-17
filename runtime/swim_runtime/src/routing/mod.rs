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

use crate::error::{ConnectionDropped, ResolutionError, RouterError, RoutingError};

use futures::future::BoxFuture;
use futures::FutureExt;
use std::fmt::{Display, Formatter};
use swim_utilities::future::item_sink::{ItemSink, SendError};
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, RequestEnvelope};
use tokio::sync::mpsc;
use url::Url;

pub type CloseReceiver = promise::Receiver<mpsc::Sender<Result<(), RoutingError>>>;
pub type CloseSender = promise::Sender<mpsc::Sender<Result<(), RoutingError>>>;

/// A key into the server routing table specifying an endpoint to which [`Envelope`]s can be sent.
/// This is deliberately non-descriptive to allow it to be [`Copy`] and so very cheap to use as a
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Location {
    /// Indicates that envelopes will be routed to a remote host.
    Remote(u32),
    /// Indicates that envelopes will be routed to another agent on this host.
    Plane(u32),
    /// Indicates that envelopes will be routed to a downlink on the client.
    Client(u32),
}

/// An opaque routing address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingAddr(Location);

impl RoutingAddr {
    pub const fn remote(id: u32) -> Self {
        RoutingAddr(Location::Remote(id))
    }

    pub const fn plane(id: u32) -> Self {
        RoutingAddr(Location::Plane(id))
    }

    pub const fn client(id: u32) -> Self {
        RoutingAddr(Location::Client(id))
    }

    pub fn is_plane(&self) -> bool {
        matches!(self, RoutingAddr(Location::Plane(_)))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, RoutingAddr(Location::Remote(_)))
    }

    pub fn is_client(&self) -> bool {
        matches!(self, RoutingAddr(Location::Client(_)))
    }
}

impl Display for RoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingAddr(Location::Remote(id)) => write!(f, "Remote({:X})", id),
            RoutingAddr(Location::Plane(id)) => write!(f, "Plane({:X})", id),
            RoutingAddr(Location::Client(id)) => write!(f, "Client({:X})", id),
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
    pub sender: TaggedSender,
    pub on_drop: promise::Receiver<ConnectionDropped>,
}

impl Route {
    pub fn new(sender: TaggedSender, on_drop: promise::Receiver<ConnectionDropped>) -> Self {
        Route { sender, on_drop }
    }
}

#[derive(Debug)]
pub struct BidirectionalRoute {
    pub sender: TaggedSender,
    pub receiver: mpsc::Receiver<TaggedEnvelope>,
    pub on_drop: promise::Receiver<ConnectionDropped>,
}

impl BidirectionalRoute {
    pub fn new(
        sender: TaggedSender,
        receiver: mpsc::Receiver<TaggedEnvelope>,
        on_drop: promise::Receiver<ConnectionDropped>,
    ) -> Self {
        BidirectionalRoute {
            sender,
            receiver,
            on_drop,
        }
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

/// Trait for routers capable of resolving addresses and returning bidirectional connections to them.
/// The connections can be used to both send and receive [`Envelope`]s to and from the corresponding addresses.
pub trait BidirectionalRouter: Router {
    /// Resolve a bidirectional connection for a given host.
    fn resolve_bidirectional(
        &mut self,
        host: Url,
    ) -> BoxFuture<Result<BidirectionalRoute, ResolutionError>>;
}

/// Create router instances bound to particular routing addresses.
pub trait RouterFactory: Send + Sync {
    type Router: Router + 'static;

    /// Create a new router for a given routing address.
    fn create_for(&self, addr: RoutingAddr) -> Self::Router;
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
