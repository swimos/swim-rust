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

use crate::request::Request;
use crate::routing::error::{
    ConnectionError, HttpError, NoAgentAtRoute, ResolutionError, RouterError, RoutingError,
    SendError, Unresolvable,
};
use crate::routing::remote::{RawRoute, SchemeSocketAddr};
use crate::routing::ws::WsMessage;
use crate::warp::envelope::{Envelope, OutgoingLinkMessage};
use futures::future::BoxFuture;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use url::Url;
use utilities::errors::Recoverable;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

pub mod error;
pub mod remote;
pub mod ws;

#[cfg(test)]
mod tests;

pub type CloseReceiver = promise::Receiver<mpsc::Sender<Result<(), RoutingError>>>;
pub type CloseSender = promise::Sender<mpsc::Sender<Result<(), RoutingError>>>;

trait RoutingRequest {}

/// A key into the server routing table specifying an endpoint to which [`Envelope`]s can be sent.
/// This is deliberately non-descriptive to allow it to be [`Copy`] and so very cheap to use as a
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Location {
    /// Indicates that envelopes will be routed to a remote host.
    RemoteEndpoint(u32),
    /// Indicates that envelopes will be routed to another agent on this host.
    Local(u32),
    /// Indicates that envelopes will be routed to the client.
    Client,
}

/// An opaque routing address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingAddr(Location);

impl RoutingAddr {
    pub const fn remote(id: u32) -> Self {
        RoutingAddr(Location::RemoteEndpoint(id))
    }

    pub const fn local(id: u32) -> Self {
        RoutingAddr(Location::Local(id))
    }

    pub const fn client() -> Self {
        RoutingAddr(Location::Client)
    }

    pub fn is_local(&self) -> bool {
        matches!(self, RoutingAddr(Location::Local(_)))
            || matches!(self, RoutingAddr(Location::Client))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, RoutingAddr(Location::RemoteEndpoint(_)))
    }
}

impl Display for RoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingAddr(Location::RemoteEndpoint(id)) => write!(f, "Remote({:X})", id),
            RoutingAddr(Location::Local(id)) => write!(f, "Local({:X})", id),
            RoutingAddr(Location::Client) => {
                write!(f, "Client")
            }
        }
    }
}

/// An [`Envelope`] tagged with the key of the endpoint into routing table from which it originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedEnvelope(pub RoutingAddr, pub Envelope);

impl From<TaggedEnvelope> for WsMessage {
    fn from(env: TaggedEnvelope) -> Self {
        let TaggedEnvelope(_, envelope) = env;
        envelope.into()
    }
}

/// An [`OutgoingLinkMessage`] tagged with the key of the endpoint into routing table from which it
/// originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedClientEnvelope(pub RoutingAddr, pub OutgoingLinkMessage);

impl TaggedClientEnvelope {
    pub fn lane(&self) -> &str {
        self.1.path.lane.as_str()
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

/// Interface for interacting with the server [`Envelope`] router.
pub trait Router: Send + Sync {
    /// Given a routing address, resolve the corresponding router entry
    /// consisting of a sender that will push envelopes to the endpoint.
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
        origin: Option<Origin>,
    ) -> BoxFuture<Result<Route, ResolutionError>>;

    /// Find and return the corresponding routing address of an endpoint for a given route.
    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
        origin: Option<Origin>,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Create router instances bound to particular routing addresses.
pub trait RouterFactory: Send + Sync {
    type Router: Router;

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

    pub async fn send_item(&mut self, envelope: Envelope) -> Result<(), SendError> {
        Ok(self
            .inner
            .send(TaggedEnvelope(self.tag, envelope))
            .await
            .map_err(|e| {
                let TaggedEnvelope(_addr, env) = e.0;
                SendError::new(RoutingError::CloseError, env)
            })?)
    }
}

/// Reasons for a router connection to be dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionDropped {
    /// The connection was explicitly closed.
    Closed,
    /// No data passed through the connection, in either direction, within the specified duration.
    TimedOut(Duration),
    /// A remote connection failed with an error.
    Failed(ConnectionError),
    /// A local agent failed.
    AgentFailed,
    /// The promise indicating the reason was dropped (this is likely a bug).
    Unknown,
}

impl Display for ConnectionDropped {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionDropped::Closed => write!(f, "The connection was explicitly closed."),
            ConnectionDropped::TimedOut(t) => write!(f, "The connection timed out after {:?}.", t),
            ConnectionDropped::Failed(err) => write!(f, "The connection failed: '{}'", err),
            ConnectionDropped::AgentFailed => write!(f, "The agent failed."),
            ConnectionDropped::Unknown => write!(f, "The reason could not be determined."),
        }
    }
}

impl ConnectionDropped {
    //The Recoverable trait cannot be implemented as ConnectionDropped is not an Error.
    pub fn is_recoverable(&self) -> bool {
        match self {
            ConnectionDropped::TimedOut(_) => true,
            ConnectionDropped::Failed(err) => err.is_transient(),
            ConnectionDropped::AgentFailed => true,
            _ => false,
        }
    }
}

type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
type EndpointRequest = Request<Result<RawRoute, Unresolvable>>;
type RoutesRequest = Request<HashSet<RelativeUri>>;
type ResolutionRequest = Request<Result<RoutingAddr, RouterError>>;

/// Requests that can be serviced by the plane event loop.
#[derive(Debug)]
pub enum PlaneRoutingRequest {
    /// Get a handle to an agent (starting it where necessary).
    Agent {
        name: RelativeUri,
        request: AgentRequest,
    },
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        id: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for an agent.
    Resolve {
        host: Option<Url>,
        name: RelativeUri,
        request: ResolutionRequest,
    },
    /// Get all of the active routes for the plane.
    Routes(RoutesRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Origin {
    Local(RelativeUri),
    Remote(SchemeSocketAddr),
}

impl Origin {
    /// Returns the part of the path used as a key for retrieving the corresponding
    /// connection manager.
    ///
    /// A Url is used for remote locations, and Node for local ones.
    pub fn get_manager_key(&self) -> Result<String, ConnectionError> {
        match self {
            Origin::Local(relative_uri) => Ok(relative_uri.to_string()),

            Origin::Remote(scheme_socket_addr) => {
                let addr = scheme_socket_addr.to_string();

                Ok(url::Url::parse(&addr)
                    .map_err(|_| ConnectionError::Http(HttpError::invalid_url(addr, None)))?
                    .to_string())
            }
        }
    }
}

impl Display for Origin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Origin::Local(relative_uri) => {
                write!(f, "{}", relative_uri.to_string())
            }
            Origin::Remote(schema_socket_addr) => {
                write!(f, "{}", schema_socket_addr.to_string())
            }
        }
    }
}
