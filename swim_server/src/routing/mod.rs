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

use crate::routing::remote::{RawRoute, RoutingRequest};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use tokio::sync::mpsc;
use url::Url;

use swim_common::request::Request;
use swim_common::routing::RoutingError;
use swim_common::routing::SendError;
use swim_common::routing::{ConnectionError, ResolutionError};
use swim_common::warp::envelope::{Envelope, EnvelopeHeader, OutgoingLinkMessage};
use tokio::sync::oneshot;
use utilities::errors::Recoverable;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

use crate::agent::meta::{MetaKind, MetaPath};
use crate::routing::error::RouterError;
use swim_common::warp::path::RelativePath;

pub mod error;
pub mod remote;
#[cfg(test)]
mod tests;

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

impl ServerRouterFactory for TopLevelRouterFactory {
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

impl ServerRouter for TopLevelRouter {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
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

/// A key into the server routing table specifying an endpoint to which [`Envelope`]s can be sent.
/// This is deliberately non-descriptive to allow it to be [`Copy`] and so very cheap to use as a
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Location {
    /// Indicates that envelopes will be routed to a remote host.
    RemoteEndpoint(u32),
    /// Indicates that envelopes will be routed to another agent on this host.
    Local(u32),
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

    pub fn is_local(&self) -> bool {
        matches!(self, RoutingAddr(Location::Local(_)))
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
        }
    }
}

pub enum ServerEnvelope {
    AgentEnvelope(Envelope),
    MetaEnvelope(Envelope, MetaKind),
}

impl ServerEnvelope {
    pub fn agent(envelope: Envelope) -> ServerEnvelope {
        ServerEnvelope::AgentEnvelope(envelope)
    }

    pub fn meta(envelope: Envelope, kind: MetaKind) -> ServerEnvelope {
        ServerEnvelope::MetaEnvelope(envelope, kind)
    }

    pub fn relative_path(&self) -> Option<RelativePath> {
        match self {
            ServerEnvelope::AgentEnvelope(inner) => inner.header.relative_path(),
            ServerEnvelope::MetaEnvelope(inner, _) => inner.header.relative_path(),
        }
    }

    pub fn into_envelope(self) -> Envelope {
        match self {
            ServerEnvelope::AgentEnvelope(inner) => inner,
            ServerEnvelope::MetaEnvelope(inner, _) => inner,
        }
    }
}

impl From<Envelope> for ServerEnvelope {
    fn from(envelope: Envelope) -> ServerEnvelope {
        let Envelope { header, body } = envelope;

        if let EnvelopeHeader::IncomingLink(header, path) = header {
            match path.into_kind_and_path() {
                Ok((kind, path)) => ServerEnvelope::meta(
                    Envelope {
                        header: EnvelopeHeader::IncomingLink(header, path),
                        body,
                    },
                    kind,
                ),
                Err(path) => ServerEnvelope::agent(Envelope {
                    header: EnvelopeHeader::IncomingLink(header, path),
                    body,
                }),
            }
        } else {
            ServerEnvelope::agent(Envelope { header, body })
        }
    }
}

/// An [`Envelope`] tagged with the key of the endpoint into routing table from which it originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedAgentEnvelope(pub RoutingAddr, pub Envelope);

impl From<TaggedAgentEnvelope> for TaggedEnvelope {
    fn from(env: TaggedAgentEnvelope) -> Self {
        TaggedEnvelope::AgentEnvelope(env)
    }
}

/// An [`Envelope`] for a meta lane, tagged with the key of the endpoint into routing table from
/// which it originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedMetaEnvelope(pub RoutingAddr, pub Envelope, pub MetaKind);

impl From<TaggedMetaEnvelope> for TaggedEnvelope {
    fn from(env: TaggedMetaEnvelope) -> Self {
        TaggedEnvelope::MetaEnvelope(env)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaggedEnvelope {
    AgentEnvelope(TaggedAgentEnvelope),
    MetaEnvelope(TaggedMetaEnvelope),
}

impl TaggedEnvelope {
    pub fn agent(envelope: TaggedAgentEnvelope) -> TaggedEnvelope {
        TaggedEnvelope::AgentEnvelope(envelope)
    }

    pub fn meta(meta: TaggedMetaEnvelope) -> TaggedEnvelope {
        TaggedEnvelope::MetaEnvelope(meta)
    }

    pub fn split(self) -> (RoutingAddr, Envelope, LaneIdentifierKind) {
        match self {
            TaggedEnvelope::MetaEnvelope(TaggedMetaEnvelope(addr, envelope, ..)) => {
                (addr, envelope, LaneIdentifierKind::Meta)
            }
            TaggedEnvelope::AgentEnvelope(TaggedAgentEnvelope(addr, envelope)) => {
                (addr, envelope, LaneIdentifierKind::Agent)
            }
        }
    }

    pub fn relative_path(&self) -> Option<RelativePath> {
        match self {
            TaggedEnvelope::AgentEnvelope(inner) => inner.1.header.relative_path(),
            TaggedEnvelope::MetaEnvelope(inner) => inner.1.header.relative_path(),
        }
    }

    pub fn addr(&self) -> RoutingAddr {
        match self {
            TaggedEnvelope::AgentEnvelope(inner) => inner.0,
            TaggedEnvelope::MetaEnvelope(inner) => inner.0,
        }
    }

    pub fn envelope(&self) -> &Envelope {
        match self {
            TaggedEnvelope::AgentEnvelope(inner) => &inner.1,
            TaggedEnvelope::MetaEnvelope(inner) => &inner.1,
        }
    }

    pub fn into_envelope(self) -> Envelope {
        match self {
            TaggedEnvelope::AgentEnvelope(inner) => inner.1,
            TaggedEnvelope::MetaEnvelope(inner) => inner.1,
        }
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
pub trait ServerRouter: Send + Sync {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>>;

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Create router instances bound to particular routing addresses.
pub trait ServerRouterFactory: Send + Sync {
    type Router: ServerRouter;

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

    pub async fn transform_and_send(&mut self, envelope: Envelope) -> Result<(), SendError> {
        self.send_item(ServerEnvelope::AgentEnvelope(envelope))
            .await
    }

    pub async fn send_item(&mut self, envelope: ServerEnvelope) -> Result<(), SendError> {
        let TaggedSender { tag, inner } = self;

        let envelope = match envelope {
            ServerEnvelope::AgentEnvelope(envelope) => {
                TaggedEnvelope::agent(TaggedAgentEnvelope(*tag, envelope))
            }
            ServerEnvelope::MetaEnvelope(envelope, kind) => {
                TaggedEnvelope::meta(TaggedMetaEnvelope(*tag, envelope, kind))
            }
        };

        Ok(inner
            .send(envelope)
            .await
            .map_err(|e| SendError::new(RoutingError::CloseError, e.0.into_envelope()))?)
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

/// An abstraction over both agent lanes and meta lanes.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct LaneIdentifier {
    /// The corresponding lane URI.
    lane_uri: String,
    /// The lane's kind.
    kind: LaneIdentifierKind,
}

impl LaneIdentifier {
    pub fn from(lane_uri: String, kind: LaneIdentifierKind) -> LaneIdentifier {
        LaneIdentifier { lane_uri, kind }
    }

    pub fn agent(lane_uri: String) -> LaneIdentifier {
        LaneIdentifier {
            lane_uri,
            kind: LaneIdentifierKind::Agent,
        }
    }

    pub fn meta(lane_uri: String) -> LaneIdentifier {
        LaneIdentifier {
            lane_uri,
            kind: LaneIdentifierKind::Meta,
        }
    }

    pub fn kind(&self) -> LaneIdentifierKind {
        self.kind
    }

    pub fn lane_uri(&self) -> &str {
        &self.lane_uri
    }
}

/// An identifier representing either an agent's lanes or its metadata lanes.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum LaneIdentifierKind {
    /// An agent's lane.
    Agent,
    /// An agent's metadata lanes.
    ///
    /// Agent lane meta requests have their URI's prefixed in the form of `swim:meta:X` and these
    /// are forwarded to the corresponding meta lanes.
    Meta,
}
