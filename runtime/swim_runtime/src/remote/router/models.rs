use crate::error::{NoAgentAtRoute, RoutingError};
use crate::remote::table::BidirectionalRegistrator;
use crate::remote::RawRoute;
use crate::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use swim_utilities::future::request::Request;
use swim_utilities::routing::uri::RelativeUri;
use swim_warp::envelope::ResponseEnvelope;
use tokio::sync::mpsc;
use url::Url;

pub type EndpointRequest = Request<Result<RawRoute, RoutingError>>;
pub type BidirectionalRequest = Request<Result<BidirectionalRegistrator, RoutingError>>;
pub type ConnectionChannel = (TaggedSender, Option<mpsc::Receiver<RouterEvent>>);
pub type AgentRequest = Request<Result<Arc<dyn Any + Send + Sync>, NoAgentAtRoute>>;
pub type RoutesRequest = Request<HashSet<RelativeUri>>;
pub type ResolutionRequest = Request<Result<RoutingAddr, RoutingError>>;
pub type BidirectionalReceiverRequest = Request<mpsc::Receiver<TaggedEnvelope>>;

#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    /// Incoming message from a remote host.
    Message(ResponseEnvelope),
    /// There was an error in the connection. If a retry strategy exists this will trigger it.
    ConnectionClosed,
    /// The remote host is unreachable. This will not trigger the retry system.
    Unreachable(String),
    /// The router is stopping.
    Stopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    /// A connection type that can both send and receive messages.
    Full,
    /// A connection type that can only send messages.
    Outgoing,
}

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
        addr: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for an agent.
    Resolve {
        host: Option<Url>,
        route: RelativeUri,
        request: ResolutionRequest,
    },
    /// Get all of the active routes for the plane.
    Routes(RoutesRequest),
}

/// Requests that are generated by the remote router to be serviced by the connection manager.
#[derive(Debug)]
pub enum RemoteRoutingRequest {
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: EndpointRequest,
    },
    /// Resolve the routing address for a host.
    ResolveUrl {
        host: Url,
        request: ResolutionRequest,
    },
    /// Establish a bidirectional connection.
    Bidirectional {
        host: Url,
        request: BidirectionalRequest,
    },
}

#[derive(Debug)]
pub enum DownlinkRoutingRequest<Path> {
    /// Obtain a connection.
    Connect {
        target: Path,
        request: Request<Result<ConnectionChannel, RoutingError>>,
        conn_type: ConnectionType,
    },
    /// Get channel to route messages to a specified routing address.
    Endpoint {
        addr: RoutingAddr,
        request: Request<Result<RawRoute, RoutingError>>,
    },
}

#[derive(Debug)]
pub enum Address {
    Local(RelativeUri),
    Remote(Url, RelativeUri),
}

impl Address {
    pub fn uri(&self) -> &RelativeUri {
        match self {
            Address::Local(uri) => uri,
            Address::Remote(_, uri) => uri,
        }
    }

    pub fn url(&self) -> Option<&Url> {
        match self {
            Address::Local(_) => None,
            Address::Remote(url, _) => Some(url),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Address::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Address::Remote(_, _))
    }

    pub fn into_string(self) -> String {
        match self {
            Address::Local(addr) => addr.to_string(),
            Address::Remote(mut base, path) => {
                if let Ok(mut parts) = base.path_segments_mut() {
                    parts.push(path.to_string().as_ref());
                }

                base.to_string()
            }
        }
    }
}

impl From<(Option<Url>, RelativeUri)> for Address {
    fn from(p: (Option<Url>, RelativeUri)) -> Self {
        match p {
            (Some(url), uri) => Address::Remote(url, uri),
            (None, uri) => Address::Local(uri),
        }
    }
}

impl From<RelativeUri> for Address {
    fn from(uri: RelativeUri) -> Self {
        Address::Local(uri)
    }
}
