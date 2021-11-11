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

use swim_form::structural::write::StructuralWritable;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_model::{Attr, Text, Value};

const NODE_NOT_FOUND_TAG: &str = "nodeNotFound";
const LANE_NOT_FOUND_TAG: &str = "laneNotFound";

/// Model for Warp protocol envelopes.
#[derive(Clone, Debug, PartialEq, Form)]
pub enum Envelope {
    #[form(tag = "auth")]
    Auth {
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "deauth")]
    DeAuth {
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "link")]
    Link {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "sync")]
    Sync {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlink")]
    Unlink {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "command")]
    Command {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "linked")]
    Linked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        rate: Option<f64>,
        prio: Option<f64>,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "synced")]
    Synced {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "unlinked")]
    Unlinked {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
    #[form(tag = "event")]
    Event {
        #[form(name = "node")]
        node_uri: Text,
        #[form(name = "lane")]
        lane_uri: Text,
        #[form(body)]
        body: Option<Value>,
    },
}

/// Parameters to configure a Warp link.
#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct LinkParams {
    pub rate: Option<f64>,
    pub prio: Option<f64>,
}

impl LinkParams {
    pub fn new(rate: Option<f64>, prio: Option<f64>) -> Self {
        LinkParams { rate, prio }
    }
}

/// Envelopes for negotiating a connection (auth/deauth).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationEnvelope {
    kind: NegotiationKind,
    body: Option<Value>,
}

/// Envelopes directed to an agent.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestEnvelope {
    Link(RelativePath, LinkParams, Option<Value>),
    Sync(RelativePath, LinkParams, Option<Value>),
    Unlink(RelativePath, Option<Value>),
    Command(RelativePath, Option<Value>),
}

/// Agents produced by an agent.
#[derive(Clone, Debug, PartialEq)]
pub enum ResponseEnvelope {
    Linked(RelativePath, LinkParams, Option<Value>),
    Synced(RelativePath, Option<Value>),
    Unlinked(RelativePath, Option<Value>),
    Event(RelativePath, Option<Value>),
}

/// Specializes an [`Envelope`] into one of three different sub-types.
#[derive(Clone, Debug, PartialEq)]
pub enum DiscriminatedEnvelope {
    Negotation(NegotiationEnvelope),
    Request(RequestEnvelope),
    Response(ResponseEnvelope),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeKind {
    Auth,
    DeAuth,
    Link,
    Sync,
    Unlink,
    Command,
    Linked,
    Synced,
    Event,
    Unlinked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NegotiationKind {
    Auth,
    DeAuth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Link,
    Sync,
    Unlink,
    Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseKind {
    Linked,
    Synced,
    Unlinked,
    Event,
}

enum AddressedKind {
    Command,
    Synced,
    Event,
    Unlink,
    Unlinked,
}

/// Builder type for envelopes corresponding to a lane but without link parameters.
pub struct AddressedBuilder {
    kind: AddressedKind,
    node_uri: Text,
    lane_uri: Text,
    body: Option<Value>,
}

enum LinkEnvKind {
    Link,
    Sync,
    Linked,
}

/// Builder type for envelopes corresponding to a lane with link parameters.
pub struct LinkEnvelopeBuilder {
    kind: LinkEnvKind,
    node_uri: Text,
    lane_uri: Text,
    rate: Option<f64>,
    prio: Option<f64>,
    body: Option<Value>,
}

impl AddressedBuilder {
    fn new(kind: AddressedKind) -> Self {
        AddressedBuilder {
            kind,
            node_uri: Text::empty(),
            lane_uri: Text::empty(),
            body: None,
        }
    }

    pub fn node_uri<T: Into<Text>>(mut self, uri: T) -> Self {
        self.node_uri = uri.into();
        self
    }

    pub fn lane_uri<T: Into<Text>>(mut self, uri: T) -> Self {
        self.lane_uri = uri.into();
        self
    }

    pub fn with_body<T: StructuralWritable>(mut self, body: Option<T>) -> Self {
        self.body = body.map(|t| t.into_structure());
        self
    }

    pub fn body<T: StructuralWritable>(mut self, body: T) -> Self {
        self.body = Some(body.into_structure());
        self
    }

    /// Build the completed envelope.
    pub fn done(self) -> Envelope {
        let AddressedBuilder {
            kind,
            node_uri,
            lane_uri,
            body,
        } = self;
        if node_uri.is_empty() || lane_uri.is_empty() {
            panic!("Boom!");
        }
        match kind {
            AddressedKind::Command => Envelope::Command {
                node_uri,
                lane_uri,
                body,
            },
            AddressedKind::Synced => Envelope::Synced {
                node_uri,
                lane_uri,
                body,
            },
            AddressedKind::Event => Envelope::Event {
                node_uri,
                lane_uri,
                body,
            },
            AddressedKind::Unlink => Envelope::Unlink {
                node_uri,
                lane_uri,
                body,
            },
            AddressedKind::Unlinked => Envelope::Unlinked {
                node_uri,
                lane_uri,
                body,
            },
        }
    }
}

impl LinkEnvelopeBuilder {
    fn new(kind: LinkEnvKind) -> Self {
        LinkEnvelopeBuilder {
            kind,
            node_uri: Text::empty(),
            lane_uri: Text::empty(),
            rate: None,
            prio: None,
            body: None,
        }
    }

    pub fn node_uri<T: Into<Text>>(mut self, uri: T) -> Self {
        self.node_uri = uri.into();
        self
    }

    pub fn lane_uri<T: Into<Text>>(mut self, uri: T) -> Self {
        self.lane_uri = uri.into();
        self
    }

    pub fn with_body<T: StructuralWritable>(mut self, body: Option<T>) -> Self {
        self.body = body.map(|t| t.into_structure());
        self
    }

    pub fn body<T: StructuralWritable>(mut self, body: T) -> Self {
        self.body = Some(body.into_structure());
        self
    }

    pub fn link_params(mut self, params: LinkParams) -> Self {
        let LinkParams { rate, prio } = params;
        self.rate = rate;
        self.prio = prio;
        self
    }

    pub fn rate(mut self, value: f64) -> Self {
        self.rate = Some(value);
        self
    }

    pub fn priority(mut self, value: f64) -> Self {
        self.prio = Some(value);
        self
    }

    /// Build the completed envelope.
    pub fn done(self) -> Envelope {
        let LinkEnvelopeBuilder {
            kind,
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        } = self;
        match kind {
            LinkEnvKind::Link => Envelope::Link {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            },
            LinkEnvKind::Sync => Envelope::Sync {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            },
            LinkEnvKind::Linked => Envelope::Linked {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            },
        }
    }
}

impl Envelope {
    /// Create an authorizaton envelope with no body.
    pub fn auth_empty() -> Envelope {
        Envelope::Auth { body: None }
    }

    /// Create an authorizaton envelope with a body.
    pub fn auth<T: StructuralWritable>(body: T) -> Envelope {
        Envelope::Auth {
            body: Some(body.into_structure()),
        }
    }

    /// Create an deauthorizaton envelope with no body.
    pub fn deauth_empty() -> Envelope {
        Envelope::DeAuth { body: None }
    }

    /// Create an deauthorizaton envelope with a body.
    pub fn deauth<T: StructuralWritable>(body: T) -> Envelope {
        Envelope::DeAuth {
            body: Some(body.into_structure()),
        }
    }

    /// Get a builder for link envelopes.
    pub fn link() -> LinkEnvelopeBuilder {
        LinkEnvelopeBuilder::new(LinkEnvKind::Link)
    }

    /// Get a builder for sync envelopes.
    pub fn sync() -> LinkEnvelopeBuilder {
        LinkEnvelopeBuilder::new(LinkEnvKind::Sync)
    }

    /// Get a builder for unlink envelopes.
    pub fn unlink() -> AddressedBuilder {
        AddressedBuilder::new(AddressedKind::Unlink)
    }

    /// Get a builder for command envelopes.
    pub fn command() -> AddressedBuilder {
        AddressedBuilder::new(AddressedKind::Command)
    }

    /// Get a builder for linked envelopes.
    pub fn linked() -> LinkEnvelopeBuilder {
        LinkEnvelopeBuilder::new(LinkEnvKind::Linked)
    }

    /// Get a builder for synced envelopes.
    pub fn synced() -> AddressedBuilder {
        AddressedBuilder::new(AddressedKind::Synced)
    }

    /// Get a builder for unlinked envelopes.
    pub fn unlinked() -> AddressedBuilder {
        AddressedBuilder::new(AddressedKind::Unlinked)
    }

    /// Get a builder for event envelopes.
    pub fn event() -> AddressedBuilder {
        AddressedBuilder::new(AddressedKind::Event)
    }

    /// Convenience method to create a node not found response.
    pub fn node_not_found<N, L>(node: N, lane: L) -> Self
    where
        N: Into<Text>,
        L: Into<Text>,
    {
        Envelope::Unlinked {
            node_uri: node.into(),
            lane_uri: lane.into(),
            body: Some(Value::of_attr(Attr::of(NODE_NOT_FOUND_TAG))),
        }
    }

    /// Convenience method to create a lane not found response.
    pub fn lane_not_found<N, L>(node: N, lane: L) -> Self
    where
        N: Into<Text>,
        L: Into<Text>,
    {
        Envelope::Unlinked {
            node_uri: node.into(),
            lane_uri: lane.into(),
            body: Some(Value::of_attr(Attr::of(LANE_NOT_FOUND_TAG))),
        }
    }

    pub fn kind(&self) -> EnvelopeKind {
        match self {
            Envelope::Auth { .. } => EnvelopeKind::Auth,
            Envelope::DeAuth { .. } => EnvelopeKind::DeAuth,
            Envelope::Link { .. } => EnvelopeKind::Link,
            Envelope::Sync { .. } => EnvelopeKind::Sync,
            Envelope::Unlink { .. } => EnvelopeKind::Unlink,
            Envelope::Command { .. } => EnvelopeKind::Command,
            Envelope::Linked { .. } => EnvelopeKind::Linked,
            Envelope::Synced { .. } => EnvelopeKind::Synced,
            Envelope::Unlinked { .. } => EnvelopeKind::Unlinked,
            Envelope::Event { .. } => EnvelopeKind::Event,
        }
    }

    pub fn path(&self) -> Option<RelativePath> {
        match self.disriminate_header() {
            EnvelopeHeader::Request(path) => Some(path),
            EnvelopeHeader::Response(path) => Some(path),
            _ => None,
        }
    }

    pub fn body(&self) -> Option<&Value> {
        match self {
            Envelope::Auth { body, .. } => body.as_ref(),
            Envelope::DeAuth { body, .. } => body.as_ref(),
            Envelope::Link { body, .. } => body.as_ref(),
            Envelope::Sync { body, .. } => body.as_ref(),
            Envelope::Unlink { body, .. } => body.as_ref(),
            Envelope::Command { body, .. } => body.as_ref(),
            Envelope::Linked { body, .. } => body.as_ref(),
            Envelope::Synced { body, .. } => body.as_ref(),
            Envelope::Unlinked { body, .. } => body.as_ref(),
            Envelope::Event { body, .. } => body.as_ref(),
        }
    }

    pub fn into_negotiation(self) -> Option<NegotiationEnvelope> {
        match self {
            Envelope::Auth { body } => Some(NegotiationEnvelope {
                kind: NegotiationKind::Auth,
                body,
            }),
            Envelope::DeAuth { body } => Some(NegotiationEnvelope {
                kind: NegotiationKind::DeAuth,
                body,
            }),
            _ => None,
        }
    }

    pub fn into_request(self) -> Option<RequestEnvelope> {
        match self {
            Envelope::Link {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => Some(RequestEnvelope::Link(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Sync {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => Some(RequestEnvelope::Sync(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Unlink {
                node_uri,
                lane_uri,
                body,
            } => Some(RequestEnvelope::Unlink(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Command {
                node_uri,
                lane_uri,
                body,
            } => Some(RequestEnvelope::Command(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            _ => None,
        }
    }

    pub fn into_response(self) -> Option<ResponseEnvelope> {
        match self {
            Envelope::Linked {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => Some(ResponseEnvelope::Linked(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Synced {
                node_uri,
                lane_uri,
                body,
            } => Some(ResponseEnvelope::Synced(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Unlinked {
                node_uri,
                lane_uri,
                body,
            } => Some(ResponseEnvelope::Unlinked(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Event {
                node_uri,
                lane_uri,
                body,
            } => Some(ResponseEnvelope::Event(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            _ => None,
        }
    }

    /// Split into one of the three specialized envelope types.
    pub fn discriminate(self) -> DiscriminatedEnvelope {
        match self {
            Envelope::Auth { body } => DiscriminatedEnvelope::Negotation(NegotiationEnvelope {
                kind: NegotiationKind::Auth,
                body,
            }),
            Envelope::DeAuth { body } => DiscriminatedEnvelope::Negotation(NegotiationEnvelope {
                kind: NegotiationKind::DeAuth,
                body,
            }),
            Envelope::Link {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => DiscriminatedEnvelope::Request(RequestEnvelope::Link(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Sync {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => DiscriminatedEnvelope::Request(RequestEnvelope::Sync(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Unlink {
                node_uri,
                lane_uri,
                body,
            } => DiscriminatedEnvelope::Request(RequestEnvelope::Unlink(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Command {
                node_uri,
                lane_uri,
                body,
            } => DiscriminatedEnvelope::Request(RequestEnvelope::Command(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Linked {
                node_uri,
                lane_uri,
                rate,
                prio,
                body,
            } => DiscriminatedEnvelope::Response(ResponseEnvelope::Linked(
                RelativePath::new(node_uri, lane_uri),
                LinkParams::new(rate, prio),
                body,
            )),
            Envelope::Synced {
                node_uri,
                lane_uri,
                body,
            } => DiscriminatedEnvelope::Response(ResponseEnvelope::Synced(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Unlinked {
                node_uri,
                lane_uri,
                body,
            } => DiscriminatedEnvelope::Response(ResponseEnvelope::Unlinked(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
            Envelope::Event {
                node_uri,
                lane_uri,
                body,
            } => DiscriminatedEnvelope::Response(ResponseEnvelope::Event(
                RelativePath::new(node_uri, lane_uri),
                body,
            )),
        }
    }

    pub fn disriminate_header(&self) -> EnvelopeHeader {
        match self {
            Envelope::Auth { .. } => EnvelopeHeader::Negotiation,
            Envelope::DeAuth { .. } => EnvelopeHeader::Negotiation,
            Envelope::Link {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Request(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Request(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Unlink {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Request(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Command {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Request(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Linked {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Response(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Synced {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Response(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Unlinked {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Response(RelativePath::new(node_uri.clone(), lane_uri.clone())),
            Envelope::Event {
                node_uri, lane_uri, ..
            } => EnvelopeHeader::Response(RelativePath::new(node_uri.clone(), lane_uri.clone())),
        }
    }
}

pub enum EnvelopeHeader {
    Request(RelativePath),
    Response(RelativePath),
    Negotiation,
}

impl RequestEnvelope {
    pub fn kind(&self) -> RequestKind {
        match self {
            RequestEnvelope::Link(..) => RequestKind::Link,
            RequestEnvelope::Sync(..) => RequestKind::Sync,
            RequestEnvelope::Unlink(..) => RequestKind::Unlink,
            RequestEnvelope::Command(..) => RequestKind::Command,
        }
    }

    pub fn body(&self) -> Option<&Value> {
        match self {
            RequestEnvelope::Link(_, _, body) => body.as_ref(),
            RequestEnvelope::Sync(_, _, body) => body.as_ref(),
            RequestEnvelope::Unlink(_, body) => body.as_ref(),
            RequestEnvelope::Command(_, body) => body.as_ref(),
        }
    }

    pub fn into_body(self) -> Option<Value> {
        match self {
            RequestEnvelope::Link(_, _, body) => body,
            RequestEnvelope::Sync(_, _, body) => body,
            RequestEnvelope::Unlink(_, body) => body,
            RequestEnvelope::Command(_, body) => body,
        }
    }

    pub fn path(&self) -> &RelativePath {
        match self {
            RequestEnvelope::Link(path, _, _) => path,
            RequestEnvelope::Sync(path, _, _) => path,
            RequestEnvelope::Unlink(path, _) => path,
            RequestEnvelope::Command(path, _) => path,
        }
    }

    pub fn into_path(self) -> RelativePath {
        match self {
            RequestEnvelope::Link(path, _, _) => path,
            RequestEnvelope::Sync(path, _, _) => path,
            RequestEnvelope::Unlink(path, _) => path,
            RequestEnvelope::Command(path, _) => path,
        }
    }
}

impl ResponseEnvelope {
    pub fn kind(&self) -> ResponseKind {
        match self {
            ResponseEnvelope::Linked(..) => ResponseKind::Linked,
            ResponseEnvelope::Synced(..) => ResponseKind::Synced,
            ResponseEnvelope::Unlinked(..) => ResponseKind::Unlinked,
            ResponseEnvelope::Event(..) => ResponseKind::Event,
        }
    }

    pub fn body(&self) -> Option<&Value> {
        match self {
            ResponseEnvelope::Linked(_, _, body) => body.as_ref(),
            ResponseEnvelope::Synced(_, body) => body.as_ref(),
            ResponseEnvelope::Unlinked(_, body) => body.as_ref(),
            ResponseEnvelope::Event(_, body) => body.as_ref(),
        }
    }

    pub fn into_body(self) -> Option<Value> {
        match self {
            ResponseEnvelope::Linked(_, _, body) => body,
            ResponseEnvelope::Synced(_, body) => body,
            ResponseEnvelope::Unlinked(_, body) => body,
            ResponseEnvelope::Event(_, body) => body,
        }
    }

    pub fn path(&self) -> &RelativePath {
        match self {
            ResponseEnvelope::Linked(path, _, _) => path,
            ResponseEnvelope::Synced(path, _) => path,
            ResponseEnvelope::Unlinked(path, _) => path,
            ResponseEnvelope::Event(path, _) => path,
        }
    }

    pub fn into_path(self) -> RelativePath {
        match self {
            ResponseEnvelope::Linked(path, _, _) => path,
            ResponseEnvelope::Synced(path, _) => path,
            ResponseEnvelope::Unlinked(path, _) => path,
            ResponseEnvelope::Event(path, _) => path,
        }
    }
}

impl From<NegotiationEnvelope> for Envelope {
    fn from(env: NegotiationEnvelope) -> Self {
        let NegotiationEnvelope { kind, body } = env;
        match kind {
            NegotiationKind::Auth => Envelope::Auth { body },
            NegotiationKind::DeAuth => Envelope::DeAuth { body },
        }
    }
}

impl From<RequestEnvelope> for Envelope {
    fn from(env: RequestEnvelope) -> Self {
        match env {
            RequestEnvelope::Link(RelativePath { node, lane }, LinkParams { rate, prio }, body) => {
                Envelope::Link {
                    node_uri: node,
                    lane_uri: lane,
                    rate,
                    prio,
                    body,
                }
            }
            RequestEnvelope::Sync(RelativePath { node, lane }, LinkParams { rate, prio }, body) => {
                Envelope::Sync {
                    node_uri: node,
                    lane_uri: lane,
                    rate,
                    prio,
                    body,
                }
            }
            RequestEnvelope::Unlink(RelativePath { node, lane }, body) => Envelope::Unlink {
                node_uri: node,
                lane_uri: lane,
                body,
            },
            RequestEnvelope::Command(RelativePath { node, lane }, body) => Envelope::Command {
                node_uri: node,
                lane_uri: lane,
                body,
            },
        }
    }
}

impl From<ResponseEnvelope> for Envelope {
    fn from(env: ResponseEnvelope) -> Self {
        match env {
            ResponseEnvelope::Linked(
                RelativePath { node, lane },
                LinkParams { rate, prio },
                body,
            ) => Envelope::Linked {
                node_uri: node,
                lane_uri: lane,
                rate,
                prio,
                body,
            },
            ResponseEnvelope::Synced(RelativePath { node, lane }, body) => Envelope::Synced {
                node_uri: node,
                lane_uri: lane,
                body,
            },
            ResponseEnvelope::Unlinked(RelativePath { node, lane }, body) => Envelope::Unlinked {
                node_uri: node,
                lane_uri: lane,
                body,
            },
            ResponseEnvelope::Event(RelativePath { node, lane }, body) => Envelope::Event {
                node_uri: node,
                lane_uri: lane,
                body,
            },
        }
    }
}

impl RequestEnvelope {
    pub fn link<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        RequestEnvelope::Link(RelativePath::new(node, lane), Default::default(), None)
    }

    pub fn sync<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        RequestEnvelope::Sync(RelativePath::new(node, lane), Default::default(), None)
    }

    pub fn unlink<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        RequestEnvelope::Unlink(RelativePath::new(node, lane), None)
    }

    pub fn command<N1, N2, T>(node: N1, lane: N2, body: T) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
        T: StructuralWritable,
    {
        RequestEnvelope::Command(RelativePath::new(node, lane), Some(body.structure()))
    }
}

impl ResponseEnvelope {
    pub fn linked<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        ResponseEnvelope::Linked(RelativePath::new(node, lane), Default::default(), None)
    }

    pub fn synced<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        ResponseEnvelope::Synced(RelativePath::new(node, lane), None)
    }

    pub fn unlinked<N1, N2>(node: N1, lane: N2) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
    {
        ResponseEnvelope::Unlinked(RelativePath::new(node, lane), None)
    }

    pub fn event<N1, N2, T>(node: N1, lane: N2, body: T) -> Self
    where
        N1: Into<Text>,
        N2: Into<Text>,
        T: StructuralWritable,
    {
        ResponseEnvelope::Event(RelativePath::new(node, lane), Some(body.structure()))
    }
}

#[cfg(test)]
mod tests {

    use super::Envelope;
    use crate::map::MapUpdate;
    use swim_model::Value;
    use swim_recon::printer::print_recon_compact;

    #[test]
    fn auth_envelope_to_recon() {
        let env = Envelope::auth_empty();
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@auth");

        let env = Envelope::auth(2);
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@auth 2");

        let env = Envelope::auth(Value::from_vec(vec![("first", 1), ("second", 2)]));
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@auth{first:1,second:2}");
    }

    #[test]
    fn deauth_envelope_to_recon() {
        let env = Envelope::deauth_empty();
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@deauth");

        let env = Envelope::deauth(2);
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@deauth 2");

        let env = Envelope::deauth(Value::from_vec(vec![("first", 1), ("second", 2)]));
        let recon = format!("{}", print_recon_compact(&env));
        assert_eq!(recon, "@deauth{first:1,second:2}");
    }

    #[test]
    fn link_envelope_to_recon() {
        let env = Envelope::link()
            .node_uri("node")
            .lane_uri("lane")
            .rate(0.5)
            .priority(1.0)
            .done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@link(node:node,lane:lane,rate:0.5,prio:1.0)");
    }

    #[test]
    fn sync_envelope_to_recon() {
        let env = Envelope::sync()
            .node_uri("node")
            .lane_uri("lane")
            .rate(0.5)
            .priority(1.0)
            .done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@sync(node:node,lane:lane,rate:0.5,prio:1.0)");
    }

    #[test]
    fn unlink_envelope_to_recon() {
        let env = Envelope::unlink().node_uri("node").lane_uri("lane").done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@unlink(node:node,lane:lane)");
    }

    #[test]
    fn command_envelope_to_recon() {
        let body: MapUpdate<Value, Value> = MapUpdate::Clear;
        let env = Envelope::command()
            .node_uri("node")
            .lane_uri("lane")
            .body(body)
            .done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@command(node:node,lane:lane)@clear");
    }

    #[test]
    fn linked_envelope_to_recon() {
        let env = Envelope::linked()
            .node_uri("node")
            .lane_uri("lane")
            .rate(0.5)
            .priority(1.0)
            .done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@linked(node:node,lane:lane,rate:0.5,prio:1.0)");

        let env = Envelope::linked().node_uri("node").lane_uri("lane").done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@linked(node:node,lane:lane)");
    }

    #[test]
    fn synced_envelope_to_recon() {
        let env = Envelope::synced().node_uri("node").lane_uri("lane").done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@synced(node:node,lane:lane)");
    }

    #[test]
    fn unlinked_envelope_to_recon() {
        let env = Envelope::unlinked()
            .node_uri("node")
            .lane_uri("lane")
            .done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@unlinked(node:node,lane:lane)");
    }

    #[test]
    fn event_envelope_to_recon() {
        let env = Envelope::event().node_uri("node").lane_uri("lane").done();

        let recon = format!("{}", print_recon_compact(&env));

        assert_eq!(recon, "@event(node:node,lane:lane)");
    }
}
