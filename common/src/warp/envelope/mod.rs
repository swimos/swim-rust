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

use std::convert::TryFrom;

use crate::model::{Attr, Item, Value};
use crate::warp::path::RelativePath;
use either::Either;

#[cfg(test)]
mod tests;

const NODE_FIELD: &str = "node";
const LANE_FIELD: &str = "lane";
const PRIO_FIELD: &str = "prio";
const RATE_FIELD: &str = "rate";

const AUTH_TAG: &str = "auth";
const AUTHED_TAG: &str = "authed";
const DEAUTH_TAG: &str = "deauth";
const DEAUTHED_TAG: &str = "deauthed";
const LINK_TAG: &str = "link";
const SYNC_TAG: &str = "sync";
const UNLINK_TAG: &str = "unlink";
const CMD_TAG: &str = "command";
const LINKED_TAG: &str = "linked";
const SYNCED_TAG: &str = "synced";
const UNLINKED_TAG: &str = "unlinked";
const EVENT_TAG: &str = "event";

/// Header for negotiation envelopes (authorization and deauthorization).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NegotiationHeader {
    Auth,
    Deauth,
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

/// Either an authorization or deauthorization request.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NegotiationRequest {
    pub header: NegotiationHeader,
    pub body: Option<Value>,
}

/// Either an authorization or deauthorization response.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NegotiationResponse {
    pub header: NegotiationHeader,
    pub body: Option<Value>,
}

/// Header for outgoing link envelopes.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OutgoingHeader {
    Link(LinkParams),
    Sync(LinkParams),
    Unlink,
    Command,
}

/// Header for incoming link envelopes.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum IncomingHeader {
    Linked(LinkParams),
    Synced,
    Unlinked,
    Event,
}

pub type LinkHeader = Either<IncomingHeader, OutgoingHeader>;

/// Whether an envelope is a request or a response.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Direction {
    Request,
    Response,
}

/// Header for any envelope, completely describing the type.
#[derive(Debug, PartialEq, Clone)]
pub enum EnvelopeHeader {
    IncomingLink(IncomingHeader, RelativePath),
    OutgoingLink(OutgoingHeader, RelativePath),
    Negotiation(NegotiationHeader, Direction),
}

/// A message related to a link to or from a remote lane.
#[derive(Debug, PartialEq, Clone)]
pub struct LinkMessage<Header> {
    pub header: Header,
    pub path: RelativePath,
    pub body: Option<Value>,
}

pub type OutgoingLinkMessage = LinkMessage<OutgoingHeader>;
pub type IncomingLinkMessage = LinkMessage<IncomingHeader>;
pub type AnyLinkMessage = LinkMessage<LinkHeader>;

impl<Header> LinkMessage<Header> {
    fn make_message<S: Into<String>>(
        header: Header,
        node: S,
        lane: S,
        body: Option<Value>,
    ) -> Self {
        let path = RelativePath {
            node: node.into(),
            lane: lane.into(),
        };
        LinkMessage { header, path, body }
    }
}

impl LinkMessage<OutgoingHeader> {
    pub fn make_link<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_message(
            OutgoingHeader::Link(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_unlink<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_message(OutgoingHeader::Unlink, node, lane, body)
    }

    pub fn make_sync<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_message(
            OutgoingHeader::Sync(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_command<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_message(OutgoingHeader::Command, node, lane, body)
    }

    pub fn link<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_link(node, lane, None, None, None)
    }

    pub fn sync<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_sync(node, lane, None, None, None)
    }

    pub fn unlink<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_unlink(node, lane, None)
    }
}

impl LinkMessage<IncomingHeader> {
    pub fn make_linked<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_message(
            IncomingHeader::Linked(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_unlinked<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_message(IncomingHeader::Unlinked, node, lane, body)
    }

    pub fn make_synced<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_message(IncomingHeader::Synced, node, lane, body)
    }

    pub fn make_event<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_message(IncomingHeader::Event, node, lane, body)
    }

    pub fn unlinked<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_unlinked(node, lane, None)
    }

    pub fn linked<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_linked(node, lane, None, None, None)
    }

    pub fn synced<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_synced(node, lane, None)
    }
}

/// Model for Warp protocol envelopes.
#[derive(Debug, PartialEq, Clone)]
pub struct Envelope {
    pub header: EnvelopeHeader,
    pub body: Option<Value>,
}

impl Envelope {
    /// Determine the kind of a message.
    pub fn into_message(self) -> AnyMessage {
        let Envelope { header, body } = self;
        match header {
            EnvelopeHeader::IncomingLink(header, path) => {
                AnyMessage::IncomingLink(IncomingLinkMessage { header, path, body })
            }
            EnvelopeHeader::OutgoingLink(header, path) => {
                AnyMessage::OutgoingLink(OutgoingLinkMessage { header, path, body })
            }
            EnvelopeHeader::Negotiation(header, Direction::Request) => {
                AnyMessage::NegotiationRequest(NegotiationRequest { header, body })
            }
            EnvelopeHeader::Negotiation(header, Direction::Response) => {
                AnyMessage::NegotiationResponse(NegotiationResponse { header, body })
            }
        }
    }

    /// Determine if this message is an incoming message.
    pub fn into_incoming(self) -> Result<IncomingLinkMessage, Self> {
        let Envelope { header, body } = self;
        match header {
            EnvelopeHeader::IncomingLink(header, path) => {
                Ok(IncomingLinkMessage { header, path, body })
            }
            _ => Err(Envelope { header, body }),
        }
    }

    /// Determine if this message is an outgoing message.
    pub fn into_outgoing(self) -> Result<OutgoingLinkMessage, Self> {
        let Envelope { header, body } = self;
        match header {
            EnvelopeHeader::OutgoingLink(header, path) => {
                Ok(OutgoingLinkMessage { header, path, body })
            }
            _ => Err(Envelope { header, body }),
        }
    }

    /// Determine if this message is a negotiation request.
    pub fn into_negotiation_request(self) -> Result<NegotiationRequest, Self> {
        let Envelope { header, body } = self;
        match header {
            EnvelopeHeader::Negotiation(header, Direction::Request) => {
                Ok(NegotiationRequest { header, body })
            }
            _ => Err(Envelope { header, body }),
        }
    }

    /// Determine if this message is a negotiation response.
    pub fn into_negotiation_response(self) -> Result<NegotiationResponse, Self> {
        let Envelope { header, body } = self;
        match header {
            EnvelopeHeader::Negotiation(header, Direction::Response) => {
                Ok(NegotiationResponse { header, body })
            }
            _ => Err(Envelope { header, body }),
        }
    }

    fn make_incoming<S: Into<String>>(
        header: IncomingHeader,
        node: S,
        lane: S,
        body: Option<Value>,
    ) -> Self {
        let path = RelativePath {
            node: node.into(),
            lane: lane.into(),
        };
        Envelope {
            header: EnvelopeHeader::IncomingLink(header, path),
            body,
        }
    }

    fn make_outgoing<S: Into<String>>(
        header: OutgoingHeader,
        node: S,
        lane: S,
        body: Option<Value>,
    ) -> Self {
        let path = RelativePath {
            node: node.into(),
            lane: lane.into(),
        };
        Envelope {
            header: EnvelopeHeader::OutgoingLink(header, path),
            body,
        }
    }

    pub fn make_sync<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_outgoing(
            OutgoingHeader::Sync(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_link<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_outgoing(
            OutgoingHeader::Link(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_linked<S: Into<String>>(
        node: S,
        lane: S,
        rate: Option<f64>,
        prio: Option<f64>,
        body: Option<Value>,
    ) -> Self {
        Self::make_incoming(
            IncomingHeader::Linked(LinkParams::new(rate, prio)),
            node,
            lane,
            body,
        )
    }

    pub fn make_auth(body: Option<Value>) -> Self {
        Envelope {
            header: EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Request),
            body,
        }
    }

    pub fn make_authed(body: Option<Value>) -> Self {
        Envelope {
            header: EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Response),
            body,
        }
    }

    pub fn make_deauth(body: Option<Value>) -> Self {
        Envelope {
            header: EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Request),
            body,
        }
    }

    pub fn make_deauthed(body: Option<Value>) -> Self {
        Envelope {
            header: EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Response),
            body,
        }
    }

    pub fn make_unlink<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_outgoing(OutgoingHeader::Unlink, node, lane, body)
    }

    pub fn make_unlinked<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_incoming(IncomingHeader::Unlinked, node, lane, body)
    }

    pub fn make_command<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_outgoing(OutgoingHeader::Command, node, lane, body)
    }

    pub fn make_event<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_incoming(IncomingHeader::Event, node, lane, body)
    }

    pub fn make_synced<S: Into<String>>(node: S, lane: S, body: Option<Value>) -> Self {
        Self::make_incoming(IncomingHeader::Synced, node, lane, body)
    }
}

/// Enumeration that splits out each type of message at the top level.
pub enum AnyMessage {
    OutgoingLink(OutgoingLinkMessage),
    IncomingLink(IncomingLinkMessage),
    NegotiationRequest(NegotiationRequest),
    NegotiationResponse(NegotiationResponse),
}

impl Envelope {
    /// Returns the tag (envelope type) of the current [`Envelope`] variant.
    pub fn tag(&self) -> &'static str {
        match self.header {
            EnvelopeHeader::IncomingLink(IncomingHeader::Linked(_), _) => LINKED_TAG,
            EnvelopeHeader::IncomingLink(IncomingHeader::Synced, _) => SYNC_TAG,
            EnvelopeHeader::IncomingLink(IncomingHeader::Unlinked, _) => UNLINKED_TAG,
            EnvelopeHeader::IncomingLink(IncomingHeader::Event, _) => EVENT_TAG,
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Link(_), _) => LINK_TAG,
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Sync(_), _) => SYNC_TAG,
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Unlink, _) => UNLINK_TAG,
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Command, _) => CMD_TAG,
            EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Request) => AUTH_TAG,
            EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Request) => {
                DEAUTH_TAG
            }
            EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Response) => AUTHED_TAG,
            EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Response) => {
                DEAUTHED_TAG
            }
        }
    }

    pub fn link<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_link(node, lane, None, None, None)
    }

    pub fn sync<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_sync(node, lane, None, None, None)
    }

    pub fn unlink<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_unlink(node, lane, None)
    }

    pub fn unlinked<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_unlinked(node, lane, None)
    }

    pub fn linked<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_linked(node, lane, None, None, None)
    }

    pub fn synced<S: Into<String>>(node: S, lane: S) -> Self {
        Self::make_synced(node, lane, None)
    }
}

/// Errors that may occur when parsing a [`Value`] in to an [`Envelope`]. A variant's associated
/// data is the cause of the error.
#[derive(Debug, PartialEq)]
pub enum EnvelopeParseErr {
    MissingHeader(String),
    UnexpectedKey(String),
    DuplicateKey(String),
    UnexpectedType(Value),
    UnexpectedItem(Item),
    Malformatted,
    DuplicateHeader(String),
    UnknownTag(String),
}

/// Attempt to parse a ['Value'] in to an ['Envelope']. Returning either the parsed [`Envelope`] or
/// an [`EnvelopeParseErr`] detailing the failure cause.
///
/// # Examples
/// ```
/// use std::convert::TryFrom;
/// use common::model::{Value, Attr, Item};
/// use common::warp::envelope::Envelope;
///
/// let record = Value::Record(
///         vec![
///             Attr::of(("command", Value::Record(
///                 Vec::new(),
///                 vec![
///                   Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
///                   Item::Slot(Value::Text(String::from("lane")), Value::Text(String::from("lane_uri"))),
///               ],
///             ))),
///         ],
///         Vec::new(),
///     );
///
/// let envelope = Envelope::try_from(record).unwrap();
/// assert_eq!(envelope, Envelope::make_command("node_uri".to_string(), "lane_uri".to_string(), None));
/// ```
/// An ['Envelope'] is formed of named headers and a value, with the exception of `node` and `lane` where they
/// may be positional; `node` at header index 0 and `lane` at header index 1. See [`Envelope`] for
/// a list of the acceptable variants.
impl TryFrom<Value> for Envelope {
    type Error = EnvelopeParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let (mut attrs, mut body) = match value {
            Value::Record(a, i) => (a, i),
            v => {
                return Err(EnvelopeParseErr::UnexpectedType(v));
            }
        };

        let body = {
            if attrs.len() > 1 {
                Some(Value::Record(attrs.drain(1..).collect(), body))
            } else if body.len() == 1 {
                match body.pop() {
                    Some(item) => match item {
                        Item::ValueItem(inner) => Some(inner),
                        i => return Err(EnvelopeParseErr::UnexpectedItem(i)),
                    },
                    None => None,
                }
            } else {
                None
            }
        };

        let envelope_header = match attrs.pop() {
            Some(v) => v,
            None => return Err(EnvelopeParseErr::Malformatted),
        };

        let Attr { name: tag, value } = envelope_header;

        match tag.as_str() {
            LINKED_TAG => {
                let (path, params) = extract_path_and_params(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::IncomingLink(IncomingHeader::Linked(params), path),
                    body,
                })
            }
            SYNCED_TAG => {
                let path = extract_path(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::IncomingLink(IncomingHeader::Synced, path),
                    body,
                })
            }
            UNLINKED_TAG => {
                let path = extract_path(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::IncomingLink(IncomingHeader::Unlinked, path),
                    body,
                })
            }
            EVENT_TAG => {
                let path = extract_path(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::IncomingLink(IncomingHeader::Event, path),
                    body,
                })
            }
            LINK_TAG => {
                let (path, params) = extract_path_and_params(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::OutgoingLink(OutgoingHeader::Link(params), path),
                    body,
                })
            }
            SYNC_TAG => {
                let (path, params) = extract_path_and_params(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::OutgoingLink(OutgoingHeader::Sync(params), path),
                    body,
                })
            }
            UNLINK_TAG => {
                let path = extract_path(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::OutgoingLink(OutgoingHeader::Unlink, path),
                    body,
                })
            }
            CMD_TAG => {
                let path = extract_path(value)?;
                Ok(Envelope {
                    header: EnvelopeHeader::OutgoingLink(OutgoingHeader::Command, path),
                    body,
                })
            }
            AUTH_TAG => {
                if value != Value::Extant {
                    Err(EnvelopeParseErr::UnexpectedType(value))
                } else {
                    Ok(Envelope {
                        header: EnvelopeHeader::Negotiation(
                            NegotiationHeader::Auth,
                            Direction::Request,
                        ),
                        body,
                    })
                }
            }
            DEAUTH_TAG => {
                if value != Value::Extant {
                    Err(EnvelopeParseErr::UnexpectedType(value))
                } else {
                    Ok(Envelope {
                        header: EnvelopeHeader::Negotiation(
                            NegotiationHeader::Deauth,
                            Direction::Request,
                        ),
                        body,
                    })
                }
            }
            AUTHED_TAG => {
                if value != Value::Extant {
                    Err(EnvelopeParseErr::UnexpectedType(value))
                } else {
                    Ok(Envelope {
                        header: EnvelopeHeader::Negotiation(
                            NegotiationHeader::Auth,
                            Direction::Response,
                        ),
                        body,
                    })
                }
            }
            DEAUTHED_TAG => {
                if value != Value::Extant {
                    Err(EnvelopeParseErr::UnexpectedType(value))
                } else {
                    Ok(Envelope {
                        header: EnvelopeHeader::Negotiation(
                            NegotiationHeader::Deauth,
                            Direction::Response,
                        ),
                        body,
                    })
                }
            }
            s => Err(EnvelopeParseErr::UnknownTag(String::from(s))),
        }
    }
}

fn extract_path(items: Value) -> Result<RelativePath, EnvelopeParseErr> {
    match extract_path_and_params(items)? {
        (
            path,
            LinkParams {
                rate: None,
                prio: None,
            },
        ) => Ok(path),
        (_, LinkParams { rate: Some(_), .. }) => {
            Err(EnvelopeParseErr::UnexpectedKey(RATE_FIELD.to_string()))
        }
        _ => Err(EnvelopeParseErr::UnexpectedKey(PRIO_FIELD.to_string())),
    }
}

const POS_NODE_ORDINAL: usize = 0;
const POS_LANE_ORDINAL: usize = 1;

fn extract_path_and_params(items: Value) -> Result<(RelativePath, LinkParams), EnvelopeParseErr> {
    match items {
        Value::Record(attrs, items) if attrs.is_empty() => {
            let parts = items.into_iter().enumerate().try_fold(
                (None, None, None, None),
                |(mut node, mut lane, mut rate, mut prio), (index, item)| match item {
                    Item::Slot(Value::Text(name), value) if name == NODE_FIELD => {
                        get_text(NODE_FIELD, &mut node, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::ValueItem(value) if index == POS_NODE_ORDINAL => {
                        get_text(NODE_FIELD, &mut node, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::Slot(Value::Text(name), value) if name == LANE_FIELD => {
                        get_text(LANE_FIELD, &mut lane, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::ValueItem(value) if index == POS_LANE_ORDINAL => {
                        get_text(LANE_FIELD, &mut lane, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::Slot(Value::Text(name), value) if name == RATE_FIELD => {
                        get_float(RATE_FIELD, &mut rate, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::Slot(Value::Text(name), value) if name == PRIO_FIELD => {
                        get_float(PRIO_FIELD, &mut prio, value)?;
                        Ok((node, lane, rate, prio))
                    }
                    Item::Slot(Value::Text(name), _) => Err(EnvelopeParseErr::UnexpectedKey(name)),
                    ow => Err(EnvelopeParseErr::UnexpectedItem(ow)),
                },
            );
            match parts? {
                (Some(node), Some(lane), rate, prio) => {
                    Ok((RelativePath { node, lane }, LinkParams { rate, prio }))
                }
                (Some(_), _, _, _) => Err(EnvelopeParseErr::MissingHeader(LANE_FIELD.to_string())),
                _ => Err(EnvelopeParseErr::MissingHeader(NODE_FIELD.to_string())),
            }
        }
        ow => Err(EnvelopeParseErr::UnexpectedType(ow)),
    }
}

fn get_text(name: &str, target: &mut Option<String>, value: Value) -> Result<(), EnvelopeParseErr> {
    if target.is_some() {
        Err(EnvelopeParseErr::DuplicateHeader(name.to_string()))
    } else {
        match value {
            Value::Text(node_str) => {
                *target = Some(node_str);
                Ok(())
            }
            bad_value => Err(EnvelopeParseErr::UnexpectedType(bad_value)),
        }
    }
}

fn get_float(name: &str, target: &mut Option<f64>, value: Value) -> Result<(), EnvelopeParseErr> {
    if target.is_some() {
        Err(EnvelopeParseErr::DuplicateHeader(name.to_string()))
    } else {
        match value {
            Value::Float64Value(x) => {
                *target = Some(x);
                Ok(())
            }
            bad_value => Err(EnvelopeParseErr::UnexpectedType(bad_value)),
        }
    }
}

impl From<Envelope> for Value {
    fn from(envelope: Envelope) -> Self {
        let Envelope { header, body } = envelope;
        let header_attr = match header {
            EnvelopeHeader::IncomingLink(IncomingHeader::Linked(params), path) => {
                Attr::with_items(LINKED_TAG, header_slots(path, Some(params)))
            }
            EnvelopeHeader::IncomingLink(IncomingHeader::Synced, path) => {
                Attr::with_items(SYNCED_TAG, header_slots(path, None))
            }
            EnvelopeHeader::IncomingLink(IncomingHeader::Unlinked, path) => {
                Attr::with_items(UNLINKED_TAG, header_slots(path, None))
            }
            EnvelopeHeader::IncomingLink(IncomingHeader::Event, path) => {
                Attr::with_items(EVENT_TAG, header_slots(path, None))
            }
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Link(params), path) => {
                Attr::with_items(LINK_TAG, header_slots(path, Some(params)))
            }
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Sync(params), path) => {
                Attr::with_items(SYNC_TAG, header_slots(path, Some(params)))
            }
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Unlink, path) => {
                Attr::with_items(UNLINK_TAG, header_slots(path, None))
            }
            EnvelopeHeader::OutgoingLink(OutgoingHeader::Command, path) => {
                Attr::with_items(CMD_TAG, header_slots(path, None))
            }
            EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Request) => {
                Attr::from(AUTH_TAG)
            }
            EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Request) => {
                Attr::from(DEAUTH_TAG)
            }
            EnvelopeHeader::Negotiation(NegotiationHeader::Auth, Direction::Response) => {
                Attr::from(AUTHED_TAG)
            }
            EnvelopeHeader::Negotiation(NegotiationHeader::Deauth, Direction::Response) => {
                Attr::from(DEAUTHED_TAG)
            }
        };
        let mut attrs = vec![header_attr];
        let envelope = match body {
            Some(Value::Record(mut body_attrs, body)) => {
                attrs.append(&mut body_attrs);
                body
            }
            Some(ow) => vec![Item::ValueItem(ow)],
            _ => vec![],
        };
        Value::Record(attrs, envelope)
    }
}

fn header_slots(path: RelativePath, params: Option<LinkParams>) -> Vec<Item> {
    let RelativePath { node, lane } = path;
    let mut slots = vec![Item::slot(NODE_FIELD, node), Item::slot(LANE_FIELD, lane)];
    if let Some(LinkParams { rate, prio }) = params {
        if let Some(r) = rate {
            slots.push(Item::slot(RATE_FIELD, r))
        }
        if let Some(p) = prio {
            slots.push(Item::slot(PRIO_FIELD, p))
        }
    }
    slots
}
