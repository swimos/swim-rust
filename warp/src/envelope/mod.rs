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
use std::ops::Deref;
use model::{Value, Item};

#[cfg(test)]
mod tests;

/// A model to exchange over WARP connections.
#[derive(Debug, PartialEq)]
pub enum Envelope {
    // @link
    LinkRequest(LinkAddressed),
    // @sync
    SyncRequest(LinkAddressed),
    // @linked
    LinkedResponse(LinkAddressed),

    // @event
    EventMessage(LaneAddressed),
    // @command
    CommandMessage(LaneAddressed),
    // @synced
    SyncedResponse(LaneAddressed),
    // @unlink
    UnlinkRequest(LaneAddressed),
    // @unlinked
    UnlinkedResponse(LaneAddressed),

    // @auth
    AuthRequest(HostAddressed),
    // @authed
    AuthedResponse(HostAddressed),
    // @deauth
    DeauthRequest(HostAddressed),
    // @deauthed
    DeauthedResponse(HostAddressed),
}

impl Envelope {
    /// Returns the tag (envelope type) of the current [`Envelope`] variant.
    pub fn tag(self) -> &'static str {
        match self {
            Envelope::LinkRequest(_) => "link",
            Envelope::SyncRequest(_) => "sync",
            Envelope::LinkedResponse(_) => "linked",
            Envelope::EventMessage(_) => "event",
            Envelope::CommandMessage(_) => "command",
            Envelope::SyncedResponse(_) => "synced",
            Envelope::UnlinkRequest(_) => "unlink",
            Envelope::UnlinkedResponse(_) => "unlinked",
            Envelope::AuthRequest(_) => "auth",
            Envelope::AuthedResponse(_) => "authed",
            Envelope::DeauthRequest(_) => "deauth",
            Envelope::DeauthedResponse(_) => "deauthed",
        }
    }
}

/// A simple [`Envelope`] payload to deliver to the other end of an active network connection.
#[derive(Debug, PartialEq)]
pub struct HostAddressed {
    pub body: Option<Value>,
}

/// An [`Envelope`]'s payload that is routed to a particular lane, of a particular node. Both the
/// `node_uri` and `lane_uri` must be provided.
#[derive(Debug, PartialEq)]
pub struct LaneAddressed {
    pub node_uri: String,
    pub lane_uri: String,
    pub body: Option<Value>,
}

/// An [`Envelope`] to route along the path of a currently open link. If the `rate` or `prio` are
/// not provided then they are set to `None`.
#[derive(Debug, PartialEq)]
pub struct LinkAddressed {
    pub lane: LaneAddressed,
    pub rate: Option<f64>,
    pub prio: Option<f64>,
}

/// Errors that may occur when parsing a [`Value`] in to an [`Envelope`]. A variant's associated
/// data is the cause of the error.
#[derive(Debug, PartialEq)]
pub enum EnvelopeParseErr {
    MissingHeader(String),
    UnexpectedKey(String),
    UnexpectedType(Value),
    UnexpectedItem(Item),
    Malformatted,
    DuplicateHeader(String),
    UnknownTag(String),
}

#[derive(Debug, PartialEq)]
struct LaneAddressedBuilder {
    node_uri: Option<String>,
    lane_uri: Option<String>,
    body: Option<Value>,
}

#[derive(Debug, PartialEq)]
struct LinkAddressedBuilder {
    lane: LaneAddressedBuilder,
    rate: Option<f64>,
    prio: Option<f64>,
}

/// Builds a [`LaneAddressed`] variant. Verifying that neither the `node_uri` or `lane_uri` are
/// `None`.
impl LaneAddressedBuilder {
    fn build(self) -> Result<LaneAddressed, EnvelopeParseErr> {
        match self {
            LaneAddressedBuilder {
                node_uri: Some(node_uri),
                lane_uri: Some(lane_uri),
                body,
            } => Ok(LaneAddressed {
                node_uri,
                lane_uri,
                body,
            }),
            LaneAddressedBuilder { node_uri: None, .. } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("node")))
            }
            LaneAddressedBuilder { lane_uri: None, .. } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("lane")))
            }
        }
    }
}

impl LinkAddressedBuilder {
    fn build(self) -> Result<LinkAddressed, EnvelopeParseErr> {
        match self {
            LinkAddressedBuilder { lane, .. } => {
                let lane = lane.build()?;
                Ok(LinkAddressed {
                    lane,
                    rate: self.rate,
                    prio: self.prio,
                })
            }
        }
    }
}

/// Parses a [`LinkAddressed`] envelope from a vector of [`Item`]s. Returning a builder which
/// can be used for validation or an [`EnvelopeParseErr`] with a cause if any errors are
/// encountered.
fn parse_link_addressed(
    items: Vec<Item>,
    body: Option<Value>,
) -> Result<LinkAddressedBuilder, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(
        LinkAddressedBuilder {
            lane: LaneAddressedBuilder {
                node_uri: None,
                lane_uri: None,
                body,
            },
            rate: None,
            prio: None,
        },
        |mut link_addressed, (index, item)| {
            match item {
                Item::Slot(slot_key, slot_value) => {
                    if let Value::Text(slot_key_val) = slot_key {
                        match slot_key_val.as_str() {
                            "prio" => {
                                if let Value::Float64Value(slot_val) = slot_value {
                                    link_addressed.prio = Some(*slot_val);
                                    Ok(link_addressed)
                                } else {
                                    Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()))
                                }
                            }
                            "rate" => {
                                if let Value::Float64Value(slot_val) = slot_value {
                                    link_addressed.rate = Some(*slot_val);
                                    Ok(link_addressed)
                                } else {
                                    Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()))
                                }
                            }
                            _ => {
                                if let Value::Text(slot_val) = slot_value {
                                    parse_lane_addressed_value(
                                        slot_key_val,
                                        slot_val,
                                        &mut link_addressed.lane,
                                    )?;
                                    Ok(link_addressed)
                                } else {
                                    Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()))
                                }
                            }
                        }
                    } else {
                        Err(EnvelopeParseErr::UnexpectedType(slot_key.to_owned()))
                    }
                }
                // Lane/Node URI without a key
                Item::ValueItem(slot_value) => {
                    parse_lane_addressed_index(index, slot_value, &mut link_addressed.lane)?;
                    Ok(link_addressed)
                }
            }
        },
    )
}

/// Parses a [`LaneAddressed`] envelope from a vector of [`Item`]s. Returning a builder which
/// can be used for validation or an [`EnvelopeParseErr`] with a cause if any errors are
/// encountered. Both the `node_uri` and `lane_uri` [`Item`]s should be provided.
fn parse_lane_addressed(
    items: Vec<Item>,
    body: Option<Value>,
) -> Result<LaneAddressedBuilder, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(
        LaneAddressedBuilder {
            node_uri: None,
            lane_uri: None,
            body,
        },
        |mut lane_addressed, (index, item)| match item {
            Item::Slot(Value::Text(slot_key), Value::Text(slot_value)) => {
                parse_lane_addressed_value(slot_key, slot_value, &mut lane_addressed)?;
                Ok(lane_addressed)
            }
            Item::ValueItem(slot_value) => {
                parse_lane_addressed_index(index, slot_value, &mut lane_addressed)?;
                Ok(lane_addressed)
            }
            _ => Err(EnvelopeParseErr::UnexpectedItem(item.to_owned())),
        },
    )
}

fn parse_lane_addressed_value(
    key: &str,
    val: &str,
    lane_addressed: &mut LaneAddressedBuilder,
) -> Result<(), EnvelopeParseErr> {
    if key == "node" {
        match lane_addressed.node_uri {
            Some(_) => Err(EnvelopeParseErr::DuplicateHeader(String::from("node"))),
            None => {
                lane_addressed.node_uri = Some(val.deref().to_string());
                Ok(())
            }
        }
    } else if key == "lane" {
        match lane_addressed.lane_uri {
            Some(_) => Err(EnvelopeParseErr::DuplicateHeader(String::from("lane"))),
            None => {
                lane_addressed.lane_uri = Some(val.deref().to_string());
                Ok(())
            }
        }
    } else {
        Err(EnvelopeParseErr::UnexpectedKey(key.to_owned()))
    }
}

fn parse_lane_addressed_index(
    index: usize,
    value: &Value,
    lane_addressed: &mut LaneAddressedBuilder,
) -> Result<(), EnvelopeParseErr> {
    if index == 0 {
        parse_lane_addressed_value("node", &value.to_string(), lane_addressed)
    } else if index == 1 {
        parse_lane_addressed_value("lane", &value.to_string(), lane_addressed)
    } else {
        Err(EnvelopeParseErr::Malformatted)
    }
}

fn to_linked_addressed<F>(
    value: Value,
    body: Option<Value>,
    func: F,
) -> Result<Envelope, EnvelopeParseErr>
where
    F: Fn(LinkAddressed) -> Envelope,
{
    match value {
        Value::Record(_, headers) => {
            let link_addressed = parse_link_addressed(headers, body)?.build()?;
            Ok(func(link_addressed))
        }
        v => Err(EnvelopeParseErr::UnexpectedType(v)),
    }
}

fn to_lane_addressed<F>(
    value: Value,
    body: Option<Value>,
    func: F,
) -> Result<Envelope, EnvelopeParseErr>
where
    F: Fn(LaneAddressed) -> Envelope,
{
    match value {
        Value::Record(_, headers) => {
            let lane_builder = parse_lane_addressed(headers, body)?.build()?;
            Ok(func(lane_builder))
        }
        v => Err(EnvelopeParseErr::UnexpectedType(v)),
    }
}

/// Attempt to parse a ['Value'] in to an ['Envelope']. Returning either the parsed [`Envelope`] or
/// an [`EnvelopeParseErr`] detailing the failure cause.
///
/// # Examples
/// ```
/// use std::convert::TryFrom;
/// use model::{Value, Item, Attr};
/// use warp::envelope::{Envelope, LaneAddressed};
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
/// assert_eq!(envelope, Envelope::CommandMessage(LaneAddressed {
///        node_uri: String::from("node_uri"),
///        lane_uri: String::from("lane_uri"),
///        body: None,
/// }));
///
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

        let envelope_type = match attrs.pop() {
            Some(v) => v,
            None => return Err(EnvelopeParseErr::Malformatted),
        };

        match envelope_type.name.as_str() {
            "event" => to_lane_addressed(envelope_type.value, body, Envelope::EventMessage),
            "command" => to_lane_addressed(envelope_type.value, body, Envelope::CommandMessage),
            "link" => to_linked_addressed(envelope_type.value, body, Envelope::LinkRequest),
            "linked" => to_linked_addressed(envelope_type.value, body, Envelope::LinkedResponse),
            "sync" => to_linked_addressed(envelope_type.value, body, Envelope::SyncRequest),
            "synced" => to_lane_addressed(envelope_type.value, body, Envelope::SyncedResponse),
            "unlink" => to_lane_addressed(envelope_type.value, body, Envelope::UnlinkRequest),
            "unlinked" => {
                to_lane_addressed(envelope_type.value, body, { Envelope::UnlinkedResponse })
            }
            "auth" => Ok(Envelope::AuthRequest(HostAddressed { body })),
            "authed" => Ok(Envelope::AuthedResponse(HostAddressed { body })),
            "deauth" => Ok(Envelope::DeauthRequest(HostAddressed { body })),
            "deauthed" => Ok(Envelope::DeauthedResponse(HostAddressed { body })),
            s => Err(EnvelopeParseErr::UnknownTag(String::from(s))),
        }
    }
}
