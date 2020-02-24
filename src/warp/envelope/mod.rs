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

use crate::model::{Item, Value};

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq)]
pub enum Envelope {
    EventMessage(LaneAddressed),
    CommandMessage(LaneAddressed),
    LinkRequest(LinkAddressed),
    LinkedResponse(LinkAddressed),
    SyncRequest(LinkAddressed),
    SyncedResponse(LaneAddressed),
    UnlinkRequest(LaneAddressed),
    UnlinkedResponse(LaneAddressed),
    AuthRequest(HostAddressed),
    AuthedResponse(HostAddressed),
    DeauthRequest(HostAddressed),
    DeauthedResponse(HostAddressed),
}

#[derive(Debug, PartialEq)]
struct LaneAddressedBuilder {
    node_uri: Option<String>,
    lane_uri: Option<String>,
    body: Option<Value>,
}

impl LaneAddressedBuilder {
    fn build(self) -> Result<LaneAddressed, EnvelopeParseErr> {
        match self {
            LaneAddressedBuilder {
                node_uri: Some(node_uri),
                lane_uri: Some(lane_uri),
                body
            } => {
                Ok(LaneAddressed {
                    node_uri,
                    lane_uri,
                    body,
                })
            }
            LaneAddressedBuilder {
                node_uri: None, ..
            } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("node")))
            }
            LaneAddressedBuilder {
                lane_uri: None, ..
            } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("lane")))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct LinkAddressedBuilder {
    lane: LaneAddressedBuilder,
    rate: Option<f64>,
    prio: Option<f64>,
}

impl LinkAddressedBuilder {
    fn build(self) -> Result<LinkAddressed, EnvelopeParseErr> {
        match self {
            LinkAddressedBuilder {
                rate: Some(r),
                prio: Some(p),
                lane
            } => {
                match lane.build() {
                    Ok(lane) => {
                        Ok(LinkAddressed {
                            lane,
                            rate: r,
                            prio: p,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            LinkAddressedBuilder {
                rate: None,
                ..
            } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("rate")))
            }
            LinkAddressedBuilder {
                prio: None,
                ..
            } => {
                Err(EnvelopeParseErr::MissingHeader(String::from("prio")))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct HostAddressed {
    pub body: Option<Value>,
}

#[derive(Debug, PartialEq)]
pub struct LaneAddressed {
    pub node_uri: String,
    pub lane_uri: String,
    pub body: Option<Value>,
}

#[derive(Debug, PartialEq)]
pub struct LinkAddressed {
    pub lane: LaneAddressed,
    pub rate: f64,
    pub prio: f64,
}

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

fn parse_link_addressed(items: Vec<Item>, body: Option<Value>) -> Result<LinkAddressedBuilder, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(LinkAddressedBuilder {
        lane: LaneAddressedBuilder {
            node_uri: None,
            lane_uri: None,
            body,
        },
        rate: None,
        prio: None,
    }, |mut link_addressed, (index, item)| {
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
                                if let Err(e) = parse_lane_addressed_value(slot_key_val, slot_val, &mut link_addressed.lane) {
                                    Err(e)
                                } else {
                                    Ok(link_addressed)
                                }
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
                if let Err(e) = parse_lane_addressed_index(index, slot_value, &mut link_addressed.lane) {
                    Err(e)
                } else {
                    Ok(link_addressed)
                }
            }
        }
    })
}

fn parse_lane_addressed(items: Vec<Item>, body: Option<Value>) -> Result<LaneAddressedBuilder, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(LaneAddressedBuilder {
        node_uri: None,
        lane_uri: None,
        body,
    }, |mut lane_addressed, (index, item)| {
        match item {
            Item::Slot(slot_key, slot_value) => {
                if let Value::Text(key) = slot_key {
                    if let Value::Text(slot_val) = slot_value {
                        if let Err(e) = parse_lane_addressed_value(key, slot_val, &mut lane_addressed) {
                            Err(e)
                        } else {
                            Ok(lane_addressed)
                        }
                    } else {
                        Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()))
                    }
                } else {
                    Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()))
                }
            }
            Item::ValueItem(slot_value) => {
                if let Err(e) = parse_lane_addressed_index(index, slot_value, &mut lane_addressed) {
                    Err(e)
                } else {
                    Ok(lane_addressed)
                }
            }
        }
    })
}

fn parse_lane_addressed_value<'a>(key: &str, val: &String, lane_addressed: &'a mut LaneAddressedBuilder)
                                  -> Result<&'a LaneAddressedBuilder, EnvelopeParseErr> {
    if key == "node" {
        match lane_addressed.node_uri {
            Some(_) => Err(EnvelopeParseErr::DuplicateHeader(String::from("node"))),
            None => {
                lane_addressed.node_uri = Some(val.deref().to_string());
                Ok(lane_addressed)
            }
        }
    } else if key == "lane" {
        match lane_addressed.lane_uri {
            Some(_) => Err(EnvelopeParseErr::DuplicateHeader(String::from("lane"))),
            None => {
                lane_addressed.lane_uri = Some(val.deref().to_string());
                Ok(lane_addressed)
            }
        }
    } else {
        Err(EnvelopeParseErr::UnexpectedKey(key.to_owned()))
    }
}

fn parse_lane_addressed_index<'a>(index: usize, value: &Value, lane_addressed: &'a mut LaneAddressedBuilder)
                                  -> Result<&'a LaneAddressedBuilder, EnvelopeParseErr> {
    if index == 0 {
        parse_lane_addressed_value("node", &value.to_string(), lane_addressed)
    } else if index == 1 {
        parse_lane_addressed_value("lane", &value.to_string(), lane_addressed)
    } else {
        Err(EnvelopeParseErr::Malformatted)
    }
}

fn to_linked_addressed<F>(value: Value, body: Option<Value>, func: F) -> Result<Envelope, EnvelopeParseErr>
    where F: Fn(LinkAddressed) -> Envelope {
    match value {
        Value::Record(_, headers) => {
            return match parse_link_addressed(headers, body) {
                Ok(link_builder) => {
                    match link_builder.build() {
                        Ok(la) => Ok(func(la)),
                        Err(e) => Err(e)
                    }
                }
                Err(e) => Err(e)
            };
        }
        v @ _ => {
            Err(EnvelopeParseErr::UnexpectedType(v))
        }
    }
}

fn to_lane_addressed<F>(value: Value, body: Option<Value>, func: F) -> Result<Envelope, EnvelopeParseErr>
    where F: Fn(LaneAddressed) -> Envelope {
    match value {
        Value::Record(_, headers) => {
            return match parse_lane_addressed(headers, body) {
                Ok(lane_builder) => {
                    match lane_builder.build() {
                        Ok(la) => Ok(func(la)),
                        Err(e) => Err(e)
                    }
                }
                Err(e) => Err(e)
            };
        }
        v @ _ => {
            Err(EnvelopeParseErr::UnexpectedType(v))
        }
    }
}

impl TryFrom<Value> for Envelope {
    type Error = EnvelopeParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let (mut attrs, mut body) = match value {
            Value::Record(a, i) => (a, i),
            v @ _ => {
                return Err(EnvelopeParseErr::UnexpectedType(v));
            }
        };

        let body = {
            if attrs.len() > 1 {
                Some(Value::Record(attrs.drain(1..).collect(), body))
            } else if body.len() == 1 {
                match body.pop() {
                    Some(item) => {
                        match item {
                            Item::ValueItem(inner) => Some(inner),
                            i @ _ => return Err(EnvelopeParseErr::UnexpectedItem(i))
                        }
                    }
                    None => None
                }
            } else {
                None
            }
        };

        let envelope_type = match attrs.pop() {
            Some(v) => v,
            None => return Err(EnvelopeParseErr::Malformatted)
        };

        return match envelope_type.name.as_str() {
            "event" => {
                to_lane_addressed(envelope_type.value, body, |la| {
                    Envelope::EventMessage(la)
                })
            }
            "command" => {
                to_lane_addressed(envelope_type.value, body, |la| {
                    Envelope::CommandMessage(la)
                })
            }
            "link" => {
                to_linked_addressed(envelope_type.value, body, |la| {
                    Envelope::LinkRequest(la)
                })
            }
            "linked" => {
                to_linked_addressed(envelope_type.value, body, |la| {
                    Envelope::LinkedResponse(la)
                })
            }
            "sync" => {
                to_linked_addressed(envelope_type.value, body, |la| {
                    Envelope::SyncRequest(la)
                })
            }
            "synced" => {
                to_lane_addressed(envelope_type.value, body, |la| {
                    Envelope::SyncedResponse(la)
                })
            }
            "unlink" => {
                to_lane_addressed(envelope_type.value, body, |la| {
                    Envelope::UnlinkRequest(la)
                })
            }
            "unlinked" => {
                to_lane_addressed(envelope_type.value, body, |la| {
                    Envelope::UnlinkedResponse(la)
                })
            }
            "auth" => {
                Ok(Envelope::AuthRequest(HostAddressed { body }))
            }
            "authed" => {
                Ok(Envelope::AuthedResponse(HostAddressed { body }))
            }
            "deauth" => {
                Ok(Envelope::DeauthRequest(HostAddressed { body }))
            }
            "deauthed" => {
                Ok(Envelope::DeauthedResponse(HostAddressed { body }))
            }
            s @ _ => {
                Err(EnvelopeParseErr::UnknownTag(String::from(s)))
            }
        };
    }
}
