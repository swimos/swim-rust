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

use crate::model::{Attr, Item, Value};

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
pub struct HostAddressed {
    pub body: Option<Value>,
}

#[derive(Debug, PartialEq)]
pub struct LaneAddressed {
    pub node_uri: Option<String>,
    pub lane_uri: Option<String>,
    pub body: Option<Value>,
}

#[derive(Debug, PartialEq)]
pub struct LinkAddressed {
    pub lane: LaneAddressed,
    pub rate: Option<f64>,
    pub prio: Option<f64>,
}

#[derive(Debug, PartialEq)]
pub enum EnvelopeParseErr {
    UnexpectedKey(String),
    UnexpectedType(Value),
    UnexpectedItem(Item),
    Malformatted,
    DuplicateTag(String),
    UnknownTag(String),
}

fn parse_link_addressed(items: Vec<Item>, body: Option<Value>) -> Result<LinkAddressed, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(LinkAddressed {
        lane: LaneAddressed {
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

fn parse_lane_addressed(items: Vec<Item>, body: Option<Value>) -> Result<LaneAddressed, EnvelopeParseErr> {
    items.iter().enumerate().try_fold(LaneAddressed {
        node_uri: None,
        lane_uri: None,
        body,
    }, |mut lane_addressed, (index, item)| {
        match item {
            Item::Slot(slot_key, slot_value) => {
                if let Value::Text(slot_key_val) = slot_key {
                    if let Value::Text(slot_val) = slot_value {
                        if let Err(e) = parse_lane_addressed_value(slot_key_val, slot_val, &mut lane_addressed) {
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

//noinspection DuplicatedCode
fn parse_lane_addressed_value<'a>(key: &str, val: &String, lane_addressed: &'a mut LaneAddressed)
                                  -> Result<&'a LaneAddressed, EnvelopeParseErr> {
    if key == "node" {
        if lane_addressed.node_uri.is_some() {
            Err(EnvelopeParseErr::DuplicateTag(String::from("node")))
        } else {
            lane_addressed.node_uri = Some(val.deref().to_string());
            Ok(lane_addressed)
        }
    } else if key == "lane" {
        if lane_addressed.lane_uri.is_some() {
            Err(EnvelopeParseErr::DuplicateTag(String::from("lane")))
        } else {
            lane_addressed.lane_uri = Some(val.deref().to_string());
            Ok(lane_addressed)
        }
    } else {
        Err(EnvelopeParseErr::UnexpectedKey(key.to_owned()))
    }
}

//noinspection DuplicatedCode
fn parse_lane_addressed_index<'a>(index: usize, value: &Value, lane_addressed: &'a mut LaneAddressed)
                                  -> Result<&'a LaneAddressed, EnvelopeParseErr> {
    if index == 0 {
        if lane_addressed.node_uri.is_some() {
            Err(EnvelopeParseErr::DuplicateTag(String::from("node")))
        } else {
            lane_addressed.node_uri = Some(value.deref().to_string());
            Ok(lane_addressed)
        }
    } else if index == 1 {
        if lane_addressed.lane_uri.is_some() {
            Err(EnvelopeParseErr::DuplicateTag(String::from("lane")))
        } else {
            lane_addressed.lane_uri = Some(value.deref().to_string());
            Ok(lane_addressed)
        }
    } else {
        Err(EnvelopeParseErr::Malformatted)
    }
}

fn dispatch_linked_addressed<F>(envelope_type: Attr, body: Option<Value>, func: F) -> Result<Envelope, EnvelopeParseErr>
    where F: Fn(LinkAddressed) -> Envelope {
    match envelope_type.value {
        Value::Record(_, headers) => {
            return match parse_link_addressed(headers, body) {
                Ok(la) => Ok(func(la)),
                Err(e) => Err(e)
            };
        }
        v @ _ => {
            Err(EnvelopeParseErr::UnexpectedType(v))
        }
    }
}

fn dispatch_lane_addressed<F>(envelope_type: Attr, body: Option<Value>, func: F) -> Result<Envelope, EnvelopeParseErr>
    where F: Fn(LaneAddressed) -> Envelope {
    match envelope_type.value {
        Value::Record(_, headers) => {
            return match parse_lane_addressed(headers, body) {
                Ok(la) => Ok(func(la)),
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
                let single = body.pop().unwrap();
                match single {
                    Item::ValueItem(inner) => Some(inner),
                    i @ _ => return Err(EnvelopeParseErr::UnexpectedItem(i))
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
                dispatch_lane_addressed(envelope_type, body, |la| {
                    Envelope::EventMessage(la)
                })
            }
            "command" => {
                dispatch_lane_addressed(envelope_type, body, |la| {
                    Envelope::CommandMessage(la)
                })
            }
            "link" => {
                dispatch_linked_addressed(envelope_type, body, |la| {
                    Envelope::LinkRequest(la)
                })
            }
            "linked" => {
                dispatch_linked_addressed(envelope_type, body, |la| {
                    Envelope::LinkedResponse(la)
                })
            }
            "sync" => {
                dispatch_linked_addressed(envelope_type, body, |la| {
                    Envelope::SyncRequest(la)
                })
            }
            "synced" => {
                dispatch_lane_addressed(envelope_type, body, |la| {
                    Envelope::SyncedResponse(la)
                })
            }
            "unlink" => {
                dispatch_lane_addressed(envelope_type, body, |la| {
                    Envelope::UnlinkRequest(la)
                })
            }
            "unlinked" => {
                dispatch_lane_addressed(envelope_type, body, |la| {
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
