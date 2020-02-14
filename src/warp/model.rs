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

use std::convert::{TryFrom, TryInto};
use std::ops::Deref;

use crate::model::{Attr, Item, Value};

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum Envelope {
    EventMessage,
    CommandMessage,
    LinkRequest(LinkAddressed),
    LinkedResponse(LinkAddressed),
    SyncRequest(LinkAddressed),
    SyncedResponse,
    UnlinkRequest,
    UnlinkedResponse,
    AuthRequest,
    AuthedResponse,
    DeauthRequest,
    DeauthedResponse,
}

#[derive(Debug, PartialEq)]
pub struct LaneAddressed(String, String);

#[derive(Debug, PartialEq)]
pub struct LinkAddressed {
    pub node_uri: String,
    pub lane_uri: String,
    pub prio: f64,
    pub rate: f64,
    pub body: Value,
}

#[derive(Debug, PartialEq)]
pub enum EnvelopeParseErr {
    UnexpectedKey(String),
    UnexpectedType(Value),
    Malformatted,
}

fn parse_link_addressed(items: Vec<Item>, body: Value) -> Result<LinkAddressed, EnvelopeParseErr> {
    let mut la = LinkAddressed {
        node_uri: String::new(),
        lane_uri: String::new(),
        rate: 0.0,
        prio: 0.0,
        body,
    };

    let mut index = 0;

    for item in items.iter() {
        match item {
            Item::Slot(slot_key, slot_value) => {
                if let Value::Text(slot_key_val) = slot_key {
                    match slot_key_val.as_str() {
                        "prio" => {
                            if let Value::Float64Value(slot_val) = slot_value {
                                la.prio = *slot_val;
                            } else {
                                return Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()));
                            }
                        }
                        "rate" => {
                            if let Value::Float64Value(slot_val) = slot_value {
                                la.rate = *slot_val;
                            } else {
                                return Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()));
                            }
                        }
                        _ => {
                            if let Value::Text(slot_val) = slot_value {
                                if slot_key_val == "node" {
                                    la.node_uri = slot_val.deref().to_string();
                                } else if slot_key_val == "lane" {
                                    la.lane_uri = slot_val.deref().to_string();
                                } else {
                                    return Err(EnvelopeParseErr::UnexpectedKey(slot_key_val.to_owned()));
                                }
                            } else {
                                return Err(EnvelopeParseErr::UnexpectedType(slot_value.to_owned()));
                            }
                        }
                    }
                }
            }
            // Lane/Node URI without a key
            Item::ValueItem(slot_value) => {
                if index == 0 {
                    la.node_uri = slot_value.deref().to_string();
                } else if index == 1 {
                    la.lane_uri = slot_value.deref().to_string();
                } else {
                    return Err(EnvelopeParseErr::Malformatted);
                }
            }
        }

        index += 1;
    }
    Ok(la)
}

// Cast equivalent
impl TryFrom<Value> for Envelope {
    type Error = EnvelopeParseErr;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let (mut vec, body) = match value {
            Value::Record(a, i) => (a, i),
            t @ _ => {
                return Err(EnvelopeParseErr::UnexpectedType(t));
            }
        };

        if vec.len() > 2 {
            return Err(EnvelopeParseErr::Malformatted);
        }

        vec.reverse();

        let envelope_type = vec.pop().unwrap();
        let attributes = {
            if let Some(v) = vec.pop() {
                vec![v]
            } else {
                Vec::new()
            }
        };

        let rec = {
            if attributes.len() == 0 && body.len() == 0 {
                Value::Extant
            } else {
                Value::Record(attributes, body)
            }
        };

        return match envelope_type.name.as_str() {
            "event" => Ok(Envelope::EventMessage),
            "command" => Ok(Envelope::CommandMessage),
            "link" => {
                match envelope_type.value {
                    Value::Record(_, headers) => {
                        return match parse_link_addressed(headers, rec) {
                            Ok(la) => Ok(Envelope::LinkRequest(la)),
                            Err(e) => Err(e)
                        };
                    }
                    u @ _ => {
                        Err(EnvelopeParseErr::UnexpectedType(u))
                    }
                }
            }
            "linked" => {
                match envelope_type.value {
                    Value::Record(_, headers) => {
                        return match parse_link_addressed(headers, rec) {
                            Ok(la) => Ok(Envelope::LinkedResponse(la)),
                            Err(e) => Err(e)
                        };
                    }
                    u @ _ => {
                        Err(EnvelopeParseErr::UnexpectedType(u))
                    }
                }
            }
            "sync" => {
                match envelope_type.value {
                    Value::Record(_, headers) => {
                        return match parse_link_addressed(headers, rec) {
                            Ok(la) => Ok(Envelope::SyncRequest(la)),
                            Err(e) => Err(e)
                        };
                    }
                    u @ _ => {
                        Err(EnvelopeParseErr::UnexpectedType(u))
                    }
                }
            }
            "synced" => Ok(Envelope::SyncedResponse),
            "unlink" => Ok(Envelope::UnlinkRequest),
            "unlinked" => Ok(Envelope::UnlinkedResponse),
            "auth" => Ok(Envelope::AuthRequest),
            "authed" => Ok(Envelope::AuthedResponse),
            "deauth" => Ok(Envelope::DeauthRequest),
            "deauthed" => Ok(Envelope::DeauthedResponse),
            _ => {
                panic!()
            }
        };
    }
}

// Mold equivalent
impl TryInto<Attr> for Envelope {
    type Error = EnvelopeParseErr;

    fn try_into(self) -> Result<Attr, Self::Error> {
        unimplemented!()
    }
}