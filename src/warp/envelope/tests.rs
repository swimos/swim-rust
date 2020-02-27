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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use crate::model::{Attr, Item, Value};
use crate::model::Item::ValueItem;
use crate::model::Value::Float64Value;
use crate::warp::envelope::{Envelope, EnvelopeParseErr, HostAddressed, LaneAddressed, LinkAddressed};

fn run_test(record: Value, expected: Envelope) {
    let e = Envelope::try_from(record);

    match e {
        Ok(env) => assert_that!(expected, eq(env)),
        Err(e) => {
            println!("{:?}", e);
            panic!(e);
        }
    }
}

fn link_addressed_no_body() -> LinkAddressed {
    LinkAddressed {
        lane: lane_addressed_no_body(),
        prio: Some(0.5),
        rate: Some(1.0),
    }
}

fn link_addressed_test_record() -> LinkAddressed {
    LinkAddressed {
        lane: lane_addressed_test_record(),
        prio: Some(0.5),
        rate: Some(1.0),
    }
}

fn lane_addressed_no_body() -> LaneAddressed {
    LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        body: None,
    }
}

fn lane_addressed_test_record() -> LaneAddressed {
    LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        body: Some(Value::Record(
            vec![Attr { name: String::from("test"), value: Value::Extant }],
            Vec::new())),
    }
}

fn link_named_headers() -> Vec<Item> {
    vec![
        Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
        Item::Slot(Value::Text(String::from("lane")), Value::Text(String::from("lane_uri"))),
        Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
        Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
    ]
}

fn lane_named_headers() -> Vec<Item> {
    vec![
        Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
        Item::Slot(Value::Text(String::from("lane")), Value::Text(String::from("lane_uri"))),
    ]
}

fn lane_positional_headers() -> Vec<Item> {
    vec![
        Item::ValueItem(Value::Text(String::from("node_uri"))),
        Item::ValueItem(Value::Text(String::from("lane_uri"))),
    ]
}

fn link_positional_headers() -> Vec<Item> {
    vec![
        Item::ValueItem(Value::Text(String::from("node_uri"))),
        Item::ValueItem(Value::Text(String::from("lane_uri"))),
        Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
        Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
    ]
}

fn create_record(tag: &str, items: Vec<Item>) -> Value {
    Value::Record(
        vec![
            Attr::of((tag, Value::Record(
                Vec::new(),
                items,
            ))),
        ],
        Vec::new(),
    )
}

fn create_record_with_test(tag: &str, items: Vec<Item>) -> Value {
    Value::Record(
        vec![
            Attr::of((tag, Value::Record(
                Vec::new(),
                items,
            ))),
            Attr::of(("test", Value::Extant))
        ],
        Vec::new(),
    )
}

// "@sync(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)"
#[test]
fn parse_sync_with_named_headers() {
    let record = create_record("sync", link_named_headers());
    run_test(record, Envelope::SyncRequest(link_addressed_no_body()));
}

// @sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_sync_with_positional_headers() {
    let record = create_record("sync", link_positional_headers());
    run_test(record, Envelope::SyncRequest(link_addressed_no_body()));
}

// @sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_sync_with_body() {
    let record = create_record_with_test("sync", link_positional_headers());
    run_test(record, Envelope::SyncRequest(link_addressed_test_record()));
}

// @link(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_link_with_named_headers() {
    let record = create_record("link", link_named_headers());
    run_test(record, Envelope::LinkRequest(link_addressed_no_body()));
}

// @link(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_link_with_positional_headers() {
    let record = create_record("link", link_positional_headers());
    run_test(record, Envelope::LinkRequest(link_addressed_no_body()));
}

// @link(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_link_with_body() {
    let record = create_record_with_test("link", link_named_headers());
    run_test(record, Envelope::LinkRequest(link_addressed_test_record()));
}

// @linked(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_linked_with_named_headers() {
    let record = create_record("linked", link_named_headers());
    run_test(record, Envelope::LinkedResponse(link_addressed_no_body()));
}

// @linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_linked_with_positional_headers() {
    let record = create_record("linked", link_positional_headers());
    run_test(record, Envelope::LinkedResponse(link_addressed_no_body()));
}

// @linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_linked_with_body() {
    let record = create_record_with_test("linked", link_positional_headers());
    run_test(record, Envelope::LinkedResponse(link_addressed_test_record()));
}

// @auth
#[test]
fn parse_auth() {
    let record = Value::Record(
        vec![
            Attr::of(("auth", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::AuthRequest(HostAddressed {
                 body: None
             }),
    );
}

// @auth@test
#[test]
fn parse_auth_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("auth", Value::Extant)),
            Attr::of(("test", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::AuthRequest(HostAddressed {
                 body: Some(Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()))
             }),
    );
}

// @authed
#[test]
fn parse_authed() {
    let record = Value::Record(
        vec![
            Attr::of(("authed", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::AuthedResponse(HostAddressed {
                 body: None
             }),
    );
}

// @authed@test
#[test]
fn parse_authed_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("authed", Value::Extant)),
            Attr::of(("test", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::AuthedResponse(HostAddressed {
                 body: Some(Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()))
             }),
    );
}

// @command(node: node_uri, lane: lane_uri)
#[test]
fn parse_command_with_named_headers() {
    let record = create_record("command", lane_named_headers());
    run_test(record, Envelope::CommandMessage(lane_addressed_no_body()));
}

// @command(node_uri, lane_uri)
#[test]
fn parse_command_with_positional_headers() {
    let record = create_record("command", lane_positional_headers());
    run_test(record, Envelope::CommandMessage(lane_addressed_no_body()));
}

// @command(node_uri, lane_uri)@test
#[test]
fn parse_command_with_body() {
    let record = create_record_with_test("command", lane_positional_headers());
    run_test(record, Envelope::CommandMessage(lane_addressed_test_record()));
}

// @deauthed
#[test]
fn parse_deauthed() {
    let record = Value::Record(
        vec![
            Attr::of(("deauthed", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::DeauthedResponse(HostAddressed {
                 body: None
             }),
    );
}

// @deauthed@test
#[test]
fn parse_deauthed_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("deauthed", Value::Extant)),
            Attr::of(("test", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::DeauthedResponse(HostAddressed {
                 body: Some(Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()))
             }),
    );
}

// @deauth
#[test]
fn parse_deauth() {
    let record = Value::Record(
        vec![
            Attr::of(("deauth", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::DeauthRequest(HostAddressed {
                 body: None
             }),
    );
}

// @deauth@test
#[test]
fn parse_deauth_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("deauth", Value::Extant)),
            Attr::of(("test", Value::Extant))
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::DeauthRequest(HostAddressed {
                 body: Some(Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()))
             }),
    );
}

// @event(node: node_uri, lane: lane_uri)
#[test]
fn parse_event_with_named_headers() {
    let record = create_record("event", lane_named_headers());
    run_test(record, Envelope::EventMessage(lane_addressed_no_body()));
}

// @event(node_uri, lane_uri)
#[test]
fn parse_event_with_positional_headers() {
    let record = create_record("event", lane_positional_headers());
    run_test(record, Envelope::EventMessage(lane_addressed_no_body()));
}

// @event(node_uri, lane_uri)@test
#[test]
fn parse_event_with_body() {
    let record = create_record_with_test("event", lane_named_headers());
    run_test(record, Envelope::EventMessage(lane_addressed_test_record()));
}

// @synced(node: node_uri, lane: lane_uri)
#[test]
fn parse_synced_with_named_headers() {
    let record = create_record("synced", lane_named_headers());
    run_test(record, Envelope::SyncedResponse(lane_addressed_no_body()));
}

// @synced(node_uri, lane_uri)
#[test]
fn parse_synced_with_positional_headers() {
    let record = create_record("synced", lane_positional_headers());
    run_test(record, Envelope::SyncedResponse(lane_addressed_no_body()));
}

// @synced(node_uri, lane_uri)@test
#[test]
fn parse_synced_with_body() {
    let record = create_record_with_test("synced", lane_named_headers());
    run_test(record, Envelope::SyncedResponse(lane_addressed_test_record()));
}

// @unlink(node: node_uri, lane: lane_uri)
#[test]
fn parse_unlink_with_named_headers() {
    let record = create_record("unlink", lane_named_headers());
    run_test(record, Envelope::UnlinkRequest(lane_addressed_no_body()));
}

// @unlink(node_uri, lane_uri)
#[test]
fn parse_unlink_with_positional_headers() {
    let record = create_record("unlink", lane_positional_headers());
    run_test(record, Envelope::UnlinkRequest(lane_addressed_no_body()));
}

// @unlink(node_uri, lane_uri)@test
#[test]
fn parse_unlink_with_body() {
    let record = create_record_with_test("unlink", lane_named_headers());
    run_test(record, Envelope::UnlinkRequest(lane_addressed_test_record()));
}

// @unlinked(node: node_uri, lane: lane_uri)
#[test]
fn parse_unlinked_with_named_headers() {
    let record = create_record("unlinked", lane_named_headers());
    run_test(record, Envelope::UnlinkedResponse(lane_addressed_no_body()));
}

// @unlinked(node_uri, lane_uri)
#[test]
fn parse_unlinked_with_positional_headers() {
    let record = create_record("unlinked", lane_positional_headers());
    run_test(record, Envelope::UnlinkedResponse(lane_addressed_no_body()));
}

// @unlinked(node_uri, lane_uri)@test
#[test]
fn parse_unlinked_with_body() {
    let record = create_record_with_test("unlinked", lane_named_headers());
    run_test(record, Envelope::UnlinkedResponse(lane_addressed_test_record()));
}


#[test]
fn unknown_tag() {
    let tag = "unknown_tag";
    let record = create_record_with_test(tag, lane_named_headers());

    run_test_expect_err(record, EnvelopeParseErr::UnknownTag(String::from(tag)));
}

fn run_test_expect_err(record: Value, expected: EnvelopeParseErr) {
    let e = Envelope::try_from(record);

    match e {
        Ok(r) => panic!("Expected envelope to not parse: {:?}", r),
        Err(e) => {
            assert_eq!(e, expected);
        }
    }
}

#[test]
fn unexpected_key() {
    let record = Value::Record(
        vec![
            Attr::of(("unlinked", Value::Record(
                Vec::new(),
                vec![
                    Item::Slot(Value::Text(String::from("not_a_node")), Value::Text(String::from("node_uri"))),
                    Item::Slot(Value::Text(String::from("node_a_lane")), Value::Text(String::from("lane_uri"))),
                ],
            ))),
        ],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedKey(String::from("not_a_node")));
}

#[test]
fn unexpected_type() {
    let slot = Item::Slot(Value::Float64Value(1.0), Value::Float64Value(1.0));
    let record = Value::Record(
        vec![
            Attr::of(("unlinked", Value::Record(
                Vec::new(),
                vec![
                    slot.clone(),
                ],
            ))),
        ],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedItem(slot));
}

#[test]
fn too_many_named_headers() {
    let record = create_record("sync", vec![
        Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
        Item::Slot(Value::Text(String::from("lane")), Value::Text(String::from("lane_uri"))),
        Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
        Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
        Item::Slot(Value::Text(String::from("host")), Value::Text(String::from("swim.ai"))),
    ]);

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedKey(String::from("host")));
}

#[test]
fn too_many_positional_headers() {
    let record = create_record("sync", vec![
        Item::ValueItem(Value::Text(String::from("node_uri"))),
        Item::ValueItem(Value::Text(String::from("lane_uri"))),
        Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
        Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
        Item::ValueItem(Value::Text(String::from("swim.ai"))),
    ]);

    run_test_expect_err(record, EnvelopeParseErr::Malformatted);
}

#[test]
fn mixed_headers() {
    let record = create_record("sync", vec![
        Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
        Item::ValueItem(Value::Text(String::from("lane_uri"))),
        Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
        Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
    ]);

    run_test(record, Envelope::SyncRequest(link_addressed_no_body()));
}

#[test]
fn parse_body_multiple_attributes() {
    let record = Value::Record(
        vec![
            Attr::of(("auth", Value::Extant)),
            Attr::of(("first", Value::Extant)),
            Attr::of(("second", Value::Extant)),
            Attr::of(("third", Value::Extant)),
        ],
        Vec::new(),
    );

    run_test(record,
             Envelope::AuthRequest(HostAddressed {
                 body: Some(Value::Record(
                     vec![
                         Attr { name: String::from("first"), value: Value::Extant },
                         Attr { name: String::from("second"), value: Value::Extant },
                         Attr { name: String::from("third"), value: Value::Extant },
                     ],
                     Vec::new()))
             }),
    );
}

#[test]
fn duplicate_headers() {
    let record = Value::Record(
        vec![
            Attr::of(("sync", Value::Record(
                Vec::new(),
                vec![
                    Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
                    Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
                ],
            ))),
        ],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::DuplicateHeader(String::from("node")));
}

#[test]
fn missing_header() {
    let record = Value::Record(
        vec![
            Attr::of(("synced", Value::Record(
                Vec::new(),
                vec![
                    Item::Slot(Value::Text(String::from("node")), Value::Text(String::from("node_uri"))),
                ],
            ))),
        ],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::MissingHeader(String::from("lane")));
}

#[test]
fn multiple_attributes() {
    let record = Value::Record(
        vec![
            Attr::of(("sync", Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::Text(String::from("node_uri"))),
                    Item::ValueItem(Value::Text(String::from("lane_uri"))),
                    Item::Slot(Value::Text(String::from("prio")), Value::Float64Value(0.5)),
                    Item::Slot(Value::Text(String::from("rate")), Value::Float64Value(1.0)),
                ],
            ))),
        ],
        vec![
            ValueItem(Value::Float64Value(1.0)),
        ],
    );

    run_test(record,
             Envelope::SyncRequest(LinkAddressed {
                 lane: LaneAddressed {
                     node_uri: String::from("node_uri"),
                     lane_uri: String::from("lane_uri"),
                     body: Some(Float64Value(1.0)),
                 },
                 rate: Some(1.0),
                 prio: Some(0.5),
             }),
    );
}

#[test]
fn tag() {
    let record = Value::Record(
        vec![
            Attr::of(("auth", Value::Extant))
        ],
        Vec::new(),
    );

    let e = Envelope::try_from(record).unwrap();
    assert_eq!(e.tag(), "auth");
}
