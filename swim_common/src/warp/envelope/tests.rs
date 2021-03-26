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

use std::convert::TryFrom;

use crate::model::Item::ValueItem;
use crate::model::{Attr, Item, Value};
use crate::warp::envelope::{
    Envelope, EnvelopeParseErr, AUTHED_TAG, AUTH_TAG, CMD_TAG, DEAUTHED_TAG, DEAUTH_TAG, EVENT_TAG,
    LINKED_TAG, LINK_TAG, SYNCED_TAG, SYNC_TAG, UNLINKED_TAG, UNLINK_TAG,
};

fn run_test(record: Value, expected: Envelope) {
    let e = Envelope::try_from(record);

    match e {
        Ok(env) => assert_eq!(expected, env),
        Err(e) => {
            panic!("{}", e);
        }
    }
}

const TEST_PRIO: f64 = 0.5;
const TEST_RATE: f64 = 1.0;
const TEST_NODE: &str = "node_uri";
const TEST_LANE: &str = "lane_uri";
const TEST_TAG: &str = "test";

fn test_body() -> Value {
    Value::of_attr(Attr::of(TEST_TAG))
}

fn link_named_headers() -> Vec<Item> {
    vec![
        Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
        Item::Slot(Value::text("lane"), Value::text(TEST_LANE)),
        Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
        Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
    ]
}

fn lane_named_headers() -> Vec<Item> {
    vec![
        Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
        Item::Slot(
            Value::text(String::from("lane")),
            Value::text(String::from(TEST_LANE)),
        ),
    ]
}

fn lane_positional_headers() -> Vec<Item> {
    vec![
        Item::ValueItem(Value::text(TEST_NODE)),
        Item::ValueItem(Value::text(TEST_LANE)),
    ]
}

fn link_positional_headers() -> Vec<Item> {
    vec![
        Item::ValueItem(Value::text(TEST_NODE)),
        Item::ValueItem(Value::text(TEST_LANE)),
        Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
        Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
    ]
}

fn create_record(tag: &str, items: Vec<Item>) -> Value {
    Value::Record(
        vec![Attr::of((tag, Value::Record(Vec::new(), items)))],
        Vec::new(),
    )
}

fn create_record_with_test(tag: &str, items: Vec<Item>) -> Value {
    Value::Record(
        vec![
            Attr::of((tag, Value::Record(Vec::new(), items))),
            Attr::of((TEST_TAG, Value::Extant)),
        ],
        Vec::new(),
    )
}

// "@sync(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)"
#[test]
fn parse_sync_with_named_headers() {
    let record = create_record("sync", link_named_headers());
    run_test(
        record,
        Envelope::make_sync(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_sync_with_positional_headers() {
    let record = create_record("sync", link_positional_headers());
    run_test(
        record,
        Envelope::make_sync(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_sync_with_body() {
    let record = create_record_with_test("sync", link_positional_headers());
    run_test(
        record,
        Envelope::make_sync(
            TEST_NODE,
            TEST_LANE,
            Some(TEST_RATE),
            Some(TEST_PRIO),
            Some(test_body()),
        ),
    );
}

// @link(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_link_with_named_headers() {
    let record = create_record("link", link_named_headers());
    run_test(
        record,
        Envelope::make_link(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @link(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_link_with_positional_headers() {
    let record = create_record("link", link_positional_headers());
    run_test(
        record,
        Envelope::make_link(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @link(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_link_with_body() {
    let record = create_record_with_test("link", link_named_headers());
    run_test(
        record,
        Envelope::make_link(
            TEST_NODE,
            TEST_LANE,
            Some(TEST_RATE),
            Some(TEST_PRIO),
            Some(test_body()),
        ),
    );
}

// @linked(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_linked_with_named_headers() {
    let record = create_record("linked", link_named_headers());
    run_test(
        record,
        Envelope::make_linked(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)
#[test]
fn parse_linked_with_positional_headers() {
    let record = create_record("linked", link_positional_headers());
    run_test(
        record,
        Envelope::make_linked(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
}

// @linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test
#[test]
fn parse_linked_with_body() {
    let record = create_record_with_test("linked", link_positional_headers());
    run_test(
        record,
        Envelope::make_linked(
            TEST_NODE,
            TEST_LANE,
            Some(TEST_RATE),
            Some(TEST_PRIO),
            Some(test_body()),
        ),
    );
}

// @auth
#[test]
fn parse_auth() {
    let record = Value::Record(vec![Attr::of(("auth", Value::Extant))], Vec::new());

    run_test(record, Envelope::make_auth(None));
}

// @auth@test
#[test]
fn parse_auth_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("auth", Value::Extant)),
            Attr::of((TEST_TAG, Value::Extant)),
        ],
        Vec::new(),
    );

    run_test(record, Envelope::make_auth(Some(test_body())));
}

// @authed
#[test]
fn parse_authed() {
    let record = Value::Record(vec![Attr::of(("authed", Value::Extant))], Vec::new());

    run_test(record, Envelope::make_authed(None));
}

// @authed@test
#[test]
fn parse_authed_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("authed", Value::Extant)),
            Attr::of((TEST_TAG, Value::Extant)),
        ],
        Vec::new(),
    );

    run_test(record, Envelope::make_authed(Some(test_body())));
}

// @command(node: node_uri, lane: lane_uri)
#[test]
fn parse_command_with_named_headers() {
    let record = create_record("command", lane_named_headers());
    run_test(record, Envelope::make_command(TEST_NODE, TEST_LANE, None));
}

// @command(node_uri, lane_uri)
#[test]
fn parse_command_with_positional_headers() {
    let record = create_record("command", lane_positional_headers());
    run_test(record, Envelope::make_command(TEST_NODE, TEST_LANE, None));
}

// @command(node_uri, lane_uri)@test
#[test]
fn parse_command_with_body() {
    let record = create_record_with_test("command", lane_positional_headers());
    run_test(
        record,
        Envelope::make_command(TEST_NODE, TEST_LANE, Some(test_body())),
    );
}

// @deauthed
#[test]
fn parse_deauthed() {
    let record = Value::Record(vec![Attr::of(("deauthed", Value::Extant))], Vec::new());

    run_test(record, Envelope::make_deauthed(None));
}

// @deauthed@test
#[test]
fn parse_deauthed_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("deauthed", Value::Extant)),
            Attr::of((TEST_TAG, Value::Extant)),
        ],
        Vec::new(),
    );

    run_test(record, Envelope::make_deauthed(Some(test_body())));
}

// @deauth
#[test]
fn parse_deauth() {
    let record = Value::Record(vec![Attr::of(("deauth", Value::Extant))], Vec::new());

    run_test(record, Envelope::make_deauth(None));
}

// @deauth@test
#[test]
fn parse_deauth_with_body() {
    let record = Value::Record(
        vec![
            Attr::of(("deauth", Value::Extant)),
            Attr::of((TEST_TAG, Value::Extant)),
        ],
        Vec::new(),
    );

    run_test(record, Envelope::make_deauth(Some(test_body())));
}

// @event(node: node_uri, lane: lane_uri)
#[test]
fn parse_event_with_named_headers() {
    let record = create_record("event", lane_named_headers());
    run_test(record, Envelope::make_event(TEST_NODE, TEST_LANE, None));
}

// @event(node_uri, lane_uri)
#[test]
fn parse_event_with_positional_headers() {
    let record = create_record("event", lane_positional_headers());
    run_test(record, Envelope::make_event(TEST_NODE, TEST_LANE, None));
}

// @event(node_uri, lane_uri)@test
#[test]
fn parse_event_with_body() {
    let record = create_record_with_test("event", lane_named_headers());
    run_test(
        record,
        Envelope::make_event(TEST_NODE, TEST_LANE, Some(test_body())),
    );
}

// @synced(node: node_uri, lane: lane_uri)
#[test]
fn parse_synced_with_named_headers() {
    let record = create_record("synced", lane_named_headers());
    run_test(record, Envelope::make_synced(TEST_NODE, TEST_LANE, None));
}

// @synced(node_uri, lane_uri)
#[test]
fn parse_synced_with_positional_headers() {
    let record = create_record("synced", lane_positional_headers());
    run_test(record, Envelope::make_synced(TEST_NODE, TEST_LANE, None));
}

// @synced(node_uri, lane_uri)@test
#[test]
fn parse_synced_with_body() {
    let record = create_record_with_test("synced", lane_named_headers());
    run_test(
        record,
        Envelope::make_synced(TEST_NODE, TEST_LANE, Some(test_body())),
    );
}

// @unlink(node: node_uri, lane: lane_uri)
#[test]
fn parse_unlink_with_named_headers() {
    let record = create_record("unlink", lane_named_headers());
    run_test(record, Envelope::make_unlink(TEST_NODE, TEST_LANE, None));
}

// @unlink(node_uri, lane_uri)
#[test]
fn parse_unlink_with_positional_headers() {
    let record = create_record("unlink", lane_positional_headers());
    run_test(record, Envelope::make_unlink(TEST_NODE, TEST_LANE, None));
}

// @unlink(node_uri, lane_uri)@test
#[test]
fn parse_unlink_with_body() {
    let record = create_record_with_test("unlink", lane_named_headers());
    run_test(
        record,
        Envelope::make_unlink(TEST_NODE, TEST_LANE, Some(test_body())),
    );
}

// @unlinked(node: node_uri, lane: lane_uri)
#[test]
fn parse_unlinked_with_named_headers() {
    let record = create_record("unlinked", lane_named_headers());
    run_test(record, Envelope::make_unlinked(TEST_NODE, TEST_LANE, None));
}

// @unlinked(node_uri, lane_uri)
#[test]
fn parse_unlinked_with_positional_headers() {
    let record = create_record("unlinked", lane_positional_headers());
    run_test(record, Envelope::make_unlinked(TEST_NODE, TEST_LANE, None));
}

// @unlinked(node_uri, lane_uri)@test
#[test]
fn parse_unlinked_with_body() {
    let record = create_record_with_test("unlinked", lane_named_headers());
    run_test(
        record,
        Envelope::make_unlinked(TEST_NODE, TEST_LANE, Some(test_body())),
    );
}

#[test]
fn unknown_tag() {
    let tag = "unknown_tag";
    let record = create_record_with_test(tag, lane_named_headers());

    run_test_expect_err(record, EnvelopeParseErr::UnknownTag(tag.into()));
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
        vec![Attr::of((
            "unlinked",
            Value::Record(
                Vec::new(),
                vec![
                    Item::Slot(Value::text("not_a_node"), Value::text(TEST_NODE)),
                    Item::Slot(Value::text("node_a_lane"), Value::text(TEST_LANE)),
                ],
            ),
        ))],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedKey("not_a_node".into()));
}

#[test]
fn unexpected_type() {
    let slot = Item::Slot(Value::Float64Value(1.0), Value::Float64Value(1.0));
    let record = Value::Record(
        vec![Attr::of((
            "unlinked",
            Value::Record(Vec::new(), vec![slot.clone()]),
        ))],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedItem(slot));
}

#[test]
fn too_many_named_headers() {
    let record = create_record(
        "sync",
        vec![
            Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
            Item::Slot(Value::text("lane"), Value::text(TEST_LANE)),
            Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
            Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
            Item::Slot(Value::text("host"), Value::text("swim.ai")),
        ],
    );

    run_test_expect_err(record, EnvelopeParseErr::UnexpectedKey("host".into()));
}

#[test]
fn too_many_positional_headers() {
    let record = create_record(
        "sync",
        vec![
            Item::ValueItem(Value::text(TEST_NODE)),
            Item::ValueItem(Value::text(TEST_LANE)),
            Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
            Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
            Item::ValueItem(Value::text("swim.ai")),
        ],
    );

    run_test_expect_err(
        record,
        EnvelopeParseErr::UnexpectedItem(Item::ValueItem(Value::text("swim.ai"))),
    );
}

#[test]
fn mixed_headers() {
    let record = create_record(
        "sync",
        vec![
            Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
            Item::ValueItem(Value::text(TEST_LANE)),
            Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
            Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
        ],
    );

    run_test(
        record,
        Envelope::make_sync(TEST_NODE, TEST_LANE, Some(TEST_RATE), Some(TEST_PRIO), None),
    );
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

    run_test(
        record,
        Envelope::make_auth(Some(Value::Record(
            vec![
                Attr {
                    name: "first".into(),
                    value: Value::Extant,
                },
                Attr {
                    name: "second".into(),
                    value: Value::Extant,
                },
                Attr {
                    name: "third".into(),
                    value: Value::Extant,
                },
            ],
            Vec::new(),
        ))),
    );
}

#[test]
fn duplicate_headers() {
    let record = Value::Record(
        vec![Attr::of((
            "sync",
            Value::Record(
                Vec::new(),
                vec![
                    Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
                    Item::Slot(Value::text("node"), Value::text(TEST_NODE)),
                ],
            ),
        ))],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::DuplicateHeader("node".into()));
}

#[test]
fn missing_header() {
    let record = Value::Record(
        vec![Attr::of((
            "synced",
            Value::Record(
                Vec::new(),
                vec![Item::Slot(Value::text("node"), Value::text(TEST_NODE))],
            ),
        ))],
        Vec::new(),
    );

    run_test_expect_err(record, EnvelopeParseErr::MissingHeader("lane".into()));
}

#[test]
fn multiple_attributes() {
    let record = Value::Record(
        vec![Attr::of((
            "sync",
            Value::Record(
                Vec::new(),
                vec![
                    Item::ValueItem(Value::text(TEST_NODE)),
                    Item::ValueItem(Value::text(TEST_LANE)),
                    Item::Slot(Value::text("prio"), Value::Float64Value(TEST_PRIO)),
                    Item::Slot(Value::text("rate"), Value::Float64Value(TEST_RATE)),
                ],
            ),
        ))],
        vec![ValueItem(Value::Float64Value(1.0))],
    );

    run_test(
        record,
        Envelope::make_sync(
            TEST_NODE,
            TEST_LANE,
            Some(TEST_RATE),
            Some(TEST_PRIO),
            Some(Value::Float64Value(1.0)),
        ),
    );
}

#[test]
fn tag() {
    let record = Value::Record(vec![Attr::of(("auth", Value::Extant))], Vec::new());

    let e = Envelope::try_from(record).unwrap();
    assert_eq!(e.tag(), "auth");
}

#[test]
fn auth_to_value() {
    let env = Envelope::make_auth(Some(test_body()));
    assert_eq!(env.tag(), AUTH_TAG);
    let value: Value = env.into();
    let expected = Value::of_attrs(vec![Attr::of("auth"), Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn deauth_to_value() {
    let env = Envelope::make_deauth(Some(test_body()));
    assert_eq!(env.tag(), DEAUTH_TAG);
    let value: Value = env.into();
    let expected = Value::of_attrs(vec![Attr::of("deauth"), Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn authed_to_value() {
    let env = Envelope::make_authed(Some(test_body()));
    assert_eq!(env.tag(), AUTHED_TAG);
    let value: Value = env.into();
    let expected = Value::of_attrs(vec![Attr::of("authed"), Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn deauthed_to_value() {
    let env = Envelope::make_deauthed(Some(test_body()));
    assert_eq!(env.tag(), DEAUTHED_TAG);
    let value: Value = env.into();
    let expected = Value::of_attrs(vec![Attr::of("deauthed"), Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn link_no_params_to_value() {
    let env = Envelope::make_link(TEST_NODE, TEST_LANE, None, None, Some(test_body()));
    assert_eq!(env.tag(), LINK_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("link", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn link_with_params_to_value() {
    let env = Envelope::make_link(
        TEST_NODE,
        TEST_LANE,
        Some(TEST_RATE),
        Some(TEST_PRIO),
        Some(test_body()),
    );
    let value: Value = env.into();
    let link_attr = Attr::with_items(
        "link",
        vec![
            ("node", Value::text(TEST_NODE)),
            ("lane", Value::text(TEST_LANE)),
            ("rate", TEST_RATE.into()),
            ("prio", TEST_PRIO.into()),
        ],
    );
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn sync_no_params_to_value() {
    let env = Envelope::make_sync(TEST_NODE, TEST_LANE, None, None, Some(test_body()));
    assert_eq!(env.tag(), SYNC_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("sync", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn sync_with_params_to_value() {
    let env = Envelope::make_sync(
        TEST_NODE,
        TEST_LANE,
        Some(TEST_RATE),
        Some(TEST_PRIO),
        Some(test_body()),
    );
    let value: Value = env.into();
    let link_attr = Attr::with_items(
        "sync",
        vec![
            ("node", Value::text(TEST_NODE)),
            ("lane", Value::text(TEST_LANE)),
            ("rate", TEST_RATE.into()),
            ("prio", TEST_PRIO.into()),
        ],
    );
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn unlink_to_value() {
    let env = Envelope::make_unlink(TEST_NODE, TEST_LANE, Some(test_body()));
    assert_eq!(env.tag(), UNLINK_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("unlink", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn command_to_value() {
    let env = Envelope::make_command(TEST_NODE, TEST_LANE, Some(test_body()));
    assert_eq!(env.tag(), CMD_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("command", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn linked_no_params_to_value() {
    let env = Envelope::make_linked(TEST_NODE, TEST_LANE, None, None, Some(test_body()));
    assert_eq!(env.tag(), LINKED_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("linked", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn linked_with_params_to_value() {
    let env = Envelope::make_linked(
        TEST_NODE,
        TEST_LANE,
        Some(TEST_RATE),
        Some(TEST_PRIO),
        Some(test_body()),
    );
    let value: Value = env.into();
    let link_attr = Attr::with_items(
        "linked",
        vec![
            ("node", Value::text(TEST_NODE)),
            ("lane", Value::text(TEST_LANE)),
            ("rate", TEST_RATE.into()),
            ("prio", TEST_PRIO.into()),
        ],
    );
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn synced_to_value() {
    let env = Envelope::make_synced(TEST_NODE, TEST_LANE, Some(test_body()));
    assert_eq!(env.tag(), SYNCED_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("synced", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn unlinked_to_value() {
    let env = Envelope::make_unlinked(TEST_NODE, TEST_LANE, Some(test_body()));
    assert_eq!(env.tag(), UNLINKED_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("unlinked", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn event_to_value() {
    let env = Envelope::make_event(TEST_NODE, TEST_LANE, Some(test_body()));
    assert_eq!(env.tag(), EVENT_TAG);
    let value: Value = env.into();
    let link_attr = Attr::with_items("event", vec![("node", TEST_NODE), ("lane", TEST_LANE)]);
    let expected = Value::of_attrs(vec![link_attr, Attr::of(TEST_TAG)]);
    assert_eq!(value, expected);
}

#[test]
fn envelope_parse_err_display() {
    let err = EnvelopeParseErr::Malformatted;
    assert_eq!(err.to_string(), "Envelope was malformed.");
    let err = EnvelopeParseErr::DuplicateHeader("name".into());
    assert_eq!(err.to_string(), "Duplicate header: 'name'.");
    let err = EnvelopeParseErr::DuplicateKey("name".into());
    assert_eq!(err.to_string(), "Duplicate key: 'name'.");
    let err = EnvelopeParseErr::MissingHeader("name".into());
    assert_eq!(err.to_string(), "Required header 'name' was missing.");
    let err = EnvelopeParseErr::UnexpectedItem(Item::ValueItem(Value::Extant));
    assert_eq!(err.to_string(), "Envelope was malformed.");
    let err = EnvelopeParseErr::UnexpectedKey("name".into());
    assert_eq!(err.to_string(), "Unexpected key: 'name'.");
    let err = EnvelopeParseErr::UnexpectedType(Value::Int32Value(2));
    assert_eq!(err.to_string(), "Value of unexpected kind: 'Int32'.");
    let err = EnvelopeParseErr::UnknownTag("name".into());
    assert_eq!(err.to_string(), "Unknown tag in envelope: 'name'.");
}
