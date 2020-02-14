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

use crate::model::{Attr, Value};
use crate::model::parser::parse_single;
use crate::warp::model::{Envelope, LinkAddressed};

fn run_test(recon: &str, expected: Envelope) {
    let value = parse_single(recon).unwrap();
    let e = Envelope::try_from(value);
    let env = e.unwrap();

    assert_that!(expected, eq(env));
}

fn link_addressed_no_body() -> LinkAddressed {
    LinkAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        prio: 0.5,
        rate: 1.0,
        body: Value::Extant,
    }
}

fn link_addressed_test_record() -> LinkAddressed {
    LinkAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        prio: 0.5,
        rate: 1.0,
        body: Value::Record(
            vec![Attr { name: String::from("test"), value: Value::Extant }],
            Vec::new()),
    }
}

#[test]
fn parse_sync_with_named_headers() {
    run_test("@sync(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::SyncRequest(link_addressed_no_body()));
}

#[test]
fn parse_sync_with_positional_headers() {
    run_test("@sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::SyncRequest(link_addressed_no_body()));
}

#[test]
fn parse_sync_with_body() {
    run_test("@sync(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test",
             Envelope::SyncRequest(link_addressed_test_record()));
}

#[test]
fn parse_link_with_named_headers() {
    run_test("@link(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::LinkRequest(link_addressed_no_body()));
}

#[test]
fn parse_link_with_positional_headers() {
    run_test("@link(node_uri, lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::LinkRequest(link_addressed_no_body()));
}

#[test]
fn parse_link_with_body() {
    run_test("@link(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test",
             Envelope::LinkRequest(link_addressed_test_record()));
}

#[test]
fn parse_linked_with_named_headers() {
    run_test("@linked(node: node_uri, lane: lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::LinkedResponse(link_addressed_no_body()));
}

#[test]
fn parse_linked_with_positional_headers() {
    run_test("@linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)",
             Envelope::LinkedResponse(link_addressed_no_body()));
}

#[test]
fn parse_linked_with_body() {
    run_test("@linked(node_uri, lane_uri, prio: 0.5, rate: 1.0)@test",
             Envelope::LinkedResponse(link_addressed_test_record()));
}