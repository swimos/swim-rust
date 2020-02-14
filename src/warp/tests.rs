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
use crate::warp::model::{Envelope, HostAddressed, LaneAddressed, LinkAddressed};

fn run_test(recon: &str, expected: Envelope) {
    let value = parse_single(recon).unwrap();
    let e = Envelope::try_from(value);

    match e {
        Ok(env) => assert_that!(expected, eq(env)),
        Err(e) => {
            eprintln!("{:?}", e);
            panic!(e);
        }
    }
}

fn link_addressed_no_body() -> LinkAddressed {
    LinkAddressed {
        lane: lane_addressed_no_body(),
        prio: 0.5,
        rate: 1.0,
    }
}

fn link_addressed_test_record() -> LinkAddressed {
    LinkAddressed {
        lane: lane_addressed_test_record(),
        prio: 0.5,
        rate: 1.0,
    }
}

fn lane_addressed_no_body() -> LaneAddressed {
    LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
        body: Value::Extant,
    }
}

fn lane_addressed_test_record() -> LaneAddressed {
    LaneAddressed {
        node_uri: String::from("node_uri"),
        lane_uri: String::from("lane_uri"),
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

#[test]
fn parse_auth() {
    run_test("@auth",
             Envelope::AuthRequest(HostAddressed {
                 body: Value::Extant
             }));
}

#[test]
fn parse_auth_with_body() {
    run_test("@auth@test",
             Envelope::AuthRequest(HostAddressed {
                 body: Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()),
             }));
}

#[test]
fn parse_authed() {
    run_test("@authed",
             Envelope::AuthedResponse(HostAddressed {
                 body: Value::Extant
             }));
}

#[test]
fn parse_authed_with_body() {
    run_test("@authed@test",
             Envelope::AuthedResponse(HostAddressed {
                 body: Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()),
             }));
}

#[test]
fn parse_command_with_named_headers() {
    run_test("@command(node: node_uri, lane: lane_uri)",
             Envelope::CommandMessage(lane_addressed_no_body()));
}

#[test]
fn parse_command_with_positional_headers() {
    run_test("@command(node_uri, lane_uri)",
             Envelope::CommandMessage(lane_addressed_no_body()));
}

#[test]
fn parse_command_with_body() {
    run_test("@command(node_uri, lane_uri)@test",
             Envelope::CommandMessage(lane_addressed_test_record()));
}

#[test]
fn parse_deauthed() {
    run_test("@deauthed",
             Envelope::DeauthedResponse(HostAddressed {
                 body: Value::Extant
             }));
}

#[test]
fn parse_deauthed_with_body() {
    run_test("@deauthed@test",
             Envelope::DeauthedResponse(HostAddressed {
                 body: Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()),
             }));
}

#[test]
fn parse_deauth() {
    run_test("@deauth",
             Envelope::DeauthRequest(HostAddressed {
                 body: Value::Extant
             }));
}

#[test]
fn parse_deauth_with_body() {
    run_test("@deauth@test",
             Envelope::DeauthRequest(HostAddressed {
                 body: Value::Record(
                     vec![Attr { name: String::from("test"), value: Value::Extant }],
                     Vec::new()),
             }));
}


#[test]
fn parse_event_with_named_headers() {
    run_test("@event(node: node_uri, lane: lane_uri)",
             Envelope::EventMessage(lane_addressed_no_body()));
}

#[test]
fn parse_event_with_positional_headers() {
    run_test("@event(node_uri, lane_uri)",
             Envelope::EventMessage(lane_addressed_no_body()));
}

#[test]
fn parse_event_with_body() {
    run_test("@event(node_uri, lane_uri)@test",
             Envelope::EventMessage(lane_addressed_test_record()));
}

#[test]
fn parse_synced_with_named_headers() {
    run_test("@synced(node: node_uri, lane: lane_uri)",
             Envelope::SyncedResponse(lane_addressed_no_body()));
}

#[test]
fn parse_synced_with_positional_headers() {
    run_test("@synced(node_uri, lane_uri)",
             Envelope::SyncedResponse(lane_addressed_no_body()));
}

#[test]
fn parse_synced_with_body() {
    run_test("@synced(node_uri, lane_uri)@test",
             Envelope::SyncedResponse(lane_addressed_test_record()));
}

#[test]
fn parse_unlink_with_named_headers() {
    run_test("@unlink(node: node_uri, lane: lane_uri)",
             Envelope::UnlinkRequest(lane_addressed_no_body()));
}

#[test]
fn parse_unlink_with_positional_headers() {
    run_test("@unlink(node_uri, lane_uri)",
             Envelope::UnlinkRequest(lane_addressed_no_body()));
}

#[test]
fn parse_unlink_with_body() {
    run_test("@unlink(node_uri, lane_uri)@test",
             Envelope::UnlinkRequest(lane_addressed_test_record()));
}

#[test]
fn parse_unlinked_with_named_headers() {
    run_test("@unlinked(node: node_uri, lane: lane_uri)",
             Envelope::UnlinkedResponse(lane_addressed_no_body()));
}

#[test]
fn parse_unlinked_with_positional_headers() {
    run_test("@unlinked(node_uri, lane_uri)",
             Envelope::UnlinkedResponse(lane_addressed_no_body()));
}

#[test]
fn parse_unlinked_with_body() {
    run_test("@unlinked(node_uri, lane_uri)@test",
             Envelope::UnlinkedResponse(lane_addressed_test_record()));
}