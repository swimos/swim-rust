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

use crate::agent::dispatch::pending::PendingEnvelopes;
use crate::routing::{TaggedClientEnvelope, RoutingAddr};
use swim_common::warp::envelope::{LinkMessage, OutgoingHeader, OutgoingLinkMessage};
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::warp::path::RelativePath;

#[test]
fn empty_pending_envelopes() {

    let mut pending = PendingEnvelopes::new(1);
    assert!(pending.pop("a").is_none());

}

const ADDR: RoutingAddr = RoutingAddr::remote(1);

fn make_envelope(lane: &str, n: i32) -> TaggedClientEnvelope {
    TaggedClientEnvelope(ADDR, OutgoingLinkMessage::make_command(
        "node", lane, Some(n.into_value())))
}

fn check_envelope(expected_lane: &str, n: i32, envelope: TaggedClientEnvelope) {
    let TaggedClientEnvelope(
        addr,
        LinkMessage {
            header,
            path,
            body
        }) = envelope;

    assert_eq!(addr, ADDR);
    assert!(matches!(header, OutgoingHeader::Command));
    assert!(matches!(path, RelativePath { node, lane } if node == "node" && lane == expected_lane));
    assert_eq!(body, Some(Value::Int32Value(n)));
}

#[test]
fn push_and_pop_pending() {

    let mut pending = PendingEnvelopes::new(4);

    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 2)), Ok(true));
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 3)), Ok(true));
    assert_eq!(pending.enqueue("b".to_string(), make_envelope("b", 4)), Ok(true));

    let first = pending.pop("a");
    assert!(first.is_some());
    check_envelope("a", 2, first.unwrap());

    let second = pending.pop("a");
    assert!(second.is_some());
    check_envelope("a", 3, second.unwrap());

    assert!(pending.pop("a").is_none());

    let third = pending.pop("b");
    assert!(third.is_some());
    check_envelope("b", 4, third.unwrap());

    assert!(pending.pop("b").is_none());
}

#[test]
fn pending_full() {

    let mut pending = PendingEnvelopes::new(2);
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 2)), Ok(true));
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 3)), Ok(false));

    let result = pending.enqueue("a".to_string(), make_envelope("a", 4));
    assert!(result.is_err());
    let (label, envelope) = result.err().unwrap();
    assert_eq!(label, "a");
    check_envelope("a", 4, envelope);

    let result = pending.enqueue("b".to_string(), make_envelope("b", 5));
    assert!(result.is_err());
    let (label, envelope) = result.err().unwrap();
    assert_eq!(label, "b");
    check_envelope("b", 5, envelope);

}

#[test]
fn push_front_pending() {
    let mut pending = PendingEnvelopes::new(4);
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 2)), Ok(true));
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 3)), Ok(true));
    assert_eq!(pending.replace("a".to_string(), make_envelope("a", 1)), Ok(true));


    let first = pending.pop("a");
    assert!(first.is_some());
    check_envelope("a", 1, first.unwrap());

    let second = pending.pop("a");
    assert!(second.is_some());
    check_envelope("a", 2, second.unwrap());

    let third = pending.pop("a");
    assert!(third.is_some());
    check_envelope("a", 3, third.unwrap());

    assert!(pending.pop("a").is_none());
}

#[test]
fn push_front_pending_full() {

    let mut pending = PendingEnvelopes::new(2);
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 2)), Ok(true));
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 3)), Ok(false));

    let result = pending.replace("a".to_string(), make_envelope("a", 4));
    assert!(result.is_err());
    let (label, envelope) = result.err().unwrap();
    assert_eq!(label, "a");
    check_envelope("a", 4, envelope);

    let result = pending.replace("b".to_string(), make_envelope("b", 5));
    assert!(result.is_err());
    let (label, envelope) = result.err().unwrap();
    assert_eq!(label, "b");
    check_envelope("b", 5, envelope);

}

#[test]
fn clear_pending_for_lane() {

    let mut pending = PendingEnvelopes::new(4);

    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 2)), Ok(true));
    assert_eq!(pending.enqueue("a".to_string(), make_envelope("a", 3)), Ok(true));
    assert_eq!(pending.enqueue("b".to_string(), make_envelope("b", 4)), Ok(true));

    assert!(pending.clear("a"));

    assert!(pending.pop("a").is_none());

    let third = pending.pop("b");
    assert!(third.is_some());
    check_envelope("b", 4, third.unwrap());

    assert!(pending.pop("b").is_none());
}

