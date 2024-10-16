// Copyright 2015-2024 Swim Inc.
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

use crate::selector::pubsub::relay::{
    parse_lane_selector, parse_node_selector, parse_value_selector, LaneSelector, NodeSelector,
    PayloadSegment, RelayPayloadSelector, Segment,
};
use crate::selector::{KeySelector, PayloadSelector, PubSubSelector, TopicSelector};
use swimos_model::Value;

#[test]
fn parse_lane() {
    fn ok(pattern: &str, expected: LaneSelector<PubSubSelector>) {
        match parse_lane_selector(pattern) {
            Ok(actual) => {
                assert_eq!(expected, actual);
            }
            Err(e) => {
                panic!("Failed to parse lane selector {}: {:?}", pattern, e);
            }
        }
    }

    fn err(pattern: &str) {
        if let Ok(actual) = parse_lane_selector(pattern) {
            panic!(
                "Expected parse error from pattern {}, but got {:?}",
                pattern, actual
            );
        }
    }

    ok(
        "lane",
        LaneSelector::new(Segment::Static("lane".to_string()), "lane".to_string()),
    );
    ok(
        "$key",
        LaneSelector::new(
            Segment::Selector(PubSubSelector::inject(KeySelector::default())),
            "$key".to_string(),
        ),
    );
    ok(
        "$payload",
        LaneSelector::new(
            Segment::Selector(PubSubSelector::inject(PayloadSelector::default())),
            "$payload".to_string(),
        ),
    );
    ok(
        "$topic",
        LaneSelector::new(
            Segment::Selector(PubSubSelector::inject(TopicSelector)),
            "$topic".to_string(),
        ),
    );
    err("$donut");
    err("a string with words");
    err(" ");
    err("!!!");
    err("$");
    err("/$");
    err("/::");
    err("/");
    err("//");
    err("//aaa");
    err("::/");
    err(":/");
    err("");
    err(":");
    err("$");
    err("~");
    err("$key.$value");
    err("$key/::$value");
    err("blah/blah");
    err("$key/$value");
    err("$key.field/blah");
    err("blah/$key");
}

#[test]
fn parse_node() {
    fn ok(pattern: &str, expected: NodeSelector<PubSubSelector>) {
        match parse_node_selector(pattern) {
            Ok(actual) => {
                assert_eq!(expected, actual);
            }
            Err(e) => {
                panic!("Failed to parse node selector {}: {:?}", pattern, e);
            }
        }
    }

    fn err(pattern: &str) {
        if let Ok(actual) = parse_node_selector(pattern) {
            panic!(
                "Expected parse error from pattern {}, but got {:?}",
                pattern, actual
            );
        }
    }
    ok(
        "/lane",
        NodeSelector::new(
            "/lane".to_string(),
            vec![Segment::Static("/lane".to_string())],
        ),
    );
    ok(
        "/lane/sub/path",
        NodeSelector::new(
            "/lane/sub/path".to_string(),
            vec![Segment::Static("/lane/sub/path".to_string())],
        ),
    );
    ok(
        "/node/$key",
        NodeSelector::new(
            "/node/$key".to_string(),
            vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(KeySelector::default())),
            ],
        ),
    );
    ok(
        "/node/$payload",
        NodeSelector::new(
            "/node/$payload".to_string(),
            vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(PayloadSelector::default())),
            ],
        ),
    );
    ok(
        "/node/$topic",
        NodeSelector::new(
            "/node/$topic".to_string(),
            vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(TopicSelector)),
            ],
        ),
    );
    err("/");
    err("//");
    err("///");
    err("/a//b");
    err("/a/b//");
    err("/a/b//c/");
    err("/a/b/c//");
    err("/a/b/c/d//");
    err("/$blah");
    err("/$key/$blah/blah/");
    err("/$key/$blah/blah");
}

#[test]
fn parse_payload() {
    fn value_ok(pattern: &str, expected: RelayPayloadSelector<PubSubSelector>) {
        match parse_value_selector(pattern, true) {
            Ok(actual) => {
                assert_eq!(expected, actual);
            }
            Err(e) => {
                panic!("Failed to parse payload selector {}: {:?}", pattern, e);
            }
        }
    }

    value_ok(
        "blah",
        RelayPayloadSelector::value(
            PayloadSegment::Value(Value::from("blah")),
            "blah".to_string(),
            true,
        ),
    );
    value_ok(
        "$key",
        RelayPayloadSelector::value(
            PayloadSegment::Selector(PubSubSelector::inject(KeySelector::default())),
            "$key".to_string(),
            true,
        ),
    );
    value_ok(
        "$payload",
        RelayPayloadSelector::value(
            PayloadSegment::Selector(PubSubSelector::inject(PayloadSelector::default())),
            "$payload".to_string(),
            true,
        ),
    );
    value_ok(
        "$topic",
        RelayPayloadSelector::value(
            PayloadSegment::Selector(PubSubSelector::inject(TopicSelector)),
            "$topic".to_string(),
            true,
        ),
    );
}
