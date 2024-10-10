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
    Inner, LaneSelector, NodeSelector, PayloadSegment, PayloadSelector, Segment,
};
use crate::selector::PayloadSelector as ValueSelector;
use crate::selector::{KeySelector, PubSubSelector, TopicSelector};
use std::str::FromStr;
use swimos_model::Value;

#[test]
fn parse_lane() {
    fn ok(pattern: &str, expected: LaneSelector) {
        match LaneSelector::from_str(pattern) {
            Ok(actual) => {
                assert_eq!(expected, actual);
            }
            Err(e) => {
                panic!("Failed to parse lane selector {}: {:?}", pattern, e);
            }
        }
    }

    fn err(pattern: &str) {
        match LaneSelector::from_str(pattern) {
            Ok(actual) => {
                panic!(
                    "Expected parse error from pattern {}, but got {:?}",
                    pattern, actual
                );
            }
            Err(_) => {}
        }
    }

    ok(
        "lane",
        LaneSelector {
            segment: Segment::Static("lane".to_string()),
            pattern: "lane".to_string(),
        },
    );
    ok(
        "$key",
        LaneSelector {
            segment: Segment::Selector(PubSubSelector::inject(KeySelector::default())),
            pattern: "$key".to_string(),
        },
    );
    ok(
        "$payload",
        LaneSelector {
            segment: Segment::Selector(PubSubSelector::inject(ValueSelector::default())),
            pattern: "$payload".to_string(),
        },
    );
    ok(
        "$topic",
        LaneSelector {
            segment: Segment::Selector(PubSubSelector::inject(TopicSelector::default())),
            pattern: "$topic".to_string(),
        },
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
    fn ok(pattern: &str, expected: NodeSelector) {
        match NodeSelector::from_str(pattern) {
            Ok(actual) => {
                assert_eq!(expected, actual);
            }
            Err(e) => {
                panic!("Failed to parse node selector {}: {:?}", pattern, e);
            }
        }
    }

    fn err(pattern: &str) {
        match NodeSelector::from_str(pattern) {
            Ok(actual) => {
                panic!(
                    "Expected parse error from pattern {}, but got {:?}",
                    pattern, actual
                );
            }
            Err(_) => {}
        }
    }
    ok(
        "/lane",
        NodeSelector {
            segments: vec![Segment::Static("/lane".to_string())],
            pattern: "/lane".to_string(),
        },
    );
    ok(
        "/lane/sub/path",
        NodeSelector {
            segments: vec![Segment::Static("/lane/sub/path".to_string())],
            pattern: "/lane/sub/path".to_string(),
        },
    );
    ok(
        "/node/$key",
        NodeSelector {
            segments: vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(KeySelector::default())),
            ],
            pattern: "/node/$key".to_string(),
        },
    );
    ok(
        "/node/$payload",
        NodeSelector {
            segments: vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(ValueSelector::default())),
            ],
            pattern: "/node/$payload".to_string(),
        },
    );
    ok(
        "/node/$topic",
        NodeSelector {
            segments: vec![
                Segment::Static("/node/".to_string()),
                Segment::Selector(PubSubSelector::inject(TopicSelector::default())),
            ],
            pattern: "/node/$topic".to_string(),
        },
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
    fn value_ok(pattern: &str, expected: PayloadSelector) {
        match PayloadSelector::value(pattern, true) {
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
        PayloadSelector {
            inner: Inner::Value {
                pattern: "blah".to_string(),
                segment: PayloadSegment::Value(Value::from("blah")),
            },
            required: true,
        },
    );
    value_ok(
        "$key",
        PayloadSelector {
            inner: Inner::Value {
                pattern: "$key".to_string(),
                segment: PayloadSegment::Selector(PubSubSelector::inject(KeySelector::default())),
            },
            required: true,
        },
    );
    value_ok(
        "$payload",
        PayloadSelector {
            inner: Inner::Value {
                pattern: "$payload".to_string(),
                segment: PayloadSegment::Selector(PubSubSelector::inject(ValueSelector::default())),
            },
            required: true,
        },
    );
    value_ok(
        "$topic",
        PayloadSelector {
            inner: Inner::Value {
                pattern: "$topic".to_string(),
                segment: PayloadSegment::Selector(PubSubSelector::inject(TopicSelector::default())),
            },
            required: true,
        },
    );
}
