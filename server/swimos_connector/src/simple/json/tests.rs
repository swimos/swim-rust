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

use crate::simple::json::{build_uri, deferred_deserializer, NodeSelectorPattern};
use serde::Serialize;
use serde_json::{json, Value};
use std::str::FromStr;

fn mock_value() -> Value {
    json! {
        {
            "success": 200,
            "payload": "waffles"
        }
    }
}

fn to_bytes(s: impl Serialize) -> Vec<u8> {
    serde_json::to_vec(&s).unwrap()
}

#[test]
fn node_uri() {
    let route =
        NodeSelectorPattern::from_str("/$key/$value.payload").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    let node_uri = build_uri(
        &route,
        &mut deferred_deserializer(key),
        &mut deferred_deserializer(value),
    )
    .expect("Failed to build node URI");

    assert_eq!(node_uri, "/1/waffles")
}

#[test]
fn invalid_node_uri() {
    let route = NodeSelectorPattern::from_str("/$key/$value").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    build_uri(
        &route,
        &mut deferred_deserializer(key),
        &mut deferred_deserializer(value),
    )
    .expect_err("Should have failed to build node URI due to object being used");
}
