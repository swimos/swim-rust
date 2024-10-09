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

use crate::deser::{Deser, MessageDeserializer};
use crate::relay::selector::NodeSelector;
use frunk::hlist;
use serde::Serialize;
use serde_json::json;
use std::str::FromStr;
use swimos_model::{Item, Value};

type JsonValue = serde_json::Value;

#[derive(Default)]
struct JsonDeserializer;

impl MessageDeserializer for JsonDeserializer {
    type Error = serde_json::Error;

    fn deserialize(&self, buf: &[u8]) -> Result<Value, Self::Error> {
        let v: serde_json::Value = serde_json::from_slice(buf)?;
        Ok(convert_json_value(v))
    }
}

fn mock_value() -> JsonValue {
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

fn convert_json_value(input: JsonValue) -> Value {
    match input {
        JsonValue::Null => Value::Extant,
        JsonValue::Bool(p) => Value::BooleanValue(p),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_u64() {
                Value::UInt64Value(i)
            } else if let Some(i) = n.as_i64() {
                Value::Int64Value(i)
            } else {
                Value::Float64Value(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        JsonValue::String(s) => Value::Text(s.into()),
        JsonValue::Array(arr) => Value::record(
            arr.into_iter()
                .map(|v| Item::ValueItem(convert_json_value(v)))
                .collect(),
        ),
        JsonValue::Object(obj) => Value::record(
            obj.into_iter()
                .map(|(k, v)| Item::Slot(Value::Text(k.into()), convert_json_value(v)))
                .collect(),
        ),
    }
}

#[test]
fn node_uri() {
    let route = NodeSelector::from_str("/$key/$value.payload").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    let topic = Value::text("topic");
    let deser = JsonDeserializer.boxed();
    let key = Deser::new(key.as_slice(), &deser);
    let value = Deser::new(value.as_slice(), &deser);
    let args = hlist![topic, key, value];

    let node_uri = route.select(&args).expect("Failed to build node URI");

    assert_eq!(node_uri, "/1/waffles")
}

#[test]
fn invalid_node_uri() {
    let route = NodeSelector::from_str("/$key/$value").expect("Failed to parse route");
    let key = to_bytes(1);
    let value = to_bytes(mock_value());

    let deser = JsonDeserializer.boxed();
    let topic = Value::text("topic");
    let key = Deser::new(key.as_slice(), &deser);
    let value = Deser::new(value.as_slice(), &deser);
    let args = hlist![topic, key, value];

    route
        .select(&args)
        .expect_err("Should have failed to build node URI due to object being used");
}
