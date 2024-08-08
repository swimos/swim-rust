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

#[cfg(test)]
mod tests;

use serde_json::Value as JsonValue;
use swimos_model::{Item, Value};

use crate::connector::MessagePart;

use super::{MessageDeserializer, MessageView};

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

#[derive(Clone, Copy, Default, Debug)]
pub struct JsonDeserializer;

impl MessageDeserializer for JsonDeserializer {
    type Error = serde_json::Error;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Payload => message.payload(),
        };
        let v: serde_json::Value = serde_json::from_slice(payload)?;
        Ok(convert_json_value(v))
    }
}
