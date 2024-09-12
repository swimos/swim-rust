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

use bytes::{Buf, BufMut, BytesMut};
use serde_json::error::Category;
use serde_json::{Map, Number, Value as JsonValue};
use swimos_model::{Attr, Item, Value, ValueKind};

use super::{is_array, is_record, MessageSerializer, SerializationError};

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonSerializer;

use super::{RESERVE_INIT, RESERVE_MULT};

impl MessageSerializer for JsonSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        let json_value = convert_recon_value(message)?;
        let mut next_res = RESERVE_INIT.max(target.remaining_mut().saturating_mul(RESERVE_MULT));
        let body_offset = target.remaining();
        loop {
            let result = serde_json::to_writer(target.writer(), &json_value);
            match result {
                Ok(()) => break Ok(()),
                Err(e) if e.classify() == Category::Io => {
                    target.truncate(body_offset);
                    target.reserve(next_res);
                    next_res = next_res.saturating_mul(RESERVE_MULT);
                }
                Err(e) => break Err(SerializationError::SerializerFailed(Box::new(e))),
            }
        }
    }
}

fn convert_recon_value(input: &Value) -> Result<JsonValue, SerializationError> {
    let v = match input {
        Value::Extant => JsonValue::Null,
        Value::Int32Value(n) => JsonValue::Number((*n).into()),
        Value::Int64Value(n) => JsonValue::Number((*n).into()),
        Value::UInt32Value(n) => JsonValue::Number((*n).into()),
        Value::UInt64Value(n) => JsonValue::Number((*n).into()),
        Value::Float64Value(x) => {
            if let Some(n) = Number::from_f64(*x) {
                JsonValue::Number(n)
            } else {
                return Err(SerializationError::FloatOutOfRange(*x));
            }
        }
        Value::BooleanValue(p) => JsonValue::Bool(*p),
        Value::BigInt(n) => {
            if let Ok(n) = i64::try_from(n) {
                JsonValue::Number(n.into())
            } else if let Ok(n) = u64::try_from(n) {
                JsonValue::Number(n.into())
            } else {
                return Err(SerializationError::IntegerOutOfRange(input.clone()));
            }
        }
        Value::BigUint(n) => {
            if let Ok(n) = u64::try_from(n) {
                JsonValue::Number(n.into())
            } else {
                return Err(SerializationError::IntegerOutOfRange(input.clone()));
            }
        }
        Value::Text(string) => JsonValue::String(string.to_string()),
        Value::Record(attrs, items) => {
            if attrs.is_empty() {
                if is_array(items) {
                    to_json_array(items)?
                } else if is_record(items) {
                    to_json_object(items)?
                } else {
                    items_arr(items)?
                }
            } else {
                to_expanded_json_object(attrs, items)?
            }
        }
        Value::Data(_) => return Err(SerializationError::InvalidKind(ValueKind::Data)),
    };
    Ok(v)
}

fn to_json_array(items: &[Item]) -> Result<JsonValue, SerializationError> {
    let items = items
        .iter()
        .filter_map(|item| match item {
            Item::ValueItem(v) => Some(v),
            Item::Slot(_, _) => None,
        })
        .map(convert_recon_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JsonValue::Array(items))
}

fn to_expanded_json_object(
    attrs: &[Attr],
    items: &[Item],
) -> Result<JsonValue, SerializationError> {
    let fields = [
        (ATTRS.to_string(), attr_object(attrs)?),
        (ITEMS.to_string(), items_arr(items)?),
    ]
    .into_iter()
    .collect();
    Ok(JsonValue::Object(fields))
}

fn items_arr(items: &[Item]) -> Result<JsonValue, SerializationError> {
    let arr_items = items
        .iter()
        .map(|item| match item {
            Item::ValueItem(v) => convert_recon_value(v),
            Item::Slot(k, v) => slot_item(k, v),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JsonValue::Array(arr_items))
}

const ATTRS: &str = "attributes";
const ITEMS: &str = "items";
const KEY: &str = "key";
const VALUE: &str = "value";

fn slot_item(key: &Value, value: &Value) -> Result<JsonValue, SerializationError> {
    let k = convert_recon_value(key)?;
    let v = convert_recon_value(value)?;
    let map = [(KEY.to_string(), k), (VALUE.to_string(), v)]
        .into_iter()
        .collect();
    Ok(JsonValue::Object(map))
}

fn attr_object(attrs: &[Attr]) -> Result<JsonValue, SerializationError> {
    let items = attrs
        .iter()
        .map(|attr| convert_recon_value(&attr.value).map(|v| (attr.name.to_string(), v)))
        .collect::<Result<Map<_, _>, _>>()?;
    Ok(JsonValue::Object(items))
}

fn to_json_object(items: &[Item]) -> Result<JsonValue, SerializationError> {
    let items = items
        .iter()
        .filter_map(|item| match item {
            Item::Slot(Value::Text(key), value) => Some((key.to_string(), value)),
            _ => None,
        })
        .map(|(k, v)| convert_recon_value(v).map(move |v| (k, v)))
        .collect::<Result<Map<_, _>, _>>()?;
    Ok(JsonValue::Object(items))
}
