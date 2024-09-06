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

use apache_avro::Schema;
use bytes::{Buf, BufMut};
use swimos_model::{Attr, Item, Value};

use crate::ser::{is_array, is_record, RESERVE_INIT, RESERVE_MULT};

use super::{MessageSerializer, SerializationError};

type AvroValue = apache_avro::types::Value;

pub struct AvroSerializer(Schema);

impl MessageSerializer for AvroSerializer {
    fn serialize(
        &self,
        _name: &str,
        message: &Value,
        target: &mut bytes::BytesMut,
    ) -> Result<(), SerializationError> {
        let AvroSerializer(schema) = self;
        let avro_value = convert_recon_value(message)?;
        let mut next_res = RESERVE_INIT.max(target.remaining_mut().saturating_mul(RESERVE_MULT));
        let body_offset = target.remaining();
        loop {
            let mut writer = apache_avro::Writer::new(schema, target.writer());
            let result = writer
                .append_value_ref(&avro_value)
                .and_then(|n| writer.flush().map(move |m| n + m));
            match result {
                Ok(0) | Err(apache_avro::Error::WriteBytes(_)) => {
                    target.truncate(body_offset);
                    target.reserve(next_res);
                    next_res = next_res.saturating_mul(RESERVE_MULT);
                }
                Ok(_) => break Ok(()),
                Err(e) => break Err(SerializationError::SerializerFailed(Box::new(e))),
            }
        }
    }
}

fn convert_recon_value(value: &Value) -> Result<AvroValue, SerializationError> {
    let v = match value {
        Value::Extant => AvroValue::Null,
        Value::Int32Value(n) => AvroValue::Int(*n),
        Value::Int64Value(n) => AvroValue::Long(*n),
        Value::UInt32Value(n) => {
            if let Ok(n) = i32::try_from(*n) {
                AvroValue::Int(n)
            } else {
                AvroValue::Long(*n as i64)
            }
        }
        Value::UInt64Value(n) => {
            if let Ok(n) = i64::try_from(*n) {
                AvroValue::Long(n)
            } else {
                return Err(SerializationError::IntegerOutOfRange(value.clone()));
            }
        }
        Value::Float64Value(x) => AvroValue::Double(*x),
        Value::BooleanValue(p) => AvroValue::Boolean(*p),
        Value::BigInt(n) => {
            if let Ok(n) = i64::try_from(n) {
                AvroValue::Long(n)
            } else {
                return Err(SerializationError::IntegerOutOfRange(value.clone()));
            }
        }
        Value::BigUint(n) => {
            if let Ok(n) = i64::try_from(n) {
                AvroValue::Long(n)
            } else {
                return Err(SerializationError::IntegerOutOfRange(value.clone()));
            }
        }
        Value::Text(text) => AvroValue::String(text.to_string()),
        Value::Record(attrs, items) => {
            if attrs.is_empty() {
                if is_array(items) {
                    to_avro_array(items)?
                } else if is_record(items) {
                    to_avro_record(items)?
                } else {
                    items_arr(items)?
                }
            } else {
                to_expanded_avro_record(attrs, items)?
            }
        }
        Value::Data(blob) => AvroValue::Bytes(blob.as_ref().to_vec()),
    };
    Ok(v)
}

fn to_avro_array(items: &[Item]) -> Result<AvroValue, SerializationError> {
    let items = items
        .iter()
        .filter_map(|item| match item {
            Item::ValueItem(v) => Some(v),
            Item::Slot(_, _) => None,
        })
        .map(convert_recon_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AvroValue::Array(items))
}

fn items_arr(items: &[Item]) -> Result<AvroValue, SerializationError> {
    let arr_items = items
        .iter()
        .map(|item| match item {
            Item::ValueItem(v) => convert_recon_value(v),
            Item::Slot(k, v) => slot_item(k, v),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AvroValue::Array(arr_items))
}

fn to_avro_record(items: &[Item]) -> Result<AvroValue, SerializationError> {
    let items = items
        .iter()
        .filter_map(|item| match item {
            Item::Slot(Value::Text(key), value) => Some((key.to_string(), value)),
            _ => None,
        })
        .map(|(k, v)| convert_recon_value(v).map(move |v| (k, v)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AvroValue::Record(items))
}

const ATTRS: &str = "attributes";
const ITEMS: &str = "items";
const KEY: &str = "key";
const VALUE: &str = "value";

fn slot_item(key: &Value, value: &Value) -> Result<AvroValue, SerializationError> {
    let k = convert_recon_value(key)?;
    let v = convert_recon_value(value)?;
    let fields = vec![(KEY.to_string(), k), (VALUE.to_string(), v)];
    Ok(AvroValue::Record(fields))
}

fn attr_object(attrs: &[Attr]) -> Result<AvroValue, SerializationError> {
    let fields = attrs
        .iter()
        .map(|attr| convert_recon_value(&attr.value).map(|v| (attr.name.to_string(), v)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AvroValue::Record(fields))
}

fn to_expanded_avro_record(
    attrs: &[Attr],
    items: &[Item],
) -> Result<AvroValue, SerializationError> {
    let fields = vec![
        (ATTRS.to_string(), attr_object(attrs)?),
        (ITEMS.to_string(), items_arr(items)?),
    ];
    Ok(AvroValue::Record(fields))
}
