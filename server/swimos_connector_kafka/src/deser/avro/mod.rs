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

use apache_avro::{types::Value as AvroValue, Schema};
use swimos_model::{Item, Value};
use thiserror::Error;

use std::{io::Cursor, time::Duration};

use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, Utc};
use swimos_form::Form;
use swimos_model::{BigInt, Blob, Timestamp};

use crate::connector::MessagePart;

use super::{MessageDeserializer, MessageView};

/// Error type for the Avro deserializer.
#[derive(Error, Debug)]
pub enum AvroError {
    #[error("Failed to read Avro record: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("Unsupported Avro kind in record.")]
    UnsupportedAvroKind,
}

fn convert_avro_value(value: AvroValue) -> Result<Value, AvroError> {
    let v = match value {
        AvroValue::Null => Value::Extant,
        AvroValue::Boolean(p) => Value::BooleanValue(p),
        AvroValue::Int(n) => Value::Int32Value(n),
        AvroValue::Long(n) => Value::Int64Value(n),
        AvroValue::Float(x) => Value::Float64Value(x.into()),
        AvroValue::Double(x) => Value::Float64Value(x),
        AvroValue::Bytes(v) | AvroValue::Fixed(_, v) => Value::Data(Blob::from_vec(v)),
        AvroValue::String(s) => Value::Text(s.into()),
        AvroValue::Enum(_, name) => Value::of_attr(name),
        AvroValue::Union(_, v) => convert_avro_value(*v)?,
        AvroValue::Array(arr) => Value::record(
            arr.into_iter()
                .map(|v| convert_avro_value(v).map(Item::ValueItem))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AvroValue::Map(obj) => Value::record(
            obj.into_iter()
                .map(|(k, v)| {
                    convert_avro_value(v).map(move |v| Item::Slot(Value::Text(k.into()), v))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AvroValue::Record(obj) => Value::record(
            obj.into_iter()
                .map(|(k, v)| {
                    convert_avro_value(v).map(move |v| Item::Slot(Value::Text(k.into()), v))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AvroValue::Date(offset) => {
            let utc_dt = DateTime::from_timestamp(offset as i64 * 86400, 0)
                .unwrap_or(DateTime::<Utc>::MAX_UTC);
            let ts = Timestamp::from(utc_dt);
            ts.into_value()
        }
        AvroValue::Decimal(d) => {
            let n = BigInt::from(d);
            Value::BigInt(n)
        }
        AvroValue::TimeMillis(n) => Duration::from_millis(n as u64).into_value(),
        AvroValue::TimeMicros(n) => Duration::from_micros(n as u64).into_value(),
        AvroValue::TimestampMillis(offset) => {
            let utc_dt =
                DateTime::from_timestamp_millis(offset).unwrap_or(DateTime::<Utc>::MAX_UTC);
            let ts = Timestamp::from(utc_dt);
            ts.into_value()
        }
        AvroValue::TimestampMicros(offset) => {
            let utc_dt =
                DateTime::from_timestamp_micros(offset).unwrap_or(DateTime::<Utc>::MAX_UTC);
            let ts = Timestamp::from(utc_dt);
            ts.into_value()
        }
        AvroValue::LocalTimestampMillis(n) => {
            let def = if n <= 0 {
                NaiveDateTime::MIN
            } else {
                NaiveDateTime::MAX
            };
            let local_time = NaiveDateTime::UNIX_EPOCH
                .checked_add_signed(TimeDelta::milliseconds(n))
                .unwrap_or(def);
            let dt = local_time.and_local_timezone(Local).unwrap();
            let ts = Timestamp::from(dt);
            ts.into_value()
        }
        AvroValue::LocalTimestampMicros(n) => {
            let def = if n <= 0 {
                NaiveDateTime::MIN
            } else {
                NaiveDateTime::MAX
            };
            let local_time = NaiveDateTime::UNIX_EPOCH
                .checked_add_signed(TimeDelta::microseconds(n))
                .unwrap_or(def);
            let dt = local_time.and_local_timezone(Local).unwrap();
            let ts = Timestamp::from(dt);
            ts.into_value()
        }
        AvroValue::Duration(_) => return Err(AvroError::UnsupportedAvroKind),
        AvroValue::Uuid(id) => Value::BigInt(id.as_u128().into()),
    };
    Ok(v)
}

/// Interpret the bytes as Avro encoded data.
#[derive(Default, Clone, Debug)]
pub struct AvroDeserializer {
    schema: Option<Schema>,
}

impl AvroDeserializer {
    pub fn new(schema: Schema) -> Self {
        AvroDeserializer {
            schema: Some(schema),
        }
    }
}

#[derive(Default)]
enum ValueAcc {
    #[default]
    Empty,
    Single(Value),
    Record(Vec<Item>),
}

impl ValueAcc {
    fn push(&mut self, value: Value) {
        match std::mem::take(self) {
            ValueAcc::Empty => *self = ValueAcc::Single(value),
            ValueAcc::Single(v) => {
                *self = ValueAcc::Record(vec![Item::ValueItem(v), Item::ValueItem(value)])
            }
            ValueAcc::Record(mut vs) => {
                vs.push(Item::ValueItem(value));
                *self = ValueAcc::Record(vs);
            }
        }
    }

    fn done(self) -> Value {
        match self {
            ValueAcc::Empty => Value::Extant,
            ValueAcc::Single(v) => v,
            ValueAcc::Record(vs) => Value::record(vs),
        }
    }
}

impl MessageDeserializer for AvroDeserializer {
    type Error = AvroError;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let AvroDeserializer { schema } = self;
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Payload => message.payload(),
        };
        let cursor = Cursor::new(payload);
        let reader = if let Some(schema) = schema {
            apache_avro::Reader::with_schema(schema, cursor)
        } else {
            apache_avro::Reader::new(cursor)
        }?;
        let mut acc = ValueAcc::Empty;
        for avro_value in reader {
            let v = convert_avro_value(avro_value?)?;
            acc.push(v);
        }
        Ok(acc.done())
    }
}
