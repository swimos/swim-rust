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

use std::{array::TryFromSliceError, convert::Infallible, io::Cursor, time::Duration};

use chrono::{DateTime, Local, NaiveDateTime, TimeDelta, Utc};
use rdkafka::{message::BorrowedMessage, Message};
use swimos_form::Form;
use swimos_model::{BigInt, Blob, Item, Timestamp, Value};
use swimos_recon::parser::{parse_recognize, AsyncParseError};

pub trait MessageDeserializer {
    type Error: std::error::Error;

    fn deserialize<'a>(
        &'a self,
        message: &'a BorrowedMessage<'_>,
        part: MessagePart,
    ) -> Result<Value, Self::Error>;

    fn boxed(self) -> BoxMessageDeserializer
    where
        Self: Sized + Send + Sync + 'static,
        Self::Error: Send + 'static,
    {
        Box::new(BoxErrorDeserializer { inner: self })
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct StringDeserializer;
#[derive(Clone, Copy, Default, Debug)]
pub struct BytesDeserializer;
#[derive(Clone, Copy, Default, Debug)]
pub struct ReconDeserializer;

impl MessageDeserializer for StringDeserializer {
    type Error = std::str::Utf8Error;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key_view::<str>(),
            MessagePart::Value => message.payload_view::<str>(),
        };
        payload
            .transpose()
            .map(|opt| Value::text(opt.unwrap_or_default()))
    }
}

impl MessageDeserializer for BytesDeserializer {
    type Error = Infallible;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Value => message.payload(),
        };
        let bytes = payload.unwrap_or(&[]);
        Ok(Value::Data(Blob::from_vec(bytes.to_vec())))
    }
}

impl MessageDeserializer for ReconDeserializer {
    type Error = AsyncParseError;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key_view::<str>(),
            MessagePart::Value => message.payload_view::<str>(),
        };
        let payload_str = match payload.transpose() {
            Ok(string) => string.unwrap_or_default(),
            Err(err) => return Err(AsyncParseError::BadUtf8(err)),
        };
        parse_recognize::<Value>(payload_str, true).map_err(AsyncParseError::Parser)
    }
}

use serde_json::Value as JsonValue;

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
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Value => message.payload(),
        };
        let bytes = payload.unwrap_or(&[]);
        let v: serde_json::Value = serde_json::from_slice(bytes)?;
        Ok(convert_json_value(v))
    }
}

use apache_avro::{types::Value as AvroValue, Schema};
use thiserror::Error;

use crate::selector::DeserializationError;

use super::MessagePart;

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
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let AvroDeserializer { schema } = self;
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Value => message.payload(),
        };
        let cursor = Cursor::new(payload.unwrap_or_default());
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

#[derive(Clone, Copy, Default, Debug)]
pub enum Endianness {
    #[default]
    LittleEndian,
    BigEndian,
}

macro_rules! num_deser {
    ($deser:ident, $numt:ty, $variant:ident) => {
        #[derive(Clone, Copy, Default, Debug)]
        pub struct $deser(Endianness);

        impl $deser {
            pub fn new(endianness: Endianness) -> Self {
                Self(endianness)
            }
        }

        impl MessageDeserializer for $deser {
            type Error = TryFromSliceError;

            fn deserialize<'a>(
                &self,
                message: &'a BorrowedMessage<'a>,
                part: MessagePart,
            ) -> Result<Value, Self::Error> {
                let $deser(endianness) = self;
                let payload = match part {
                    MessagePart::Key => message.key(),
                    MessagePart::Value => message.payload(),
                }
                .unwrap_or_default();
                let x = match endianness {
                    Endianness::LittleEndian => <$numt>::from_le_bytes(payload.try_into()?),
                    Endianness::BigEndian => <$numt>::from_be_bytes(payload.try_into()?),
                };
                Ok(Value::$variant(x.into()))
            }
        }
    };
}

num_deser!(I32Deserializer, i32, Int32Value);
num_deser!(I64Deserializer, i64, Int64Value);
num_deser!(U32Deserializer, u32, UInt32Value);
num_deser!(U64Deserializer, u64, UInt64Value);
num_deser!(F64Deserializer, f64, Float64Value);
num_deser!(F32Deserializer, f32, Float64Value);

#[derive(Clone, Copy, Default, Debug)]
pub struct UuidDeserializer;

impl MessageDeserializer for UuidDeserializer {
    type Error = TryFromSliceError;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Value => message.payload(),
        }
        .unwrap_or_default();
        let x = u128::from_be_bytes(payload.try_into()?);
        Ok(Value::BigInt(x.into()))
    }
}

pub struct BoxErrorDeserializer<D> {
    inner: D,
}

impl<D: MessageDeserializer> MessageDeserializer for BoxErrorDeserializer<D>
where
    D::Error: Send + 'static,
{
    type Error = DeserializationError;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        self.inner
            .deserialize(message, part)
            .map_err(DeserializationError::new)
    }
}

pub type BoxMessageDeserializer =
    Box<dyn MessageDeserializer<Error = DeserializationError> + Send + Sync + 'static>;

impl MessageDeserializer for BoxMessageDeserializer {
    type Error = DeserializationError;

    fn deserialize<'a>(
        &self,
        message: &'a BorrowedMessage<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        (**self).deserialize(message, part)
    }

    fn boxed(self) -> BoxMessageDeserializer
    where
        Self: Sized + Send + Sync + 'static,
        Self::Error: Send + 'static,
    {
        self
    }
}
