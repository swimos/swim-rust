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

use std::convert::Infallible;

use rdkafka::{message::BorrowedMessage, Message};
use swimos_model::{Blob, Item, Value};
use swimos_recon::parser::{parse_recognize, AsyncParseError, ParseError};

#[derive(Clone, Copy)]
pub enum MessagePart {
    Key,
    Value,
}

pub trait MessageDeserializer {

    type Error: std::error::Error;

    fn deserialize<'a>(&self, message: &'a BorrowedMessage<'a>, part: MessagePart) -> Result<Value, Self::Error>;

}

pub struct StringDeserializer;
pub struct BytesDeserializer;
pub struct ReconDeserializer;

impl MessageDeserializer for StringDeserializer {
    type Error = std::str::Utf8Error;

    fn deserialize<'a>(&self, message: &'a BorrowedMessage<'a>, part: MessagePart) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key_view::<str>(),
            MessagePart::Value => message.payload_view::<str>(),
        };
        payload.transpose().map(|opt| Value::text(opt.unwrap_or_default()))
    }
}

impl MessageDeserializer for BytesDeserializer {
    type Error = Infallible;

    fn deserialize<'a>(&self, message: &'a BorrowedMessage<'a>, part: MessagePart) -> Result<Value, Self::Error> {
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

    fn deserialize<'a>(&self, message: &'a BorrowedMessage<'a>, part: MessagePart) -> Result<Value, Self::Error> {
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

fn convert_json_value(input: serde_json::Value) -> Value {
    match input {
        serde_json::Value::Null => Value::Extant,
        serde_json::Value::Bool(p) => Value::BooleanValue(p),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_u64() {
                Value::UInt64Value(i)
            } else if let Some(i) = n.as_i64() {
                Value::Int64Value(i)
            } else {
                Value::Float64Value(n.as_f64().unwrap_or(f64::NAN))
            }
        },
        serde_json::Value::String(s) => Value::Text(s.into()),
        serde_json::Value::Array(arr) => {
            Value::from_vec(arr.into_iter().map(|v| Item::ValueItem(convert_json_value(v))).collect())
        },
        serde_json::Value::Object(obj) => {
            Value::from_vec(obj.into_iter().map(|(k, v)| Item::Slot(Value::Text(k.into()), convert_json_value(v))).collect())
        },
    }
}

pub struct JsonDeserializer;

impl MessageDeserializer for JsonDeserializer {
    type Error = serde_json::Error;

    fn deserialize<'a>(&self, message: &'a BorrowedMessage<'a>, part: MessagePart) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Value => message.payload(),
        };
        let bytes = payload.unwrap_or(&[]);
        let v: serde_json::Value = serde_json::from_slice(bytes)?;
        Ok(convert_json_value(v))
    }
}