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

#[cfg(feature = "avro")]
mod avro;
#[cfg(feature = "json")]
mod json;

#[cfg(test)]
pub mod tests;

#[cfg(feature = "json")]
pub use json::JsonDeserializer;

#[cfg(feature = "avro")]
pub use avro::AvroDeserializer;

use std::{array::TryFromSliceError, convert::Infallible};

use swimos_form::Form;
use swimos_model::{Blob, Value};
use swimos_recon::parser::{parse_recognize, AsyncParseError};

use uuid::Uuid;

use crate::error::DeserializationError;

/// An uninterpreted view of the components of a Kafka message.
pub struct MessageView<'a> {
    pub topic: &'a str,
    pub key: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> MessageView<'a> {
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn key_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.key)
    }

    pub fn payload_str(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.payload)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessagePart {
    Key,
    Payload,
}

/// A deserializer that will attempt to produce a [value](Value) from a component of a Kafka message.
pub trait MessageDeserializer {
    type Error: std::error::Error;

    fn deserialize<'a>(
        &'a self,
        message: &'a MessageView<'a>,
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

/// Interprets the bytes as a UTF8 string.
#[derive(Clone, Copy, Default, Debug)]
pub struct StringDeserializer;

/// Does not interpret the bytes at all.
#[derive(Clone, Copy, Default, Debug)]
pub struct BytesDeserializer;

/// Interpret the bytes as a UTF8 string, containing Recon.
#[derive(Clone, Copy, Default, Debug)]
pub struct ReconDeserializer;

impl MessageDeserializer for StringDeserializer {
    type Error = std::str::Utf8Error;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key_str(),
            MessagePart::Payload => message.payload_str(),
        };
        payload.map(Value::text)
    }
}

impl MessageDeserializer for BytesDeserializer {
    type Error = Infallible;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Payload => message.payload(),
        };
        Ok(Value::Data(Blob::from_vec(payload.to_vec())))
    }
}

impl MessageDeserializer for ReconDeserializer {
    type Error = AsyncParseError;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key_str(),
            MessagePart::Payload => message.payload_str(),
        };
        let payload_str = match payload {
            Ok(string) => string,
            Err(err) => return Err(AsyncParseError::BadUtf8(err)),
        };
        parse_recognize::<Value>(payload_str, true).map_err(AsyncParseError::Parser)
    }
}

/// Endianness for numeric deserializers.
#[derive(Clone, Copy, Default, Debug, Form, PartialEq, Eq)]
pub enum Endianness {
    LittleEndian,
    #[default]
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
                message: &'a MessageView<'a>,
                part: MessagePart,
            ) -> Result<Value, Self::Error> {
                let $deser(endianness) = self;
                let payload = match part {
                    MessagePart::Key => message.key(),
                    MessagePart::Payload => message.payload(),
                };
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

/// Interpret the bytes as a UUID.
#[derive(Clone, Copy, Default, Debug)]
pub struct UuidDeserializer;

impl MessageDeserializer for UuidDeserializer {
    type Error = TryFromSliceError;

    fn deserialize<'a>(
        &self,
        message: &'a MessageView<'a>,
        part: MessagePart,
    ) -> Result<Value, Self::Error> {
        let payload = match part {
            MessagePart::Key => message.key(),
            MessagePart::Payload => message.payload(),
        };
        let x = Uuid::from_bytes(payload.try_into()?);
        Ok(Value::BigInt(x.as_u128().into()))
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
        message: &'a MessageView<'a>,
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
        message: &'a MessageView<'a>,
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
