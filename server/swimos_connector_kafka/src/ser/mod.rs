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
use std::{fmt::Write, sync::Arc};
use swimos_model::{Item, Value, ValueKind};
use swimos_recon::print_recon_compact;

#[cfg(feature = "avro")]
mod avro;

#[cfg(feature = "avro")]
pub use avro::AvroSerializer;

#[cfg(feature = "json")]
mod json;

#[cfg(feature = "json")]
pub use json::JsonSerializer;

use crate::{Endianness, SerializationError};

/// A serializer that will attempt to produce a component of a Kafka message from a [value](Value).
pub trait MessageSerializer {
    /// Attempt to serialize the value to a buffer.
    ///
    /// # Arguments
    /// * `message` - The value to serialize.
    /// * `target` - The buffer into which to write the serialized data.
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError>;

    /// Wrap this serializer to be used via dynamic dispatch and shared between threads.
    fn shared(self) -> SharedMessageSerializer
    where
        Self: Sized + Send + Sync + 'static,
    {
        Arc::new(self)
    }
}

/// A serializer that will only write out UTF8 strings.
#[derive(Debug, Default, Clone, Copy)]
pub struct StringSerializer;

/// A serializer that writes out Recon strings as UTF8.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconSerializer;

/// A serializer that will only write out raw bytes data.
#[derive(Debug, Default, Clone, Copy)]
pub struct BytesSerializer;

impl MessageSerializer for StringSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Value::Text(text) = message {
            let bytes = text.as_bytes();
            target.reserve(bytes.len());
            target.put_slice(bytes);
            Ok(())
        } else {
            Err(SerializationError::InvalidKind(message.kind()))
        }
    }
}

const RESERVE_INIT: usize = 256;
const RESERVE_MULT: usize = 2;

impl MessageSerializer for ReconSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        let mut next_res = RESERVE_INIT.max(target.remaining_mut().saturating_mul(RESERVE_MULT));
        let body_offset = target.remaining();
        loop {
            if write!(target, "{}", print_recon_compact(message)).is_err() {
                target.truncate(body_offset);
                target.reserve(next_res);
                next_res = next_res.saturating_mul(RESERVE_MULT);
            } else {
                break Ok(());
            }
        }
    }
}

impl MessageSerializer for BytesSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Value::Data(blob) = message {
            let bytes = blob.as_ref();
            target.reserve(bytes.len());
            target.put_slice(bytes);
            Ok(())
        } else {
            Err(SerializationError::InvalidKind(message.kind()))
        }
    }
}

/// A serializer that will only write 32-bit signed integers.
#[derive(Clone, Copy, Default, Debug)]
pub struct I32Serializer(Endianness);

impl I32Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

/// A serializer that will only write 64-bit signed integers.
#[derive(Clone, Copy, Default, Debug)]
pub struct I64Serializer(Endianness);

impl I64Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

/// A serializer that will only write 32-bit unsigned integers.
#[derive(Clone, Copy, Default, Debug)]
pub struct U32Serializer(Endianness);

impl U32Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

/// A serializer that will only write 64-bit unsigned integers.
#[derive(Clone, Copy, Default, Debug)]
pub struct U64Serializer(Endianness);

impl U64Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

/// A serializer that will only write 32-bit floating point numbers.
#[derive(Clone, Copy, Default, Debug)]
pub struct F32Serializer(Endianness);

impl F32Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

/// A serializer that will only write 64-bit floating point numbers.
#[derive(Clone, Copy, Default, Debug)]
pub struct F64Serializer(Endianness);

impl F64Serializer {
    pub fn new(endianness: Endianness) -> Self {
        Self(endianness)
    }
}

impl MessageSerializer for I32Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Some(n) = match message {
            Value::Int32Value(n) => Some(*n),
            Value::Int64Value(n) => i32::try_from(*n).ok(),
            Value::UInt32Value(n) => i32::try_from(*n).ok(),
            Value::UInt64Value(n) => i32::try_from(*n).ok(),
            Value::BigInt(n) => i32::try_from(n).ok(),
            Value::BigUint(n) => i32::try_from(n).ok(),
            ow => return Err(SerializationError::InvalidKind(ow.kind())),
        } {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<i32>());
            match endianness {
                Endianness::LittleEndian => target.put_i32_le(n),
                Endianness::BigEndian => target.put_i32(n),
            }
            Ok(())
        } else {
            Err(SerializationError::IntegerOutOfRange(message.clone()))
        }
    }
}

impl MessageSerializer for I64Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Some(n) = match message {
            Value::Int32Value(n) => Some(*n as i64),
            Value::Int64Value(n) => Some(*n),
            Value::UInt32Value(n) => Some(*n as i64),
            Value::UInt64Value(n) => i64::try_from(*n).ok(),
            Value::BigInt(n) => i64::try_from(n).ok(),
            Value::BigUint(n) => i64::try_from(n).ok(),
            ow => return Err(SerializationError::InvalidKind(ow.kind())),
        } {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<i64>());
            match endianness {
                Endianness::LittleEndian => target.put_i64_le(n),
                Endianness::BigEndian => target.put_i64(n),
            }
            Ok(())
        } else {
            Err(SerializationError::IntegerOutOfRange(message.clone()))
        }
    }
}

impl MessageSerializer for U32Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Some(n) = match message {
            Value::Int32Value(n) => u32::try_from(*n).ok(),
            Value::Int64Value(n) => u32::try_from(*n).ok(),
            Value::UInt32Value(n) => Some(*n),
            Value::UInt64Value(n) => u32::try_from(*n).ok(),
            Value::BigInt(n) => u32::try_from(n).ok(),
            Value::BigUint(n) => u32::try_from(n).ok(),
            ow => return Err(SerializationError::InvalidKind(ow.kind())),
        } {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<u32>());
            match endianness {
                Endianness::LittleEndian => target.put_u32_le(n),
                Endianness::BigEndian => target.put_u32(n),
            }
            Ok(())
        } else {
            Err(SerializationError::IntegerOutOfRange(message.clone()))
        }
    }
}

impl MessageSerializer for U64Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Some(n) = match message {
            Value::Int32Value(n) => u64::try_from(*n).ok(),
            Value::Int64Value(n) => u64::try_from(*n).ok(),
            Value::UInt32Value(n) => Some(*n as u64),
            Value::UInt64Value(n) => Some(*n),
            Value::BigInt(n) => u64::try_from(n).ok(),
            Value::BigUint(n) => u64::try_from(n).ok(),
            ow => return Err(SerializationError::InvalidKind(ow.kind())),
        } {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<u64>());
            match endianness {
                Endianness::LittleEndian => target.put_u64_le(n),
                Endianness::BigEndian => target.put_u64(n),
            }
            Ok(())
        } else {
            Err(SerializationError::IntegerOutOfRange(message.clone()))
        }
    }
}

impl MessageSerializer for F32Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Value::Float64Value(x) = message {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<f32>());
            match endianness {
                Endianness::LittleEndian => target.put_f32_le(*x as f32),
                Endianness::BigEndian => target.put_f32(*x as f32),
            }
            Ok(())
        } else {
            Err(SerializationError::InvalidKind(message.kind()))
        }
    }
}

impl MessageSerializer for F64Serializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Value::Float64Value(x) = message {
            let Self(endianness) = self;
            target.reserve(std::mem::size_of::<f64>());
            match endianness {
                Endianness::LittleEndian => target.put_f64_le(*x),
                Endianness::BigEndian => target.put_f64(*x),
            }
            Ok(())
        } else {
            Err(SerializationError::InvalidKind(message.kind()))
        }
    }
}

/// A serializer that will only write big integers as UUIDs.
#[derive(Clone, Copy, Default, Debug)]
pub struct UuidSerializer;

impl MessageSerializer for UuidSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        if let Some(n) = match message {
            Value::BigInt(n) => u128::try_from(n).ok(),
            Value::BigUint(n) => u128::try_from(n).ok(),
            ow => return Err(SerializationError::InvalidKind(ow.kind())),
        } {
            target.reserve(std::mem::size_of::<u128>());
            target.put_u128(n);
            Ok(())
        } else {
            Err(SerializationError::IntegerOutOfRange(message.clone()))
        }
    }
}

#[cfg(any(feature = "json", feature = "avro"))]
fn is_array(items: &[Item]) -> bool {
    use swimos_model::Item;

    items.iter().all(|item| matches!(item, Item::ValueItem(_)))
}

#[cfg(any(feature = "json", feature = "avro"))]
fn is_record(items: &[Item]) -> bool {
    items
        .iter()
        .all(|item| matches!(item, Item::Slot(key, _) if key.kind() == ValueKind::Text))
}

/// A message serializer wrapped in an [`Arc`] to share between threads, called via dynamic dispatch.
pub type SharedMessageSerializer = Arc<dyn MessageSerializer + Send + Sync + 'static>;

impl MessageSerializer for SharedMessageSerializer {
    fn serialize(&self, message: &Value, target: &mut BytesMut) -> Result<(), SerializationError> {
        (**self).serialize(message, target)
    }

    fn shared(self) -> SharedMessageSerializer
    where
        Self: Sized + Send + Sync + 'static,
    {
        self
    }
}
