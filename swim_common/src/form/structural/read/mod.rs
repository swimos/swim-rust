// Copyright 2015-2021 SWIM.AI inc.
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

pub mod builder;
pub mod materializers;
#[cfg(test)]
mod tests;

use crate::form::structural::read::builder::{NoAttributes, Wrapped};
use crate::form::structural::write::StructuralWritable;
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Value, ValueKind};
use either::Either;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::sync::Arc;

mod error;

use crate::form::structural::bridge::ReadWriteBridge;
use crate::form::structural::read::materializers::value::ValueMaterializer;
pub use error::ReadError;

/// Trait for types that can be structurally deserialized, from the Swim data model.
pub trait StructuralReadable: ValueReadable {
    type Reader: HeaderReader;

    /// Create a record reader from which the value can be constructed from attributes, values and slots.
    /// Typically, types that are represented by a single value will return an error if this method is
    /// called.
    fn record_reader() -> Result<Self::Reader, ReadError>;

    /// Attempt to create the complete deserialized object from the record reader. If the record is yet
    /// incomplete this will return an error.
    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError>;

    /// Optionally provide a default value for absent fields.
    fn on_absent() -> Option<Self> {
        None
    }

    /// Attempt to write a value of a ['StructuralWritable'] type into an instance of this type.
    fn try_read_from<T: StructuralWritable>(writable: &T) -> Result<Self, ReadError> {
        let bridge: ReadWriteBridge<Self> = Default::default();
        writable.write_with(bridge)
    }

    /// Attempt to transform a value of a ['StructuralWritable'] type into an instance of this type.
    fn try_transform<T: StructuralWritable>(writable: T) -> Result<Self, ReadError> {
        let bridge: ReadWriteBridge<Self> = Default::default();
        writable.write_into(bridge)
    }
}

/// Trait for types that can (potentially) be deserialized from a single primitive value.
/// Types that are represented as complex records will typeically return an error
/// for all methods in this trait.
pub trait ValueReadable: Sized {
    fn read_extant() -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Extant))
    }
    fn read_i32(_value: i32) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Int32))
    }
    fn read_i64(_value: i64) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Int64))
    }
    fn read_u32(_value: u32) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::UInt32))
    }
    fn read_u64(_value: u64) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::UInt64))
    }
    fn read_f64(_value: f64) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Float64))
    }
    fn read_bool(_value: bool) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Boolean))
    }
    fn read_big_int(_value: BigInt) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::BigInt))
    }
    fn read_big_uint(_value: BigUint) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::BigUint))
    }
    fn read_text(_value: Cow<'_, str>) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Text))
    }
    fn read_blob(_value: Vec<u8>) -> Result<Self, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Data))
    }
}

/// Trait for readers that will deserialize a value from a record with attributes.
pub trait HeaderReader: Sized {
    type Body: BodyReader;
    type Delegate: BodyReader;

    /// Start reading an attribute.
    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError>;

    /// Read an attribute with no value.
    fn push_attr(self, name: Cow<'_, str>) -> Result<Self, ReadError> {
        let mut reader = self.read_attribute(name)?;
        reader.push_extant()?;
        Self::restore(reader)
    }

    /// Complete an attribute and continue reading the record.
    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError>;

    /// Start reading the body of the record (no further attributes can be read).
    fn start_body(self) -> Result<Self::Body, ReadError>;
}

/// Trait for readers that will deserialize a value from a record. To read a slot,
/// first call the relevant push methods to read the key of the slot, then call
/// `start_slot` and then repeat for the value of the slot.
pub trait BodyReader: Sized {
    type Delegate: HeaderReader;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Extant))
    }
    fn push_i32(&mut self, _value: i32) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Int32))
    }
    fn push_i64(&mut self, _value: i64) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Int64))
    }
    fn push_u32(&mut self, _value: u32) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::UInt32))
    }
    fn push_u64(&mut self, _value: u64) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::UInt64))
    }
    fn push_f64(&mut self, _value: f64) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Float64))
    }
    fn push_bool(&mut self, _value: bool) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Boolean))
    }
    fn push_big_int(&mut self, _value: BigInt) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::BigInt))
    }
    fn push_big_uint(&mut self, _value: BigUint) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::BigUint))
    }
    fn push_text(&mut self, _value: Cow<'_, str>) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Text))
    }
    fn push_blob(&mut self, _value: Vec<u8>) -> Result<bool, ReadError> {
        Err(ReadError::UnexpectedKind(ValueKind::Data))
    }

    /// Treat the last pushed value as the key of a slot; the next pushed value will
    /// ne treated as the value of the slot.
    fn start_slot(&mut self) -> Result<(), ReadError> {
        Err(ReadError::UnexpectedSlot)
    }

    /// Push a nested record.
    fn push_record(self) -> Result<Self::Delegate, ReadError>;

    /// Complete a nested record and continue reading this record.
    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError>;
}

/// Empty type for cases that can never ocurr (reading a primitive as a record for example).
pub enum Never {}

impl Never {
    /// Witnesses that an instance of [Never] cannot exist.
    fn explode(&self) -> ! {
        use std::hint;
        // Safe as Never has no instances.
        unsafe { hint::unreachable_unchecked() }
    }
}

impl HeaderReader for Never {
    type Body = Self;
    type Delegate = Self;

    fn read_attribute(self, _: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        self.explode()
    }

    fn restore(reader: Self::Delegate) -> Result<Self, ReadError> {
        reader.explode()
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        self.explode()
    }
}

impl BodyReader for Never {
    type Delegate = Never;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_i32(&mut self, _: i32) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_i64(&mut self, _: i64) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_u32(&mut self, _: u32) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_u64(&mut self, _: u64) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_f64(&mut self, _: f64) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_bool(&mut self, _: bool) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_big_int(&mut self, _: BigInt) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_big_uint(&mut self, _: BigUint) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_text(&mut self, _: Cow<'_, str>) -> Result<bool, ReadError> {
        self.explode()
    }

    fn push_blob(&mut self, _: Vec<u8>) -> Result<bool, ReadError> {
        self.explode()
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        self.explode()
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        self.explode()
    }

    fn restore(reader: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        reader.explode()
    }
}

impl ValueReadable for () {
    fn read_extant() -> Result<Self, ReadError> {
        Ok(())
    }
}

macro_rules! record_forbidden {
    ($target:ty) => {
        impl StructuralReadable for $target {
            type Reader = Never;

            fn record_reader() -> Result<Self::Reader, ReadError> {
                Err(ReadError::UnexpectedKind(ValueKind::Record))
            }

            fn try_terminate(_: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
                Err(ReadError::UnexpectedKind(ValueKind::Record))
            }
        }
    };
}

record_forbidden!(());

macro_rules! primitive_readable {
    ($target:ty, $read:ident) => {
        impl ValueReadable for $target {
            fn $read(value: $target) -> Result<Self, ReadError> {
                Ok(value)
            }
        }

        record_forbidden!($target);
    };
}

primitive_readable!(i32, read_i32);
primitive_readable!(i64, read_i64);
primitive_readable!(u32, read_u32);
primitive_readable!(u64, read_u64);
primitive_readable!(bool, read_bool);
primitive_readable!(f64, read_f64);
primitive_readable!(BigInt, read_big_int);
primitive_readable!(BigUint, read_big_uint);
primitive_readable!(Vec<u8>, read_blob);

impl ValueReadable for Text {
    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        Ok(value.into())
    }
}

record_forbidden!(Text);

impl ValueReadable for String {
    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        let text: Text = value.into();
        Ok(text.into())
    }
}

record_forbidden!(String);

impl ValueReadable for Blob {
    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        Ok(Blob::from_vec(value))
    }
}

record_forbidden!(Blob);

impl ValueReadable for Box<[u8]> {
    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        Ok(value.into_boxed_slice())
    }
}

record_forbidden!(Box<[u8]>);

impl ValueReadable for Value {
    fn read_extant() -> Result<Self, ReadError> {
        Ok(Value::Extant)
    }

    fn read_i32(value: i32) -> Result<Self, ReadError> {
        Ok(Value::Int32Value(value))
    }

    fn read_i64(value: i64) -> Result<Self, ReadError> {
        Ok(Value::Int64Value(value))
    }

    fn read_u32(value: u32) -> Result<Self, ReadError> {
        Ok(Value::UInt32Value(value))
    }

    fn read_u64(value: u64) -> Result<Self, ReadError> {
        Ok(Value::UInt64Value(value))
    }

    fn read_f64(value: f64) -> Result<Self, ReadError> {
        Ok(Value::Float64Value(value))
    }

    fn read_bool(value: bool) -> Result<Self, ReadError> {
        Ok(Value::BooleanValue(value))
    }

    fn read_big_int(value: BigInt) -> Result<Self, ReadError> {
        Ok(Value::BigInt(value))
    }

    fn read_big_uint(value: BigUint) -> Result<Self, ReadError> {
        Ok(Value::BigUint(value))
    }

    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        Ok(Value::Text(value.into()))
    }

    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        Ok(Value::Data(Blob::from_vec(value)))
    }
}

impl<T: ValueReadable> ValueReadable for Arc<T> {
    fn read_extant() -> Result<Self, ReadError> {
        T::read_extant().map(Arc::new)
    }

    fn read_i32(value: i32) -> Result<Self, ReadError> {
        T::read_i32(value).map(Arc::new)
    }

    fn read_i64(value: i64) -> Result<Self, ReadError> {
        T::read_i64(value).map(Arc::new)
    }

    fn read_u32(value: u32) -> Result<Self, ReadError> {
        T::read_u32(value).map(Arc::new)
    }

    fn read_u64(value: u64) -> Result<Self, ReadError> {
        T::read_u64(value).map(Arc::new)
    }

    fn read_f64(value: f64) -> Result<Self, ReadError> {
        T::read_f64(value).map(Arc::new)
    }

    fn read_bool(value: bool) -> Result<Self, ReadError> {
        T::read_bool(value).map(Arc::new)
    }

    fn read_big_int(value: BigInt) -> Result<Self, ReadError> {
        T::read_big_int(value).map(Arc::new)
    }

    fn read_big_uint(value: BigUint) -> Result<Self, ReadError> {
        T::read_big_uint(value).map(Arc::new)
    }

    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        T::read_text(value).map(Arc::new)
    }

    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        T::read_blob(value).map(Arc::new)
    }
}

impl<T: StructuralReadable> StructuralReadable for Arc<T> {
    type Reader = T::Reader;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        T::record_reader()
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        T::try_terminate(reader).map(Arc::new)
    }
}

impl<L, R> HeaderReader for Either<L, R>
where
    L: HeaderReader,
    R: HeaderReader,
{
    type Body = Either<L::Body, R::Body>;
    type Delegate = Either<L::Delegate, R::Delegate>;

    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        match self {
            Either::Left(h) => Ok(Either::Left(h.read_attribute(name)?)),
            Either::Right(h) => Ok(Either::Right(h.read_attribute(name)?)),
        }
    }

    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError> {
        match delegate {
            Either::Left(r) => Ok(Either::Left(L::restore(r)?)),
            Either::Right(r) => Ok(Either::Right(R::restore(r)?)),
        }
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        match self {
            Either::Left(h) => Ok(Either::Left(h.start_body()?)),
            Either::Right(h) => Ok(Either::Right(h.start_body()?)),
        }
    }
}

impl<L, R> BodyReader for Either<L, R>
where
    L: BodyReader,
    R: BodyReader,
{
    type Delegate = Either<L::Delegate, R::Delegate>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_extant(),
            Either::Right(r) => r.push_extant(),
        }
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_i32(value),
            Either::Right(r) => r.push_i32(value),
        }
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_i64(value),
            Either::Right(r) => r.push_i64(value),
        }
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_u32(value),
            Either::Right(r) => r.push_u32(value),
        }
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_u64(value),
            Either::Right(r) => r.push_u64(value),
        }
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_f64(value),
            Either::Right(r) => r.push_f64(value),
        }
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_bool(value),
            Either::Right(r) => r.push_bool(value),
        }
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_big_int(value),
            Either::Right(r) => r.push_big_int(value),
        }
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_big_uint(value),
            Either::Right(r) => r.push_big_uint(value),
        }
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_text(value),
            Either::Right(r) => r.push_text(value),
        }
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        match self {
            Either::Left(r) => r.push_blob(value),
            Either::Right(r) => r.push_blob(value),
        }
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        match self {
            Either::Left(r) => r.start_slot(),
            Either::Right(r) => r.start_slot(),
        }
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        match self {
            Either::Left(r) => Ok(Either::Left(r.push_record()?)),
            Either::Right(r) => Ok(Either::Right(r.push_record()?)),
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            Either::Left(d) => Ok(Either::Left(L::restore(d)?)),
            Either::Right(d) => Ok(Either::Right(R::restore(d)?)),
        }
    }
}

impl<T> ValueReadable for Option<T>
where
    T: ValueReadable,
{
    fn read_extant() -> Result<Self, ReadError> {
        Ok(T::read_extant().map(Some).unwrap_or(None))
    }

    fn read_i32(value: i32) -> Result<Self, ReadError> {
        T::read_i32(value).map(Some)
    }

    fn read_i64(value: i64) -> Result<Self, ReadError> {
        T::read_i64(value).map(Some)
    }

    fn read_u32(value: u32) -> Result<Self, ReadError> {
        T::read_u32(value).map(Some)
    }

    fn read_u64(value: u64) -> Result<Self, ReadError> {
        T::read_u64(value).map(Some)
    }

    fn read_f64(value: f64) -> Result<Self, ReadError> {
        T::read_f64(value).map(Some)
    }

    fn read_bool(value: bool) -> Result<Self, ReadError> {
        T::read_bool(value).map(Some)
    }

    fn read_big_int(value: BigInt) -> Result<Self, ReadError> {
        T::read_big_int(value).map(Some)
    }

    fn read_big_uint(value: BigUint) -> Result<Self, ReadError> {
        T::read_big_uint(value).map(Some)
    }

    fn read_text(value: Cow<'_, str>) -> Result<Self, ReadError> {
        T::read_text(value).map(Some)
    }

    fn read_blob(value: Vec<u8>) -> Result<Self, ReadError> {
        T::read_blob(value).map(Some)
    }
}

impl<T> StructuralReadable for Option<T>
where
    T: StructuralReadable,
{
    type Reader = T::Reader;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        T::record_reader()
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        T::try_terminate(reader).map(Some)
    }

    fn on_absent() -> Option<Self> {
        Some(None)
    }
}

impl<T> ValueReadable for Vec<T> where T: ValueReadable {}

pub struct VecReader<T>(Vec<T>);

impl<T> Default for VecReader<T> {
    fn default() -> Self {
        VecReader(Vec::new())
    }
}

impl<T> StructuralReadable for Vec<T>
where
    T: StructuralReadable,
{
    type Reader = VecReader<T>;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        Ok(Default::default())
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        let VecReader(vec) = reader;
        Ok(vec)
    }
}

impl<T> NoAttributes for VecReader<T> where T: StructuralReadable {}

impl<T> BodyReader for VecReader<T>
where
    T: StructuralReadable,
{
    type Delegate = Wrapped<Self, T::Reader>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_extant()?);
        Ok(true)
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_i32(value)?);
        Ok(true)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_i64(value)?);
        Ok(true)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_u32(value)?);
        Ok(true)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_u64(value)?);
        Ok(true)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_f64(value)?);
        Ok(true)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_bool(value)?);
        Ok(true)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_big_int(value)?);
        Ok(true)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_big_uint(value)?);
        Ok(true)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_text(value)?);
        Ok(true)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        let VecReader(vec) = self;
        vec.push(T::read_blob(value)?);
        Ok(true)
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        Ok(Wrapped {
            payload: self,
            reader: T::record_reader()?,
        })
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        let Wrapped {
            mut payload,
            reader,
        } = delegate;
        let VecReader(vec) = &mut payload;
        vec.push(T::try_terminate(reader)?);
        Ok(payload)
    }
}

impl StructuralReadable for Value {
    type Reader = ValueMaterializer;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        Ok(ValueMaterializer::default())
    }

    fn try_terminate(reader: ValueMaterializer) -> Result<Self, ReadError> {
        Value::try_from(reader)
    }
}
