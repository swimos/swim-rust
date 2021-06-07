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

pub mod interpreters;
#[cfg(test)]
mod tests;

use crate::form::structural::write::interpreters::value::ValueInterpreter;
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Attr, Item, Value};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hint;
use std::rc::Rc;
use std::sync::Arc;

/// Trait for types that can describe their structure using a [`StructuralWriter`].
/// Each writer is an interpreter which could, for example, realize the structure
/// as a [`Value`] or format it is a Recon string.
pub trait StructuralWritable: Sized {
    /// Write he strucutre of this value using the provided interpreter.
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error>;

    /// Write he strucutre of this value using the provided interpreter, allowing
    /// the interpreter to consume this value if needed.
    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error>;

    /// Write the structure of this value with an interpreter that cannnot generate an error.
    fn write_with_infallible<W: StructuralWriter<Error = Infallible>>(&self, writer: W) -> W::Repr {
        if let Ok(repr) = self.write_with(writer) {
            repr
        } else {
            // This is safe as Infallible has no instances.
            unsafe { hint::unreachable_unchecked() }
        }
    }

    /// Write the structure of this value with an interpreter that cannnot generate an error,
    /// allowing in t interpreter to consume this value if needed.
    fn write_into_infallible<W: StructuralWriter<Error = Infallible>>(self, writer: W) -> W::Repr {
        if let Ok(repr) = self.write_into(writer) {
            repr
        } else {
            // This is safe as Infallible has no instances.
            unsafe { hint::unreachable_unchecked() }
        }
    }

    /// Create a [`Value`] based on the strucuture of this value.
    fn structure(&self) -> Value {
        self.write_with_infallible(ValueInterpreter::default())
    }

    /// Covert this value into a [`Value`].
    fn into_structure(self) -> Value {
        self.write_into_infallible(ValueInterpreter::default())
    }

    /// If this value ocurrs as the field of a compound object, determine whehter that field
    /// can be omitted (for example the `None` case of [`Option`]). This is inteded to be
    /// used in the derive macro for this trait and should be generally need to be used.
    fn omit_as_field(&self) -> bool {
        false
    }
}

/// Base trait for strucutral writers that allow for a single, primitive value to be written.
pub trait PrimitiveWriter: Sized {
    /// The result type of the writer.
    type Repr;
    /// The type of errors that the writer can generate.
    type Error;

    fn write_extant(self) -> Result<Self::Repr, Self::Error>;
    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error>;
    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error>;
    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error>;
    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error>;
    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error>;
    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error>;
    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error>;
    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error>;
    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error>;
    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error>;
    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error>;
}

/// Interpreter into which the strucutre of a value can be described, producing either
/// some some result or an error. The canonical implementation is [`ValueInterpreter`]
/// which will realize the structure as a [`Value`] tree.
pub trait StructuralWriter: PrimitiveWriter {
    /// Type that will consume the attributes of the structure.
    type Header: HeaderWriter<Repr = Self::Repr, Body = Self::Body, Error = Self::Error>;
    /// Type that will consume the items of the structure.
    type Body: BodyWriter<Repr = Self::Repr, Error = Self::Error>;

    /// Describe a compound type.
    fn record(self, num_attrs: usize) -> Result<Self::Header, Self::Error>;
}

/// Convenience trait for variable string related conversions.
pub trait Label: Into<String> + Into<Text> + AsRef<str> {
    fn as_cow(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_ref())
    }
}

impl<T: Into<String> + Into<Text> + AsRef<str>> Label for T {}

/// Describing the structure of a record proceeds in two stages, first describing
/// the attributes in the header and then listing the items in the body (consiting
/// of either simple values or slot fields). This is used to describe the attributes.
pub trait HeaderWriter: Sized {
    /// The result type of the writer.
    type Repr;
    /// The type of errors that the writer can generate.
    type Error;
    /// The type into which this will transform when writing switches from attributes to items.
    type Body: BodyWriter;

    /// Write an attribute into the header.
    /// #Arguments
    /// * `name` - The name of the attribute.
    /// * `value` - The value whose strucuture will be used for the value of the attribute.
    fn write_attr<V: StructuralWritable>(
        self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error>;

    /// Delegate the remainder of the process to another value (its attributes will be appended
    /// to those already described).
    /// #Arguments
    /// * `value` - The value whose strucuture will be used for the remainder of the process.
    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error>;

    /// Write an attribute into the header, consuming the value.
    /// #Arguments
    /// * `name` - The name of the attribute.
    /// * `value` - The value whose strucuture will be used for the value of the attribute.
    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error>;

    /// Delegate the remainder of the process to another value (its attributes will be appended
    /// to those already described), consuming it.
    /// #Arguments
    /// * `value` - The value whose strucuture will be used for the remainder of the process.
    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error>;

    fn write_extant_attr<L: Label>(self, name: L) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &())
    }
    fn write_i32_attr<L: Label>(self, name: L, value: i32) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_i64_attr<L: Label>(self, name: L, value: i64) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_u32_attr<L: Label>(self, name: L, value: u32) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_u64_attr<L: Label>(self, name: L, value: u64) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_f64_attr<L: Label>(self, name: L, value: f64) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_bool_attr<L: Label>(self, name: L, value: bool) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_big_int_attr<L: Label>(self, name: L, value: BigInt) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_big_uint_attr<L: Label>(self, name: L, value: BigUint) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    fn write_text_attr<L: Label, T: Label>(self, name: L, value: T) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value.as_ref())
    }
    fn write_blob_attr<L: Label>(self, name: L, value: &[u8]) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }
    /// Transform this writer into another which can be used to describe the items.
    ///
    /// #Arguments
    /// * `kind` - Description of the contents of the body. If an incorrect value is provided,
    /// implementations may return an error but should not panic.
    /// * `num_items` - The number of items in the record. If an incorrect number is provided,
    /// implementations may return an error but should not panic.
    fn complete_header(
        self,
        kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error>;
}

/// Description of the overall format of a record body which writers may use to generate a
/// more optimal representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordBodyKind {
    /// The record contains only value items and no slots.
    ArrayLike,
    /// The record contains only slots and no value items.
    MapLike,
    /// The record may (or may not) contain a mixture of any items. This is valid for any record.
    Mixed,
}

impl RecordBodyKind {
    /// Determine the record body type by iterating a sequence of items.
    fn of_iter<'a, It>(it: It) -> Option<RecordBodyKind>
    where
        It: Iterator<Item = &'a Item>,
    {
        let mut kind = None;
        for item in it {
            match (&kind, item) {
                (Some(RecordBodyKind::ArrayLike), Item::ValueItem(_))
                | (Some(RecordBodyKind::MapLike), Item::Slot(_, _)) => {}
                (None, Item::ValueItem(_)) => {
                    kind = Some(RecordBodyKind::ArrayLike);
                }
                (None, Item::Slot(_, _)) => {
                    kind = Some(RecordBodyKind::MapLike);
                }
                _ => {
                    return Some(RecordBodyKind::Mixed);
                }
            }
        }
        kind
    }
}

/// Describing the structure of a record proceeds in two stages, first describing
/// the attributes in the header and then listing the items in the body (consiting
/// of either simple values or slot fields). This is used to describe the items.
pub trait BodyWriter: Sized {
    /// The result type of the writer.
    type Repr;
    /// The type of errors that the writer can generate.
    type Error;

    fn write_extant(self) -> Result<Self, Self::Error> {
        self.write_value(&())
    }
    fn write_extant_slot<L: Label>(self, name: L) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &())
    }

    fn write_i32(self, value: i32) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_i32_slot<L: Label>(self, name: L, value: i32) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_i64(self, value: i64) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_i64_slot<L: Label>(self, name: L, value: i64) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_u32(self, value: u32) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_u32_slot<L: Label>(self, name: L, value: u32) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_u64(self, value: u64) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_u64_slot<L: Label>(self, name: L, value: u64) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_f64(self, value: f64) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_f64_slot<L: Label>(self, name: L, value: f64) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_bool(self, value: bool) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_bool_slot<L: Label>(self, name: L, value: bool) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_big_int_slot<L: Label>(self, name: L, value: BigInt) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_big_unit(self, value: BigUint) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_big_uint_slot<L: Label>(self, name: L, value: BigUint) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self, Self::Error> {
        self.write_value(&value.as_ref())
    }
    fn write_text_slot<L: Label, T: Label>(self, name: L, value: T) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value.as_ref())
    }

    fn write_blob(self, value: &[u8]) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }
    fn write_blob_slot<L: Label>(self, name: L, value: &[u8]) -> Result<Self, Self::Error> {
        self.write_slot(&name.as_ref(), &value)
    }

    fn write_value<V: StructuralWritable>(self, value: &V) -> Result<Self, Self::Error>;
    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error>;

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error>;
    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error>;

    /// Finish describing the record and attempt to produce the final result.
    fn done(self) -> Result<Self::Repr, Self::Error>;
}

impl StructuralWritable for () {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_extant()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_extant()
    }
}

impl StructuralWritable for i32 {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_i32(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_i32(self)
    }
}

impl StructuralWritable for i64 {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_i64(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_i64(self)
    }
}

impl StructuralWritable for u32 {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_u32(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_u32(self)
    }
}

impl StructuralWritable for u64 {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_u64(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_u64(self)
    }
}

impl StructuralWritable for f64 {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_f64(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_f64(self)
    }
}

impl StructuralWritable for bool {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_bool(*self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_bool(self)
    }
}

impl StructuralWritable for BigInt {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_big_int(self.clone())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_big_int(self)
    }
}

impl StructuralWritable for BigUint {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_big_uint(self.clone())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_big_uint(self)
    }
}

impl StructuralWritable for String {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(Text::from(self))
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(self)
    }
}

impl<'a> StructuralWritable for &'a str {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(Text::from(*self))
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(self)
    }
}

impl StructuralWritable for Text {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(self.clone())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_text(self)
    }
}

impl<T: StructuralWritable> StructuralWritable for &T {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        (*self).write_with(writer)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

impl<T: StructuralWritable> StructuralWritable for &mut T {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        (**self).write_with(writer)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

impl<T: StructuralWritable> StructuralWritable for Arc<T> {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        (**self).write_with(writer)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match Arc::try_unwrap(self) {
            Ok(inner) => inner.write_into(writer),
            Err(outer) => outer.write_with(writer),
        }
    }
}

impl<T: StructuralWritable> StructuralWritable for Rc<T> {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        (**self).write_with(writer)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match Rc::try_unwrap(self) {
            Ok(inner) => inner.write_into(writer),
            Err(outer) => outer.write_with(writer),
        }
    }
}

impl StructuralWritable for Blob {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob_vec(self.into_vec())
    }
}

impl StructuralWritable for Vec<u8> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob_vec(self)
    }
}

impl StructuralWritable for &[u8] {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob(self)
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob(self)
    }
}

impl StructuralWritable for Box<[u8]> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        writer.write_blob_vec(self.into_vec())
    }
}

impl StructuralWritable for Value {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            Value::Extant => writer.write_extant(),
            Value::Int32Value(v) => writer.write_i32(*v),
            Value::Int64Value(v) => writer.write_i64(*v),
            Value::UInt32Value(v) => writer.write_u32(*v),
            Value::UInt64Value(v) => writer.write_u64(*v),
            Value::Float64Value(v) => writer.write_f64(*v),
            Value::BooleanValue(v) => writer.write_bool(*v),
            Value::BigInt(v) => writer.write_big_int(v.clone()),
            Value::BigUint(v) => writer.write_big_uint(v.clone()),
            Value::Text(v) => writer.write_text(v.clone()),
            Value::Data(v) => writer.write_blob(v.as_ref()),
            Value::Record(attrs, items) => {
                let mut header = writer.record(attrs.len())?;
                for Attr { name, value } in attrs.iter() {
                    header = header.write_attr(name.as_cow(), value)?;
                }
                let mut body = header.complete_header(
                    RecordBodyKind::of_iter(items.iter()).unwrap_or(RecordBodyKind::Mixed),
                    items.len(),
                )?;
                for item in items.iter() {
                    body = match item {
                        Item::ValueItem(v) => body.write_value(v)?,
                        Item::Slot(k, v) => body.write_slot(k, v)?,
                    }
                }
                body.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            Value::Extant => writer.write_extant(),
            Value::Int32Value(v) => writer.write_i32(v),
            Value::Int64Value(v) => writer.write_i64(v),
            Value::UInt32Value(v) => writer.write_u32(v),
            Value::UInt64Value(v) => writer.write_u64(v),
            Value::Float64Value(v) => writer.write_f64(v),
            Value::BooleanValue(v) => writer.write_bool(v),
            Value::BigInt(v) => writer.write_big_int(v),
            Value::BigUint(v) => writer.write_big_uint(v),
            Value::Text(v) => writer.write_text(v),
            Value::Data(v) => writer.write_blob_vec(v.into_vec()),
            Value::Record(attrs, items) => {
                let mut header = writer.record(attrs.len())?;
                for Attr { name, value } in attrs.into_iter() {
                    header = header.write_attr_into(name, value)?;
                }
                let mut body = header.complete_header(
                    RecordBodyKind::of_iter(items.iter()).unwrap_or(RecordBodyKind::Mixed),
                    items.len(),
                )?;
                for item in items.into_iter() {
                    body = match item {
                        Item::ValueItem(v) => body.write_value_into(v)?,
                        Item::Slot(k, v) => body.write_slot_into(k, v)?,
                    }
                }
                body.done()
            }
        }
    }

    fn structure(&self) -> Value {
        self.clone()
    }

    fn into_structure(self) -> Value {
        self
    }
}

impl<T: StructuralWritable> StructuralWritable for Vec<T> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        self.iter()
            .try_fold(
                writer
                    .record(0)?
                    .complete_header(RecordBodyKind::ArrayLike, self.len())?,
                |record_writer, value| record_writer.write_value(value),
            )?
            .done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let len = self.len();
        self.into_iter()
            .try_fold(
                writer
                    .record(0)?
                    .complete_header(RecordBodyKind::ArrayLike, len)?,
                |record_writer, value| record_writer.write_value_into(value),
            )?
            .done()
    }
}

impl<T: StructuralWritable> StructuralWritable for Option<T> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        if let Some(value) = self {
            value.write_with(writer)
        } else {
            writer.write_extant()
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        if let Some(value) = self {
            value.write_into(writer)
        } else {
            writer.write_extant()
        }
    }

    fn omit_as_field(&self) -> bool {
        self.is_none()
    }
}

impl<K, V, S> StructuralWritable for HashMap<K, V, S>
where
    K: StructuralWritable,
    V: StructuralWritable,
{
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let len = self.len();
        self.iter()
            .try_fold(
                writer
                    .record(0)?
                    .complete_header(RecordBodyKind::MapLike, len)?,
                |record_writer, (key, value)| record_writer.write_slot(key, value),
            )?
            .done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let len = self.len();
        self.into_iter()
            .try_fold(
                writer
                    .record(0)?
                    .complete_header(RecordBodyKind::MapLike, len)?,
                |record_writer, (key, value)| record_writer.write_slot_into(key, value),
            )?
            .done()
    }
}
