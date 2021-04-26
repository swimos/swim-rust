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

#[cfg(test)]
mod tests;

use crate::form::structural::read::{BodyReader, HeaderReader, ReadError, StructuralReadable};
use crate::form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::marker::PhantomData;

/// Bridge to forward writes to a [`StructuralWriter`] instance to the builder methods
/// on a [`StructuralReadable`] type.
pub struct ReadWriteBridge<T>(PhantomData<fn() -> T>);

impl<T> Default for ReadWriteBridge<T> {
    fn default() -> Self {
        ReadWriteBridge(PhantomData)
    }
}

type BodyOf<T> = <<T as StructuralReadable>::Reader as HeaderReader>::Body;

pub struct ReadWriteHeaderBridge<T: StructuralReadable>(ReadWriteBridge<T>, T::Reader);
pub struct ReadWriteBodyBridge<T: StructuralReadable>(ReadWriteBridge<T>, BodyOf<T>);

pub struct HeaderDelegateBridge<B: BodyReader>(<B as BodyReader>::Delegate);
pub struct BodyDelegateBridge<B: BodyReader>(<<B as BodyReader>::Delegate as HeaderReader>::Body);

pub struct ItemDelegateBridge<B>(B);
pub struct ValueDelegateBridge<H>(H);

impl<T: StructuralReadable> PrimitiveWriter for ReadWriteBridge<T> {
    type Repr = T;
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        T::read_extant()
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        T::read_i32(value)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        T::read_i64(value)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        T::read_u32(value)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        T::read_u64(value)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        T::read_f64(value)
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        T::read_bool(value)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        T::read_big_int(value)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        T::read_big_uint(value)
    }

    fn write_text<L: Label>(self, value: L) -> Result<Self::Repr, Self::Error> {
        T::read_text(value.as_cow())
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        T::read_blob(blob)
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        T::read_blob(value.to_vec())
    }
}

impl<T: StructuralReadable> StructuralWriter for ReadWriteBridge<T> {
    type Header = ReadWriteHeaderBridge<T>;
    type Body = ReadWriteBodyBridge<T>;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(ReadWriteHeaderBridge(self, T::record_reader()?))
    }
}

impl<T: StructuralReadable> HeaderWriter for ReadWriteHeaderBridge<T> {
    type Repr = T;
    type Error = ReadError;
    type Body = ReadWriteBodyBridge<T>;

    fn write_attr<V: StructuralWritable>(
        self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let ReadWriteHeaderBridge(root, header_reader) = self;
        let delegate = ItemDelegateBridge(header_reader.read_attribute(name)?);
        let delegate_reader = value.write_with(delegate)?;
        let header_reader = <T as StructuralReadable>::Reader::restore(delegate_reader)?;
        Ok(ReadWriteHeaderBridge(root, header_reader))
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        let ReadWriteHeaderBridge(_, header_reader) = self;
        let delegate = ValueDelegateBridge(header_reader);
        let body_reader = value.write_with(delegate)?;
        T::try_terminate(body_reader)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let ReadWriteHeaderBridge(root, header_reader) = self;
        let delegate =
            ItemDelegateBridge(header_reader.read_attribute(Cow::Borrowed(name.as_ref()))?);
        let delegate_reader = value.write_with(delegate)?;
        let header_reader = <T as StructuralReadable>::Reader::restore(delegate_reader)?;
        Ok(ReadWriteHeaderBridge(root, header_reader))
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        let ReadWriteHeaderBridge(_, header_reader) = self;
        let delegate = ValueDelegateBridge(header_reader);
        let body_reader = value.write_into(delegate)?;
        T::try_terminate(body_reader)
    }

    fn complete_header(
        self,
        _kind: RecordBodyKind,
        _num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let ReadWriteHeaderBridge(root, header_reader) = self;
        Ok(ReadWriteBodyBridge(root, header_reader.start_body()?))
    }
}

impl<T: StructuralReadable> BodyWriter for ReadWriteBodyBridge<T> {
    type Repr = T;
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(self, value: &V) -> Result<Self, Self::Error> {
        let ReadWriteBodyBridge(root, body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_with(delegate)?;
        Ok(ReadWriteBodyBridge(root, body_reader))
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let ReadWriteBodyBridge(root, body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_with(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_with(delegate)?;
        Ok(ReadWriteBodyBridge(root, body_reader))
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        let ReadWriteBodyBridge(root, body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_into(delegate)?;
        Ok(ReadWriteBodyBridge(root, body_reader))
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let ReadWriteBodyBridge(root, body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_into(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_into(delegate)?;
        Ok(ReadWriteBodyBridge(root, body_reader))
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let ReadWriteBodyBridge(_, body_reader) = self;
        T::try_terminate(body_reader)
    }
}

impl<B: BodyReader> HeaderWriter for HeaderDelegateBridge<B> {
    type Repr = B;
    type Error = ReadError;
    type Body = BodyDelegateBridge<B>;

    fn write_attr<V: StructuralWritable>(
        self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let HeaderDelegateBridge(header_reader) = self;
        let delegate = ItemDelegateBridge(header_reader.read_attribute(name)?);
        let delegate_reader = value.write_with(delegate)?;
        let header_reader = <B as BodyReader>::Delegate::restore(delegate_reader)?;
        Ok(HeaderDelegateBridge(header_reader))
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        let HeaderDelegateBridge(header_reader) = self;
        let delegate = ValueDelegateBridge(header_reader);
        let body_reader = value.write_with(delegate)?;
        B::restore(body_reader)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let HeaderDelegateBridge(header_reader) = self;
        let delegate =
            ItemDelegateBridge(header_reader.read_attribute(Cow::Borrowed(name.as_ref()))?);
        let delegate_reader = value.write_into(delegate)?;
        let header_reader = <B as BodyReader>::Delegate::restore(delegate_reader)?;
        Ok(HeaderDelegateBridge(header_reader))
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        let HeaderDelegateBridge(header_reader) = self;
        let delegate = ValueDelegateBridge(header_reader);
        let body_reader = value.write_into(delegate)?;
        B::restore(body_reader)
    }

    fn complete_header(
        self,
        _kind: RecordBodyKind,
        _num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let HeaderDelegateBridge(delegate) = self;
        let body_reader = delegate.start_body()?;
        Ok(BodyDelegateBridge(body_reader))
    }
}

impl<B: BodyReader> BodyWriter for BodyDelegateBridge<B> {
    type Repr = B;
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(self, value: &V) -> Result<Self, Self::Error> {
        let BodyDelegateBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_with(delegate)?;
        Ok(BodyDelegateBridge(body_reader))
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let BodyDelegateBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_with(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_with(delegate)?;
        Ok(BodyDelegateBridge(body_reader))
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        let BodyDelegateBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_into(delegate)?;
        Ok(BodyDelegateBridge(body_reader))
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let BodyDelegateBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_into(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_into(delegate)?;
        Ok(BodyDelegateBridge(body_reader))
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let BodyDelegateBridge(body_reader) = self;
        B::restore(body_reader)
    }
}

impl<B: BodyReader> PrimitiveWriter for ItemDelegateBridge<B> {
    type Repr = B;
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_extant()?;
        Ok(inner)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_i32(value)?;
        Ok(inner)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_i64(value)?;
        Ok(inner)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_u32(value)?;
        Ok(inner)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_u64(value)?;
        Ok(inner)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_f64(value)?;
        Ok(inner)
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_bool(value)?;
        Ok(inner)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_big_int(value)?;
        Ok(inner)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_big_uint(value)?;
        Ok(inner)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_text(value.as_cow())?;
        Ok(inner)
    }

    fn write_blob_vec(self, value: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_blob(value)?;
        Ok(inner)
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        let ItemDelegateBridge(mut inner) = self;
        inner.push_blob(value.to_vec())?;
        Ok(inner)
    }
}

impl<B: BodyReader> StructuralWriter for ItemDelegateBridge<B> {
    type Header = HeaderDelegateBridge<B>;
    type Body = BodyDelegateBridge<B>;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        let ItemDelegateBridge(inner) = self;
        Ok(HeaderDelegateBridge(inner.push_record()?))
    }
}

impl<H: HeaderReader> PrimitiveWriter for ValueDelegateBridge<H> {
    type Repr = H::Body;
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_extant()?;
        Ok(body_writer)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_i32(value)?;
        Ok(body_writer)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_i64(value)?;
        Ok(body_writer)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_u32(value)?;
        Ok(body_writer)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_u64(value)?;
        Ok(body_writer)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_f64(value)?;
        Ok(body_writer)
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_bool(value)?;
        Ok(body_writer)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_big_int(value)?;
        Ok(body_writer)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_big_uint(value)?;
        Ok(body_writer)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_text(value.as_cow())?;
        Ok(body_writer)
    }

    fn write_blob_vec(self, value: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_blob(value)?;
        Ok(body_writer)
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        let ValueDelegateBridge(header_writer) = self;
        let mut body_writer = header_writer.start_body()?;
        body_writer.push_blob(value.to_vec())?;
        Ok(body_writer)
    }
}

pub struct NestedBodyBridge<B>(B);

impl<H: HeaderReader> HeaderWriter for ValueDelegateBridge<H> {
    type Repr = H::Body;
    type Error = ReadError;
    type Body = NestedBodyBridge<H::Body>;

    fn write_attr<V: StructuralWritable>(
        self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let ValueDelegateBridge(header_reader) = self;
        let delegate = ItemDelegateBridge(header_reader.read_attribute(name)?);
        let delegate_reader = value.write_with(delegate)?;
        let header_reader = H::restore(delegate_reader)?;
        Ok(ValueDelegateBridge(header_reader))
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let ValueDelegateBridge(header_reader) = self;
        let delegate =
            ItemDelegateBridge(header_reader.read_attribute(Cow::Borrowed(name.as_ref()))?);
        let delegate_reader = value.write_into(delegate)?;
        let header_reader = H::restore(delegate_reader)?;
        Ok(ValueDelegateBridge(header_reader))
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(
        self,
        _kind: RecordBodyKind,
        _num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let ValueDelegateBridge(header_reader) = self;
        Ok(NestedBodyBridge(header_reader.start_body()?))
    }
}

impl<H: HeaderReader> StructuralWriter for ValueDelegateBridge<H> {
    type Header = Self;
    type Body = NestedBodyBridge<H::Body>;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<B: BodyReader> BodyWriter for NestedBodyBridge<B> {
    type Repr = B;
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(self, value: &V) -> Result<Self, Self::Error> {
        let NestedBodyBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_with(delegate)?;
        Ok(NestedBodyBridge(body_reader))
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let NestedBodyBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_with(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_with(delegate)?;
        Ok(NestedBodyBridge(body_reader))
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        let NestedBodyBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let body_reader = value.write_into(delegate)?;
        Ok(NestedBodyBridge(body_reader))
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let NestedBodyBridge(body_reader) = self;
        let delegate = ItemDelegateBridge(body_reader);
        let mut body_reader = key.write_into(delegate)?;
        body_reader.start_slot()?;
        let delegate = ItemDelegateBridge(body_reader);
        body_reader = value.write_into(delegate)?;
        Ok(NestedBodyBridge(body_reader))
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let NestedBodyBridge(inner) = self;
        Ok(inner)
    }
}
