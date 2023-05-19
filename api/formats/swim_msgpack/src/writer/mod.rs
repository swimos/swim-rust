// Copyright 2015-2023 Swim Inc.
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

use std::borrow::Cow;
use std::convert::{Infallible, TryFrom};
use std::fmt::{Display, Formatter};
use std::io;
use std::io::Write;
use std::mem::size_of;

use bytes::{Bytes, BytesMut};
use rmp::encode::{
    write_array_len, write_bin, write_bool, write_ext_meta, write_f64, write_map_len, write_nil,
    write_sint, write_str, write_u64, ByteBuf, RmpWrite, ValueWriteError,
};

use swim_form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use swim_model::bigint::{BigInt, BigUint, Sign};

use crate::{BIG_INT_EXT, BIG_UINT_EXT};

#[cfg(test)]
mod tests;

// For records, attributes and items are written to two independent collection buffers that track
// the number of items that the collection should contain, its shape (array or map) and the number
// of remaining elements to be written.
//
// If the record's body is delegated, then a new interpreter is created that will yield the Value's
// shape as a discriminate (ReprKind) so that it can be written into the parent Value accordingly,
// either as the body being a single-element array or its Record's attributes will be promoted and
// its slots set as the parent's body.
//
// Following this, the two collection buffer's contents are written out as bytes.

/// [`StructuralWriter`] implementation that uses the MessagePack format. Primitive values are
/// written with the corresponding MessagePack types. Big integers are written as MessagePack
/// extensions as raw bytes in big endian order. Strings and binary blobs are written as MessagePack
/// string and bin values. Records have the following encoding.
///
/// - Attributes are written as MessagePack map where the keys are strings. If there are no
/// attributes an empty map value is still required.
/// - The items in the body follow the attributes immediately. If the body consists entirely of
/// slots it is written as a map. If the body consists of all value items or a mix of value items
/// and slots it is written as an array. When a slot occurs in an array body it is written as
/// an array of size two.
pub struct MsgPackInterpreter;

impl MsgPackInterpreter {
    /// Allocates a buffer with a capacity of 'len' for use by a closure that writes a primitive
    /// value.
    ///
    /// Returns a result containing the written data or a message pack write error.
    fn write<F, E>(&self, len: usize, f: F) -> Result<Bytes, MsgPackWriteError>
    where
        F: FnOnce(&mut ByteBuf) -> Result<(), E>,
        MsgPackWriteError: From<E>,
    {
        let mut buf = ByteBuf::with_capacity(len);
        f(&mut buf)?;
        Ok(Bytes::from(buf.into_vec()))
    }
}

impl StructuralWriter for MsgPackInterpreter {
    type Header = MsgPackHeaderInterpreter;
    type Body = MsgPackBodyInterpreter;

    fn record(self, num_attrs: usize) -> Result<Self::Header, Self::Error> {
        let num_attrs =
            u32::try_from(num_attrs).map_err(|_| MsgPackWriteError::TooManyAttrs(num_attrs))?;
        Ok(MsgPackHeaderInterpreter {
            attrs: CollectionBuffer::for_len(num_attrs),
        })
    }
}

impl PrimitiveWriter for MsgPackInterpreter {
    type Repr = Bytes;
    type Error = MsgPackWriteError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        self.write(1, |buf| write_nil(buf).map(|_| ()))
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        self.write(size_of::<i32>(), |buf| {
            write_sint(buf, value as i64).map(|_| ())
        })
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        self.write(size_of::<i64>(), |buf| write_sint(buf, value).map(|_| ()))
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        self.write(size_of::<u32>(), |buf| {
            write_sint(buf, value as i64).map(|_| ())
        })
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        if let Ok(n) = i64::try_from(value) {
            self.write(size_of::<i64>(), |buf| write_sint(buf, n).map(|_| ()))
        } else {
            self.write(size_of::<u64>(), |buf| write_u64(buf, value))
        }
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        self.write(size_of::<f64>(), |buf| write_f64(buf, value))
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        self.write(1, |buf| write_bool(buf, value))
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let (sign, bytes) = value.to_bytes_be();
        if let Ok(n) = u32::try_from(bytes.len() + 1) {
            self.write((n + 1) as usize, |buf| {
                match write_ext_meta(buf, n, BIG_INT_EXT) {
                    Ok(_) => {
                        let sign_byte: u8 = if sign == Sign::Minus { 0 } else { 1 };
                        buf.write_u8(sign_byte)?;
                        buf.write_bytes(bytes.as_slice())
                    }
                    Err(e) => never(e),
                }
            })
        } else {
            Err(MsgPackWriteError::BigIntTooLarge(value))
        }
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let bytes = value.to_bytes_be();
        if let Ok(n) = u32::try_from(bytes.len()) {
            self.write(n as usize, |buf| {
                match write_ext_meta(buf, n, BIG_UINT_EXT) {
                    Ok(_) => buf.write_bytes(bytes.as_slice()),
                    Err(e) => never(e),
                }
            })
        } else {
            Err(MsgPackWriteError::BigUIntTooLarge(value))
        }
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let value = value.as_ref();
        self.write(value.len(), |buf| write_str(buf, value))
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        self.write(blob.len(), |buf| write_bin(buf, blob.as_slice()))
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        self.write(value.len(), |buf| write_bin(buf, value))
    }
}

fn never(err: ValueWriteError<Infallible>) -> ! {
    match err {
        ValueWriteError::InvalidMarkerWrite(e) => match e {},
        ValueWriteError::InvalidDataWrite(e) => match e {},
    }
}

pub struct MsgPackHeaderInterpreter {
    attrs: CollectionBuffer,
}

impl MsgPackHeaderInterpreter {
    fn body_interpreter(self, items_len: u32, kind: RecordBodyKind) -> MsgPackBodyInterpreter {
        let MsgPackHeaderInterpreter { attrs } = self;
        MsgPackBodyInterpreter {
            attrs,
            items: CollectionBuffer::for_len(items_len),
            kind,
        }
    }
}

impl HeaderWriter for MsgPackHeaderInterpreter {
    type Repr = Bytes;
    type Error = MsgPackWriteError;
    type Body = MsgPackBodyInterpreter;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        if self.attrs.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfAttrs)
        } else {
            self.attrs.expecting -= 1;
            write_str(&mut self.attrs, name.as_ref())?;
            let value_bytes = value.write_with(MsgPackInterpreter)?;
            self.attrs.write_bytes(value_bytes.as_ref())?;
            Ok(self)
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        let MsgPackHeaderInterpreter { mut attrs } = self;

        match value.write_with(MsgPackDelegateInterpreter)? {
            DelegateReprKind::ValueItem(bytes) => {
                let mut buf = BytesMutWrite::default();

                write_map_len(&mut buf, attrs.len)?;
                buf.extend_from_slice(attrs.buf.as_slice());

                write_array_len(&mut buf, 1)?;
                buf.extend_from_slice(bytes.as_ref());

                Ok(buf.freeze())
            }
            DelegateReprKind::Record {
                attrs: delegate_attrs,
                items: delegate_items,
                kind,
            } => {
                attrs.write_bytes(delegate_attrs.buf.as_slice())?;

                let mut buf = BytesMutWrite::default();

                write_map_len(&mut buf, attrs.len)?;
                buf.extend_from_slice(attrs.buf.as_slice());

                if kind == RecordBodyKind::MapLike {
                    write_map_len(&mut buf, delegate_items.len)?;
                } else {
                    write_array_len(&mut buf, delegate_items.len)?;
                }

                buf.extend_from_slice(delegate_items.buf.as_slice());

                Ok(buf.freeze())
            }
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        self.delegate(&value)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_attr(name.as_cow(), &value)
    }

    fn complete_header(
        self,
        kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        if self.attrs.expecting != 0 {
            Err(MsgPackWriteError::WrongNumberOfAttrs)
        } else {
            let num_items =
                u32::try_from(num_items).map_err(|_| MsgPackWriteError::TooManyItems(num_items))?;
            Ok(self.body_interpreter(num_items, kind))
        }
    }
}

/// [`BodyWriter`] implementation for [`MsgPackInterpreter`].
pub struct MsgPackBodyInterpreter {
    attrs: CollectionBuffer,
    items: CollectionBuffer,
    kind: RecordBodyKind,
}

impl BodyWriter for MsgPackBodyInterpreter {
    type Repr = Bytes;
    type Error = MsgPackWriteError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        if self.items.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else if self.kind == RecordBodyKind::MapLike {
            Err(MsgPackWriteError::IncorrectRecordKind)
        } else {
            self.items.expecting -= 1;
            let bytes = value.write_with(MsgPackInterpreter)?;
            self.items.write_bytes(bytes.as_ref())?;
            Ok(self)
        }
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        if self.items.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else {
            match self.kind {
                RecordBodyKind::ArrayLike => {
                    return Err(MsgPackWriteError::IncorrectRecordKind);
                }
                RecordBodyKind::Mixed => {
                    write_array_len(&mut self.items, 2)?;
                }
                _ => {}
            }
            self.items.expecting -= 1;
            let key_bytes = key.write_with(MsgPackInterpreter)?;
            self.items.write_bytes(key_bytes.as_ref())?;

            let value_bytes = value.write_with(MsgPackInterpreter)?;
            self.items.write_bytes(value_bytes.as_ref())?;

            Ok(self)
        }
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_slot(&key, &value)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        if self.items.expecting != 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else {
            let MsgPackBodyInterpreter { attrs, items, kind } = self;
            let CollectionBuffer {
                buf: attrs_buf,
                len: attrs_len,
                ..
            } = attrs;
            let CollectionBuffer {
                buf: items_buf,
                len: items_len,
                ..
            } = items;

            let mut buf = BytesMutWrite::default();

            // we don't want to merge the length of the delegate attribute collection into with our
            // attributes length as it _already_ is contained.
            //
            // The num_attrs passed to StructuralWriter::record should *already* contain the number
            // of attributes that this Value contains; it's existing attributes and the number of
            // attributes that are promoted from the delegate operation.

            write_map_len(&mut buf, attrs_len)?;
            buf.extend_from_slice(attrs_buf.as_slice());

            if kind == RecordBodyKind::MapLike {
                write_map_len(&mut buf, items_len)?;
            } else {
                write_array_len(&mut buf, items_len)?;
            }

            buf.extend_from_slice(items_buf.as_slice());

            Ok(buf.freeze())
        }
    }
}

/// Wrapped around a BytesMut that implements io::Write for use by rmp.
#[derive(Default)]
struct BytesMutWrite(BytesMut);

impl BytesMutWrite {
    fn freeze(self) -> Bytes {
        self.0.freeze()
    }

    fn extend_from_slice(&mut self, extend: &[u8]) {
        self.0.extend_from_slice(extend)
    }
}

impl Write for BytesMutWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writing out to MessagePack can fail because of an IO error or because a value exceeds the
/// limitations of the MessagePack format.
#[derive(Debug)]
pub enum MsgPackWriteError {
    /// An error ocurred in the underlying writer.
    IoError(io::Error),
    /// The byte representation of a big integer could not fit into a MessagePack extension value.
    BigIntTooLarge(BigInt),
    /// The byte representation of a big unsigned. integer could not fit into a MessagePack
    /// extension value.
    BigUIntTooLarge(BigUint),
    /// The record has more attributes than can be represented by a `u32`.
    TooManyAttrs(usize),
    /// The record has more items than can be represented by a `u32`.
    TooManyItems(usize),
    /// The reported number of attributes in the record did not match the number written.
    WrongNumberOfAttrs,
    /// The reported kind of the record did not match the items written.
    IncorrectRecordKind,
    /// The reported number of items in the record did not match the number written.
    WrongNumberOfItems,
}

impl From<ValueWriteError<Infallible>> for MsgPackWriteError {
    fn from(value: ValueWriteError<Infallible>) -> Self {
        match value {
            ValueWriteError::InvalidMarkerWrite(e) => match e {},
            ValueWriteError::InvalidDataWrite(e) => match e {},
        }
    }
}

impl From<Infallible> for MsgPackWriteError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

impl PartialEq for MsgPackWriteError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MsgPackWriteError::BigIntTooLarge(n), MsgPackWriteError::BigIntTooLarge(m)) => n == m,
            (MsgPackWriteError::BigUIntTooLarge(n), MsgPackWriteError::BigUIntTooLarge(m)) => {
                n == m
            }
            (MsgPackWriteError::TooManyAttrs(n), MsgPackWriteError::TooManyAttrs(m)) => n == m,
            (MsgPackWriteError::TooManyItems(n), MsgPackWriteError::TooManyItems(m)) => n == m,
            (MsgPackWriteError::WrongNumberOfAttrs, MsgPackWriteError::WrongNumberOfAttrs) => true,
            (MsgPackWriteError::IncorrectRecordKind, MsgPackWriteError::IncorrectRecordKind) => {
                true
            }
            (MsgPackWriteError::WrongNumberOfItems, MsgPackWriteError::WrongNumberOfItems) => true,
            _ => false,
        }
    }
}

impl From<io::Error> for MsgPackWriteError {
    fn from(err: io::Error) -> Self {
        MsgPackWriteError::IoError(err)
    }
}

impl From<ValueWriteError> for MsgPackWriteError {
    fn from(err: ValueWriteError) -> Self {
        MsgPackWriteError::IoError(err.into())
    }
}

impl Display for MsgPackWriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MsgPackWriteError::IoError(err) => {
                write!(f, "An error ocurred writing the content: {}", err)
            }
            MsgPackWriteError::BigIntTooLarge(_) | MsgPackWriteError::BigUIntTooLarge(_) => {
                //If it's too big for MessagePack, it's too big to print!
                write!(f, "Big integer too large to be written in MessagePack.")
            }
            MsgPackWriteError::TooManyAttrs(n) => {
                write!(f, "{} attributes is too many to encode as MessagePack.", n)
            }
            MsgPackWriteError::TooManyItems(n) => {
                write!(f, "{} items is too many to encode as MessagePack.", n)
            }
            MsgPackWriteError::WrongNumberOfAttrs => {
                write!(
                    f,
                    "The number of attributes written did not match the number reported."
                )
            }
            MsgPackWriteError::IncorrectRecordKind => {
                write!(
                    f,
                    "The record items written did not match the kind reported."
                )
            }
            MsgPackWriteError::WrongNumberOfItems => {
                write!(
                    f,
                    "The number of items written did not match the number reported."
                )
            }
        }
    }
}

impl std::error::Error for MsgPackWriteError {}

pub struct CollectionBuffer {
    /// The collection's data.
    buf: ByteBuf,
    /// The length of the collection.
    len: u32,
    /// The number of elements left to write.
    expecting: u32,
}

impl CollectionBuffer {
    fn for_len(len: u32) -> CollectionBuffer {
        CollectionBuffer {
            buf: ByteBuf::new(),
            len,
            expecting: len,
        }
    }

    fn extend(&mut self, with: CollectionBuffer) {
        match self.buf.write_bytes(with.buf.as_slice()) {
            Ok(()) => {
                self.len += with.len;
                self.expecting += with.expecting;
            }
            Err(e) => match e {},
        }
    }
}

impl Write for CollectionBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.buf.write_bytes(buf) {
            Ok(()) => Ok(buf.len()),
            Err(e) => match e {},
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Discriminate over a delegate body's output.
pub enum DelegateReprKind {
    /// The delegate operation yielded a single ValueItem that should be written as a single-element
    /// array.
    ValueItem(Bytes),
    /// The delegate operation yielded a Record.
    Record {
        /// Attributes to promote.
        attrs: CollectionBuffer,
        /// Items to set as the parent's body.
        items: CollectionBuffer,
        /// The kind of the items; array, map or a mixed collection type.
        kind: RecordBodyKind,
    },
}

/// A message pack delegate body interpreter which yields a discriminate over its representation.
///
/// This delegates all operations to a 'MsgPackInterpreter' and discriminates over its 'delegate'
/// and 'done' operations for the correct placement in the output bytes.
struct MsgPackDelegateInterpreter;

impl PrimitiveWriter for MsgPackDelegateInterpreter {
    type Repr = DelegateReprKind;
    type Error = MsgPackWriteError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_extant()
            .map(DelegateReprKind::ValueItem)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_i32(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_i64(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_u32(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_u64(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_f64(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_bool(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_big_int(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_big_uint(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_text(value)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_blob_vec(blob)
            .map(DelegateReprKind::ValueItem)
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        MsgPackInterpreter
            .write_blob(value)
            .map(DelegateReprKind::ValueItem)
    }
}

impl StructuralWriter for MsgPackDelegateInterpreter {
    type Header = MsgPackDelegateHeaderInterpreter;
    type Body = MsgPackDelegateBodyInterpreter;

    fn record(self, num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(MsgPackDelegateHeaderInterpreter {
            inner: MsgPackInterpreter.record(num_attrs)?,
        })
    }
}

pub struct MsgPackDelegateHeaderInterpreter {
    inner: MsgPackHeaderInterpreter,
}

impl HeaderWriter for MsgPackDelegateHeaderInterpreter {
    type Repr = DelegateReprKind;
    type Error = MsgPackWriteError;
    type Body = MsgPackDelegateBodyInterpreter;

    fn write_attr<V: StructuralWritable>(
        self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        Ok(MsgPackDelegateHeaderInterpreter {
            inner: self.inner.write_attr(name, value)?,
        })
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        let MsgPackDelegateHeaderInterpreter { inner } = self;
        let MsgPackHeaderInterpreter { mut attrs } = inner;

        match value.write_with(MsgPackDelegateInterpreter)? {
            DelegateReprKind::ValueItem(bytes) => {
                let mut buf = BytesMutWrite::default();

                write_map_len(&mut buf, attrs.len)?;
                buf.extend_from_slice(attrs.buf.as_slice());

                write_array_len(&mut buf, 1)?;
                buf.extend_from_slice(bytes.as_ref());

                Ok(DelegateReprKind::Record {
                    attrs,
                    items: CollectionBuffer {
                        buf: ByteBuf::from_vec(bytes.to_vec()),
                        len: 1,
                        expecting: 0,
                    },
                    kind: RecordBodyKind::ArrayLike,
                })
            }
            DelegateReprKind::Record {
                attrs: delegate_attrs,
                items: delegate_items,
                kind,
            } => {
                attrs.extend(delegate_attrs);
                Ok(DelegateReprKind::Record {
                    attrs,
                    items: delegate_items,
                    kind,
                })
            }
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        self.delegate(&value)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        Ok(MsgPackDelegateHeaderInterpreter {
            inner: self.inner.write_attr_into(name, value)?,
        })
    }

    fn complete_header(
        self,
        kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        Ok(MsgPackDelegateBodyInterpreter {
            inner: self.inner.complete_header(kind, num_items)?,
        })
    }
}

pub struct MsgPackDelegateBodyInterpreter {
    inner: MsgPackBodyInterpreter,
}

impl BodyWriter for MsgPackDelegateBodyInterpreter {
    type Repr = DelegateReprKind;
    type Error = MsgPackWriteError;

    fn write_value<V: StructuralWritable>(self, value: &V) -> Result<Self, Self::Error> {
        Ok(MsgPackDelegateBodyInterpreter {
            inner: self.inner.write_value(value)?,
        })
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        Ok(MsgPackDelegateBodyInterpreter {
            inner: self.inner.write_slot(key, value)?,
        })
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_slot(&key, &value)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let MsgPackDelegateBodyInterpreter { inner } = self;
        let MsgPackBodyInterpreter { attrs, items, kind } = inner;
        Ok(DelegateReprKind::Record { attrs, items, kind })
    }
}
