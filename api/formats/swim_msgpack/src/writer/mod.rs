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

use crate::{BIG_INT_EXT, BIG_UINT_EXT};
use swim_form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use byteorder::WriteBytesExt;
use rmp::encode::{
    write_array_len, write_bin, write_bool, write_ext_meta, write_f64, write_map_len, write_nil,
    write_sint, write_str, write_u64, ValueWriteError,
};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io::{Error, Write};
use swim_model::bigint::{BigInt, BigUint, Sign};

/// [`StructuralWriter`] implementation that uses the MessagePack format. Primitive values are
/// written with the corresponding MessagePack types. Big integers are written as MessagePack
/// extensions as raw bytes in big endian order. Strings and binary blobs are written as
/// MessagePack string and bin values. Records have the following encoding.
///
/// - Attributes are writen as MessagePack map where the keys are strings. If there are no
/// attributes an empty map value is still required.
/// - The items in the body follow the attributes immediately. If the body consists entirely of
/// slots it is written as a map. If the body consists of all value items or a mix of value items
/// and slots it is written as an array. When a slot occurs in an array body it is written as
/// an array of size two.
///
/// # Type Parameters
///
/// * `W` - Any type that implements [`std::io::Write`].
pub struct MsgPackInterpreter<'a, W> {
    writer: &'a mut W,
    started: bool,
    expecting: u32, //Keeps track of the number of attributes to write.
}

/// [`BodyWriter`] implementation for [`MsgPackInterpreter`].
pub struct MsgPackBodyInterpreter<'a, W> {
    writer: &'a mut W,
    expecting: u32, //Keeps track of the number of items to be written.
    kind: RecordBodyKind,
}

impl<'a, W> MsgPackInterpreter<'a, W> {
    pub fn new(writer: &'a mut W) -> Self {
        MsgPackInterpreter {
            writer,
            started: false,
            expecting: 0,
        }
    }

    fn child(&mut self) -> MsgPackInterpreter<'_, W> {
        let MsgPackInterpreter { writer, .. } = self;
        MsgPackInterpreter::new(*writer)
    }

    fn body_interpreter(
        self,
        expecting: u32,
        kind: RecordBodyKind,
    ) -> MsgPackBodyInterpreter<'a, W> {
        let MsgPackInterpreter { writer, .. } = self;
        MsgPackBodyInterpreter {
            writer,
            expecting,
            kind,
        }
    }
}

impl<'a, W> MsgPackBodyInterpreter<'a, W> {
    fn child(&mut self) -> MsgPackInterpreter<'_, W> {
        let MsgPackBodyInterpreter { writer, .. } = self;
        MsgPackInterpreter::new(*writer)
    }
}

/// Writing out to MessagePack can fail beacause of an IO error or because a value exceeds the
/// limitations of the MessagePack format.
#[derive(Debug)]
pub enum MsgPackWriteError {
    /// An error ocurred in the underlying writer.
    IoError(std::io::Error),
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

impl From<std::io::Error> for MsgPackWriteError {
    fn from(err: Error) -> Self {
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

impl<'a, W: Write> PrimitiveWriter for MsgPackInterpreter<'a, W> {
    type Repr = ();
    type Error = MsgPackWriteError;

    fn write_extant(mut self) -> Result<Self::Repr, Self::Error> {
        write_nil(&mut self.writer)?;
        Ok(())
    }

    fn write_i32(mut self, value: i32) -> Result<Self::Repr, Self::Error> {
        write_sint(&mut self.writer, value as i64)?;
        Ok(())
    }

    fn write_i64(mut self, value: i64) -> Result<Self::Repr, Self::Error> {
        write_sint(&mut self.writer, value)?;
        Ok(())
    }

    fn write_u32(mut self, value: u32) -> Result<Self::Repr, Self::Error> {
        write_sint(&mut self.writer, value as i64)?;
        Ok(())
    }

    fn write_u64(mut self, value: u64) -> Result<Self::Repr, Self::Error> {
        if let Ok(n) = i64::try_from(value) {
            write_sint(&mut self.writer, n)?;
        } else {
            write_u64(&mut self.writer, value)?;
        }
        Ok(())
    }

    fn write_f64(mut self, value: f64) -> Result<Self::Repr, Self::Error> {
        write_f64(&mut self.writer, value)?;
        Ok(())
    }

    fn write_bool(mut self, value: bool) -> Result<Self::Repr, Self::Error> {
        write_bool(&mut self.writer, value)?;
        Ok(())
    }

    fn write_big_int(mut self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let (sign, bytes) = value.to_bytes_be();
        if let Ok(n) = u32::try_from(bytes.len() + 1) {
            write_ext_meta(&mut self.writer, n, BIG_INT_EXT)?;
            let sign_byte: u8 = if sign == Sign::Minus { 0 } else { 1 };
            self.writer.write_u8(sign_byte)?;
            self.writer.write_all(bytes.as_slice())?;
            Ok(())
        } else {
            Err(MsgPackWriteError::BigIntTooLarge(value))
        }
    }

    fn write_big_uint(mut self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let bytes = value.to_bytes_be();
        if let Ok(n) = u32::try_from(bytes.len()) {
            write_ext_meta(&mut self.writer, n, BIG_UINT_EXT)?;
            self.writer.write_all(bytes.as_slice())?;
            Ok(())
        } else {
            Err(MsgPackWriteError::BigUIntTooLarge(value))
        }
    }

    fn write_text<T: Label>(mut self, value: T) -> Result<Self::Repr, Self::Error> {
        write_str(&mut self.writer, value.as_ref())?;
        Ok(())
    }

    fn write_blob_vec(mut self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        write_bin(&mut self.writer, blob.as_slice())?;
        Ok(())
    }

    fn write_blob(mut self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        write_bin(&mut self.writer, value)?;
        Ok(())
    }
}

impl<'a, W: Write> StructuralWriter for MsgPackInterpreter<'a, W> {
    type Header = Self;
    type Body = MsgPackBodyInterpreter<'a, W>;

    fn record(mut self, num_attrs: usize) -> Result<Self::Header, Self::Error> {
        if !self.started {
            self.started = true;
            self.expecting = to_expecting(num_attrs)?;
            write_map_len(&mut self.writer, self.expecting)?;
        }
        Ok(self)
    }
}

fn to_expecting(n: usize) -> Result<u32, MsgPackWriteError> {
    u32::try_from(n).map_err(|_| MsgPackWriteError::TooManyAttrs(n))
}

impl<'a, W: Write> HeaderWriter for MsgPackInterpreter<'a, W> {
    type Repr = ();
    type Error = MsgPackWriteError;
    type Body = MsgPackBodyInterpreter<'a, W>;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        if self.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfAttrs)
        } else {
            self.expecting -= 1;
            write_str(&mut self.writer, name.as_ref())?;
            let value_interp = self.child();
            value.write_with(value_interp)?;
            Ok(self)
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_attr(Cow::Borrowed(name.as_ref()), &value)
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn complete_header(
        mut self,
        kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        if self.expecting != 0 {
            Err(MsgPackWriteError::WrongNumberOfAttrs)
        } else {
            let expecting_items = to_expecting(num_items)?;
            if kind == RecordBodyKind::MapLike {
                write_map_len(&mut self.writer, expecting_items)?;
            } else {
                write_array_len(&mut self.writer, expecting_items)?;
            }
            Ok(self.body_interpreter(expecting_items, kind))
        }
    }
}

impl<'a, W: Write> BodyWriter for MsgPackBodyInterpreter<'a, W> {
    type Repr = ();
    type Error = MsgPackWriteError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        if self.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else if self.kind == RecordBodyKind::MapLike {
            Err(MsgPackWriteError::IncorrectRecordKind)
        } else {
            self.expecting -= 1;
            let value_writer = self.child();
            value.write_with(value_writer)?;
            Ok(self)
        }
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        if self.expecting == 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else {
            match self.kind {
                RecordBodyKind::ArrayLike => {
                    return Err(MsgPackWriteError::IncorrectRecordKind);
                }
                RecordBodyKind::Mixed => {
                    write_array_len(&mut self.writer, 2)?;
                }
                _ => {}
            }
            self.expecting -= 1;
            let key_writer = self.child();
            key.write_with(key_writer)?;
            let value_writer = self.child();
            value.write_with(value_writer)?;
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
        if self.expecting != 0 {
            Err(MsgPackWriteError::WrongNumberOfItems)
        } else {
            Ok(())
        }
    }
}
