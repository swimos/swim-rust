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

use crate::form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use byteorder::WriteBytesExt;
use num_bigint::{BigInt, BigUint, Sign};
use rmp::encode::{
    write_array_len, write_bin, write_bool, write_ext_meta, write_f64, write_map_len, write_nil,
    write_sint, write_str, write_u64,
};
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io::{ErrorKind, Write};

pub struct MsgPackInterpreter<W> {
    writer: W,
    expecting: u32,
}

pub struct MsgPackBodyInterpreter<W> {
    writer: W,
    expecting: u32,
    kind: RecordBodyKind,
}

impl<W> MsgPackInterpreter<W> {
    pub fn new(writer: W) -> Self {
        MsgPackInterpreter {
            writer,
            expecting: 0,
        }
    }

    fn child(&mut self) -> MsgPackInterpreter<&mut W> {
        let MsgPackInterpreter { writer, .. } = self;
        MsgPackInterpreter::new(writer)
    }

    fn body_interpreter(self, expecting: u32, kind: RecordBodyKind) -> MsgPackBodyInterpreter<W> {
        let MsgPackInterpreter { writer, .. } = self;
        MsgPackBodyInterpreter {
            writer,
            expecting,
            kind,
        }
    }
}

impl<W> MsgPackBodyInterpreter<W> {
    fn child(&mut self) -> MsgPackInterpreter<&mut W> {
        let MsgPackBodyInterpreter { writer, .. } = self;
        MsgPackInterpreter::new(writer)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum MsgPackError {
    BigIntTooLarge(BigInt),
    BigUIntTooLarge(BigUint),
    ToManyAttrs(usize),
    TooManyItems(usize),
    WrongNumberOfAttrs,
    IncorrectRecordKind,
    WrongNumberOfItems,
}

pub(in crate::form::structural) const BIG_INT_EXT: i8 = 0;
pub(in crate::form::structural) const BIG_UINT_EXT: i8 = 1;

impl Display for MsgPackError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MsgPackError::BigIntTooLarge(_) | MsgPackError::BigUIntTooLarge(_) => {
                //If it's too big for MessagePack, it's too big to print!
                write!(f, "Big integer too large to be written in MessagePack.")
            }
            MsgPackError::ToManyAttrs(n) => {
                write!(f, "{} attributes is too many to encode as MessagePack.", n)
            }
            MsgPackError::TooManyItems(n) => {
                write!(f, "{} items is too many to encode as MessagePack.", n)
            }
            MsgPackError::WrongNumberOfAttrs => {
                write!(
                    f,
                    "The number of attributes written did not match the number reported."
                )
            }
            MsgPackError::IncorrectRecordKind => {
                write!(
                    f,
                    "The record items written did not match the kind reported."
                )
            }
            MsgPackError::WrongNumberOfItems => {
                write!(
                    f,
                    "The number of items written did not match the number reported."
                )
            }
        }
    }
}

impl std::error::Error for MsgPackError {}

impl<W: Write> PrimitiveWriter for MsgPackInterpreter<W> {
    type Repr = ();
    type Error = std::io::Error;

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
        write_u64(&mut self.writer, value)?;
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
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::BigIntTooLarge(value),
            ))
        }
    }

    fn write_big_uint(mut self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let bytes = value.to_bytes_be();
        if let Ok(n) = u32::try_from(bytes.len()) {
            write_ext_meta(&mut self.writer, n, BIG_UINT_EXT)?;
            self.writer.write_all(bytes.as_slice())?;
            Ok(())
        } else {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::BigUIntTooLarge(value),
            ))
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

impl<W: Write> StructuralWriter for MsgPackInterpreter<W> {
    type Header = Self;
    type Body = MsgPackBodyInterpreter<W>;

    fn record(mut self, num_attrs: usize) -> Result<Self::Header, Self::Error> {
        self.expecting = to_expecting(num_attrs)?;
        write_map_len(&mut self.writer, self.expecting)?;
        Ok(self)
    }
}

fn to_expecting(n: usize) -> Result<u32, std::io::Error> {
    u32::try_from(n)
        .map_err(|_| std::io::Error::new(ErrorKind::InvalidData, MsgPackError::ToManyAttrs(n)))
}

impl<W: Write> HeaderWriter for MsgPackInterpreter<W> {
    type Repr = ();
    type Error = std::io::Error;
    type Body = MsgPackBodyInterpreter<W>;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        if self.expecting == 0 {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::WrongNumberOfAttrs,
            ))
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
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::WrongNumberOfAttrs,
            ))
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

impl<W: Write> BodyWriter for MsgPackBodyInterpreter<W> {
    type Repr = ();
    type Error = std::io::Error;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        if self.expecting == 0 {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::WrongNumberOfItems,
            ))
        } else if self.kind == RecordBodyKind::MapLike {
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::IncorrectRecordKind,
            ))
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
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::WrongNumberOfItems,
            ))
        } else {
            match self.kind {
                RecordBodyKind::ArrayLike => {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidData,
                        MsgPackError::IncorrectRecordKind,
                    ));
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
            Err(std::io::Error::new(
                ErrorKind::InvalidData,
                MsgPackError::WrongNumberOfItems,
            ))
        } else {
            Ok(())
        }
    }
}
