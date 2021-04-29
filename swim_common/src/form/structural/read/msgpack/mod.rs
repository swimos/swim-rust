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

use crate::form::structural::read::{BodyReader, HeaderReader, ReadError, StructuralReadable};
use bytes::Buf;
use rmp::decode::{read_str_len, ValueReadError};
use rmp::Marker;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::str::Utf8Error;

#[derive(Debug, PartialEq)]
pub enum MsgPackReadError {
    /// The parsed strucuture was not valid for the target type.
    Structure(ReadError),
    StringDecode(Utf8Error),
    InvalidMarker(Marker),
    UnknownExtType(i8),
    EmptyBigInt,
    Incomplete,
}

impl Display for MsgPackReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MsgPackReadError::Structure(err) => {
                write!(f, "Invalid structure: {}", err)
            }
            MsgPackReadError::StringDecode(_) => {
                write!(f, "A string value contained invalid UTF8.")
            }
            MsgPackReadError::InvalidMarker(marker) => {
                write!(f, "Unexpected message pack marker: {:?}", marker)
            }
            MsgPackReadError::UnknownExtType(code) => {
                write!(f, "{} is not a regognized extension code.", code)
            }
            MsgPackReadError::EmptyBigInt => {
                write!(f, "A big integer consisted of 0 bytes.")
            }
            MsgPackReadError::Incomplete => {
                write!(f, "The input ended part way through a record.")
            }
        }
    }
}

impl std::error::Error for MsgPackReadError {}

impl From<ReadError> for MsgPackReadError {
    fn from(err: ReadError) -> Self {
        MsgPackReadError::Structure(err)
    }
}

impl From<Utf8Error> for MsgPackReadError {
    fn from(err: Utf8Error) -> Self {
        MsgPackReadError::StringDecode(err)
    }
}

impl From<ValueReadError> for MsgPackReadError {
    fn from(err: ValueReadError) -> Self {
        match err {
            ValueReadError::TypeMismatch(marker) => MsgPackReadError::InvalidMarker(marker),
            _ => MsgPackReadError::Incomplete,
        }
    }
}

fn read_marker<R>(input: &mut R) -> Result<Marker, MsgPackReadError>
where
    R: Buf,
{
    if !input.has_remaining() {
        Err(MsgPackReadError::Incomplete)
    } else {
        let marker = Marker::from_u8(input.get_u8());
        Ok(marker)
    }
}

use crate::form::structural::write::interpreters::msgpack::{BIG_INT_EXT, BIG_UINT_EXT};
use num_bigint::{BigInt, BigUint, Sign};
use std::fmt::{Display, Formatter};

pub fn read_from_msg_pack<T: StructuralReadable, R: Buf>(
    input: &mut R,
) -> Result<T, MsgPackReadError> {
    let marker = read_marker(input)?;
    match marker {
        Marker::Null => Ok(T::read_extant()?),
        Marker::True => Ok(T::read_bool(true)?),
        Marker::False => Ok(T::read_bool(false)?),
        Marker::FixPos(n) => Ok(T::read_i32(n as i32)?),
        Marker::FixNeg(n) => Ok(T::read_i32(n as i32)?),
        Marker::I8 => compose_simple(input, Buf::get_i8, 1, T::read_i32),
        Marker::I16 => compose_simple(input, Buf::get_i16, 2, T::read_i32),
        Marker::I32 => compose_simple(input, Buf::get_i32, 4, T::read_i32),
        Marker::I64 => compose_simple(input, Buf::get_i64, 8, T::read_i64),
        Marker::U8 => compose_simple(input, Buf::get_u8, 1, T::read_i32),
        Marker::U16 => compose_simple(input, Buf::get_u16, 2, T::read_i32),
        Marker::U32 => compose_simple(input, Buf::get_u32, 4, T::read_u32),
        Marker::U64 => compose_simple(input, Buf::get_u64, 8, T::read_u64),
        Marker::F32 => compose_simple(input, Buf::get_f32, 4, T::read_f64),
        Marker::F64 => compose_simple(input, Buf::get_f64, 8, T::read_f64),
        Marker::FixStr(len) => read_string(input, len as u32, T::read_text),
        Marker::Str8 => {
            let len = if input.remaining() < 1 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u8() as u32
            };
            read_string(input, len, T::read_text)
        }
        Marker::Str16 => {
            let len = if input.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            read_string(input, len, T::read_text)
        }
        Marker::Str32 => {
            let len = if input.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            read_string(input, len, T::read_text)
        }
        Marker::Bin8 => {
            let len = if input.remaining() < 1 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u8() as u32
            };
            read_blob(input, len, T::read_blob)
        }
        Marker::Bin16 => {
            let len = if input.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            read_blob(input, len, T::read_blob)
        }
        Marker::Bin32 => {
            let len = if input.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            read_blob(input, len, T::read_blob)
        }
        Marker::FixMap(n) => {
            let body_reader = read_record(input, n as u32, T::record_reader()?)?;
            Ok(T::try_terminate(body_reader)?)
        }
        Marker::Map16 => {
            let len = if input.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            let body_reader = read_record(input, len, T::record_reader()?)?;
            Ok(T::try_terminate(body_reader)?)
        }
        Marker::Map32 => {
            let len = if input.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            let body_reader = read_record(input, len, T::record_reader()?)?;
            Ok(T::try_terminate(body_reader)?)
        }
        marker if is_ext(marker) => {
            let read_big_int = |_: &mut (), n| T::read_big_int(n);
            let read_big_uint = |_: &mut (), n| T::read_big_uint(n);
            read_ext(input, marker, &mut (), read_big_int, read_big_uint)
        }
        ow => Err(MsgPackReadError::InvalidMarker(ow)),
    }
}

fn read_ext<R, B, F, G, T>(
    input: &mut R,
    marker: Marker,
    builder: &mut B,
    on_big_int: F,
    on_big_uint: G,
) -> Result<T, MsgPackReadError>
where
    R: Buf,
    F: for<'a> FnOnce(&'a mut B, BigInt) -> Result<T, ReadError>,
    G: for<'a> FnOnce(&'a mut B, BigUint) -> Result<T, ReadError>,
{
    let len = read_ext_size(input, marker)?;
    if input.remaining() < 1 {
        Err(MsgPackReadError::Incomplete)
    } else {
        let ext_type = input.get_i8();
        match ext_type {
            BIG_INT_EXT => {
                if len == 0 {
                    Err(MsgPackReadError::EmptyBigInt)
                } else if input.remaining() < 1 {
                    Err(MsgPackReadError::Incomplete)
                } else {
                    let sig = input.get_u8();
                    let sign = if sig == 0 { Sign::Minus } else { Sign::NoSign };
                    read_blob(input, len - 1, |blob| {
                        let n = BigInt::from_bytes_be(sign, blob.as_slice());
                        Ok(on_big_int(builder, n)?)
                    })
                }
            }
            BIG_UINT_EXT => read_blob(input, len, |blob| {
                let n = BigUint::from_bytes_be(blob.as_slice());
                Ok(on_big_uint(builder, n)?)
            }),
            _ => Err(MsgPackReadError::UnknownExtType(ext_type)),
        }
    }
}

fn is_ext(marker: Marker) -> bool {
    matches!(
        marker,
        Marker::FixExt1
            | Marker::FixExt2
            | Marker::FixExt4
            | Marker::FixExt8
            | Marker::FixExt16
            | Marker::Ext8
            | Marker::Ext16
            | Marker::Ext32
    )
}

fn read_ext_size<R>(input: &mut R, marker: Marker) -> Result<u32, MsgPackReadError>
where
    R: Buf,
{
    match marker {
        Marker::FixExt1 => Ok(1),
        Marker::FixExt2 => Ok(2),
        Marker::FixExt4 => Ok(4),
        Marker::FixExt8 => Ok(8),
        Marker::FixExt16 => Ok(16),
        Marker::Ext8 => {
            if input.remaining() < 1 {
                Err(MsgPackReadError::Incomplete)
            } else {
                Ok(input.get_u8() as u32)
            }
        }
        Marker::Ext16 => {
            if input.remaining() < 2 {
                Err(MsgPackReadError::Incomplete)
            } else {
                Ok(input.get_u16() as u32)
            }
        }
        Marker::Ext32 => {
            if input.remaining() < 4 {
                Err(MsgPackReadError::Incomplete)
            } else {
                Ok(input.get_u32())
            }
        }
        _ => Err(MsgPackReadError::InvalidMarker(marker)),
    }
}

fn compose_simple<R, F1, F2, S, T, U>(
    input: &mut R,
    read: F1,
    len: usize,
    to_value: F2,
) -> Result<U, MsgPackReadError>
where
    R: Buf,
    F1: Fn(&mut R) -> S,
    S: Into<T>,
    F2: FnOnce(T) -> Result<U, ReadError>,
{
    if input.remaining() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        Ok(to_value(read(input).into())?)
    }
}

fn read_string<R, F, T>(reader: &mut R, len: u32, to_value: F) -> Result<T, MsgPackReadError>
where
    R: Buf,
    F: FnOnce(Cow<'_, str>) -> Result<T, ReadError>,
{
    let len = usize::try_from(len).expect("u32 did not fit into usize");
    let string_bytes = reader.copy_to_bytes(len);
    if string_bytes.len() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        let string = std::str::from_utf8(string_bytes.as_ref())?;
        Ok(to_value(Cow::Borrowed(string))?)
    }
}

fn read_blob<R, F, T>(reader: &mut R, len: u32, to_value: F) -> Result<T, MsgPackReadError>
where
    R: Buf,
    F: FnOnce(Vec<u8>) -> Result<T, ReadError>,
{
    let len = usize::try_from(len).expect("u32 did not fit into usize");
    let bytes = reader.copy_to_bytes(len);
    if bytes.len() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        let blob = Vec::from(bytes.as_ref());
        Ok(to_value(blob)?)
    }
}

fn read_record<R, H>(
    reader: &mut R,
    attrs: u32,
    mut header_reader: H,
) -> Result<H::Body, MsgPackReadError>
where
    R: Buf,
    H: HeaderReader,
{
    for _ in 0..attrs {
        let name_len = read_str_len(&mut reader.reader())?;
        let mut delegate = read_string(reader, name_len, move |name| {
            header_reader.read_attribute(name)
        })?;
        delegate = push_value_dynamic(reader, delegate)?;
        header_reader = H::restore(delegate)?;
    }
    let (body_len, is_map) = read_body_len(reader)?;
    let body_reader = header_reader.start_body()?;
    if is_map {
        read_map_body(reader, body_len, body_reader)
    } else {
        read_array_body(reader, body_len, body_reader)
    }
}

fn push_value_dynamic<R, B>(reader: &mut R, body_reader: B) -> Result<B, MsgPackReadError>
where
    R: Buf,
    B: BodyReader,
{
    let marker = read_marker(reader)?;
    push_value(reader, body_reader, marker)
}

fn push_value<R, B>(
    reader: &mut R,
    mut body_reader: B,
    marker: Marker,
) -> Result<B, MsgPackReadError>
where
    R: Buf,
    B: BodyReader,
{
    match marker {
        Marker::Null => body_reader.push_extant()?,
        Marker::True => body_reader.push_bool(true)?,
        Marker::False => body_reader.push_bool(false)?,
        Marker::FixPos(n) => body_reader.push_i32(n as i32)?,
        Marker::FixNeg(n) => body_reader.push_i32(n as i32)?,
        Marker::I8 => compose_simple(reader, Buf::get_i8, 1, |n| body_reader.push_i32(n))?,
        Marker::I16 => compose_simple(reader, Buf::get_i16, 2, |n| body_reader.push_i32(n))?,
        Marker::I32 => compose_simple(reader, Buf::get_i32, 4, |n| body_reader.push_i32(n))?,
        Marker::I64 => compose_simple(reader, Buf::get_i64, 8, |n| body_reader.push_i64(n))?,
        Marker::U8 => compose_simple(reader, Buf::get_u8, 1, |n| body_reader.push_i32(n))?,
        Marker::U16 => compose_simple(reader, Buf::get_u16, 2, |n| body_reader.push_i32(n))?,
        Marker::U32 => compose_simple(reader, Buf::get_u32, 4, |n| body_reader.push_u32(n))?,
        Marker::U64 => compose_simple(reader, Buf::get_u64, 8, |n| body_reader.push_u64(n))?,
        Marker::F32 => compose_simple(reader, Buf::get_f32, 2, |n| body_reader.push_f64(n))?,
        Marker::F64 => compose_simple(reader, Buf::get_f64, 2, |n| body_reader.push_f64(n))?,
        Marker::FixStr(len) => read_string(reader, len as u32, |t| body_reader.push_text(t))?,
        Marker::Str8 => {
            let len = if reader.remaining() < 1 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u8() as u32
            };
            read_string(reader, len, |t| body_reader.push_text(t))?
        }
        Marker::Str16 => {
            let len = if reader.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            read_string(reader, len, |t| body_reader.push_text(t))?
        }
        Marker::Str32 => {
            let len = if reader.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32()
            };
            read_string(reader, len, |t| body_reader.push_text(t))?
        }
        Marker::Bin8 => {
            let len = if reader.remaining() < 1 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u8() as u32
            };
            read_blob(reader, len, |blob| body_reader.push_blob(blob))?
        }
        Marker::Bin16 => {
            let len = if reader.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            read_blob(reader, len, |blob| body_reader.push_blob(blob))?
        }
        Marker::Bin32 => {
            let len = if reader.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32() as u32
            };
            read_blob(reader, len, |blob| body_reader.push_blob(blob))?
        }
        Marker::FixMap(n) => {
            let delegate = body_reader.push_record()?;
            let body_delegate = read_record(reader, n as u32, delegate)?;
            body_reader = B::restore(body_delegate)?;
            true
        }
        Marker::Map16 => {
            let len = if reader.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            let delegate = body_reader.push_record()?;
            let body_delegate = read_record(reader, len, delegate)?;
            body_reader = B::restore(body_delegate)?;
            true
        }
        Marker::Map32 => {
            let len = if reader.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32()
            };
            let delegate = body_reader.push_record()?;
            let body_delegate = read_record(reader, len, delegate)?;
            body_reader = B::restore(body_delegate)?;
            true
        }
        marker if is_ext(marker) => {
            read_ext(
                reader,
                marker,
                &mut body_reader,
                |r, n| r.push_big_int(n),
                |r, n| r.push_big_uint(n),
            )?;
            true
        }
        ow => {
            return Err(MsgPackReadError::InvalidMarker(ow));
        }
    };
    Ok(body_reader)
}

fn read_array_body<R, B>(
    input: &mut R,
    items: u32,
    mut body_reader: B,
) -> Result<B, MsgPackReadError>
where
    R: Buf,
    B: BodyReader,
{
    for _ in 0..items {
        let marker = read_marker(input)?;
        if marker == Marker::FixArray(2) {
            body_reader = push_value_dynamic(input, body_reader)?;
            body_reader.start_slot()?;
            body_reader = push_value_dynamic(input, body_reader)?;
        } else {
            body_reader = push_value(input, body_reader, marker)?;
        }
    }
    Ok(body_reader)
}

fn read_map_body<R, B>(input: &mut R, items: u32, mut body_reader: B) -> Result<B, MsgPackReadError>
where
    R: Buf,
    B: BodyReader,
{
    for _ in 0..items {
        body_reader = push_value_dynamic(input, body_reader)?;
        body_reader.start_slot()?;
        body_reader = push_value_dynamic(input, body_reader)?;
    }
    Ok(body_reader)
}

fn read_body_len<R>(input: &mut R) -> Result<(u32, bool), MsgPackReadError>
where
    R: Buf,
{
    let marker = read_marker(input)?;
    match marker {
        Marker::FixMap(n) => Ok((n as u32, true)),
        Marker::FixArray(n) => Ok((n as u32, false)),
        marker @ Marker::Map16 | marker @ Marker::Array16 => {
            let len = if input.remaining() < 2 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            Ok((len, marker == Marker::Map16))
        }
        marker @ Marker::Map32 | marker @ Marker::Array32 => {
            let len = if input.remaining() < 4 {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            Ok((len, marker == Marker::Map32))
        }
        ow => Err(MsgPackReadError::InvalidMarker(ow)),
    }
}
