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

use crate::form::structural::read::event::ReadEvent;
use crate::form::structural::read::recognizer::Recognizer;
use crate::form::structural::read::{ReadError, StructuralReadable};
use crate::form::structural::write::interpreters::msgpack::{BIG_INT_EXT, BIG_UINT_EXT};
use bytes::{Buf, BufMut, BytesMut};
use either::Either;
use num_bigint::{BigInt, BigUint, Sign};
use rmp::decode::{read_str_len, ValueReadError};
use rmp::Marker;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;

macro_rules! feed {
    ($e:expr) => {
        match $e {
            Some(Ok(_)) => {
                return Err(MsgPackReadError::UncomsumedData);
            }
            Some(Err(e)) => {
                return Err(e.into());
            }
            _ => {}
        }
    };
}

fn feed<T, E: Into<MsgPackReadError>>(maybe: Option<Result<T, E>>) -> Result<(), MsgPackReadError> {
    feed!(maybe);
    Ok(())
}

/// Attempt to read a [`StructuralReadable`] type from MessagePack data in a buffer.
pub fn read_from_msg_pack<T: StructuralReadable, R: Buf>(
    input: &mut R,
) -> Result<T, MsgPackReadError> {
    let mut str_buf = BytesMut::new();
    let marker = read_marker(input)?;
    match marker {
        Marker::Null => Ok(T::read_extant()?),
        Marker::True => Ok(T::read_bool(true)?),
        Marker::False => Ok(T::read_bool(false)?),
        Marker::FixPos(n) => Ok(T::read_i32(n as i32)?),
        Marker::FixNeg(n) => Ok(T::read_i32(n as i32)?),
        Marker::I8 => compose_simple(input, Buf::get_i8, T::read_i32),
        Marker::I16 => compose_simple(input, Buf::get_i16, T::read_i32),
        Marker::I32 => compose_simple(input, Buf::get_i32, T::read_i32),
        Marker::I64 => compose_simple(input, Buf::get_i64, T::read_i64),
        Marker::U8 => compose_simple(input, Buf::get_u8, T::read_i32),
        Marker::U16 => compose_simple(input, Buf::get_u16, T::read_i32),
        Marker::U32 => compose_simple(input, Buf::get_u32, T::read_u32),
        Marker::U64 => compose_simple(input, Buf::get_u64, T::read_u64),
        Marker::F32 => compose_simple(input, Buf::get_f32, T::read_f64),
        Marker::F64 => compose_simple(input, Buf::get_f64, T::read_f64),
        Marker::FixStr(len) => read_string(input, &mut str_buf, len as u32, T::read_text),
        Marker::Str8 => {
            let len = if input.remaining() < std::mem::size_of::<u8>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u8() as u32
            };
            read_string(input, &mut str_buf, len, T::read_text)
        }
        Marker::Str16 => {
            let len = if input.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            read_string(input, &mut str_buf, len, T::read_text)
        }
        Marker::Str32 => {
            let len = if input.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            read_string(input, &mut str_buf, len, T::read_text)
        }
        Marker::Bin8 => {
            let len = if input.remaining() < std::mem::size_of::<u8>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u8() as u32
            };
            let blob = read_blob(input, len)?;
            T::read_blob(blob).map_err(Into::into)
        }
        Marker::Bin16 => {
            let len = if input.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            let blob = read_blob(input, len)?;
            T::read_blob(blob).map_err(Into::into)
        }
        Marker::Bin32 => {
            let len = if input.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            let blob = read_blob(input, len)?;
            T::read_blob(blob).map_err(Into::into)
        }
        Marker::FixMap(n) => {
            let mut recognizer = T::make_recognizer();
            let result = read_record(input, &mut str_buf, n as u32, &mut recognizer);
            complete(result, recognizer)
        }
        Marker::Map16 => {
            let len = if input.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            let mut recognizer = T::make_recognizer();
            let result = read_record(input, &mut str_buf, len, &mut recognizer);
            complete(result, recognizer)
        }
        Marker::Map32 => {
            let len = if input.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            let mut recognizer = T::make_recognizer();
            let result = read_record(input, &mut str_buf, len, &mut recognizer);
            complete(result, recognizer)
        }
        marker if is_ext(marker) => match read_ext(input, marker)? {
            Either::Left(n) => T::read_big_int(n),
            Either::Right(n) => T::read_big_uint(n),
        }
        .map_err(Into::into),
        ow => Err(MsgPackReadError::InvalidMarker(ow)),
    }
}

fn complete<R>(
    result: Result<Option<R::Target>, MsgPackReadError>,
    mut recognizer: R,
) -> Result<R::Target, MsgPackReadError>
where
    R: Recognizer,
{
    result.and_then(move |maybe| {
        if let Some(t) = maybe {
            Ok(t)
        } else {
            match recognizer.try_flush() {
                Some(Ok(t)) => Ok(t),
                Some(Err(e)) => Err(e.into()),
                _ => Err(MsgPackReadError::Incomplete),
            }
        }
    })
}

#[derive(Debug, PartialEq)]
pub enum MsgPackReadError {
    /// The parsed strucuture was not valid for the target type.
    Structure(ReadError),
    /// The MessagePack data contained invalid UTF8 in a string.
    StringDecode(Utf8Error),
    /// An unexpected MessagePack marker was encountered.
    InvalidMarker(Marker),
    /// An unknown extension type occurred in the data.
    UnknownExtType(i8),
    /// A big integer contained 0 bytes (at least one is required for the sign).
    EmptyBigInt,
    /// The input terminated mid-way through a record.
    Incomplete,
    /// Not all input was consumed.
    UncomsumedData,
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
                write!(f, "{} is not a recognized extension code.", code)
            }
            MsgPackReadError::EmptyBigInt => {
                write!(f, "A big integer consisted of 0 bytes.")
            }
            MsgPackReadError::Incomplete => {
                write!(f, "The input ended part way through a record.")
            }
            MsgPackReadError::UncomsumedData => {
                write!(f, "Not all of the input was consumed.")
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

/// Read extnesion data. Curently we only use this for big integers.
fn read_ext<R>(input: &mut R, marker: Marker) -> Result<Either<BigInt, BigUint>, MsgPackReadError>
where
    R: Buf,
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
                    let blob = read_blob(input, len - 1)?;
                    Ok(Either::Left(BigInt::from_bytes_be(sign, blob.as_slice())))
                }
            }
            BIG_UINT_EXT => {
                let blob = read_blob(input, len)?;
                Ok(Either::Right(BigUint::from_bytes_be(blob.as_slice())))
            }
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
            if input.remaining() < std::mem::size_of::<u8>() {
                Err(MsgPackReadError::Incomplete)
            } else {
                Ok(input.get_u8() as u32)
            }
        }
        Marker::Ext16 => {
            if input.remaining() < std::mem::size_of::<u16>() {
                Err(MsgPackReadError::Incomplete)
            } else {
                Ok(input.get_u16() as u32)
            }
        }
        Marker::Ext32 => {
            if input.remaining() < std::mem::size_of::<u32>() {
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
    to_value: F2,
) -> Result<U, MsgPackReadError>
where
    R: Buf,
    F1: Fn(&mut R) -> S,
    S: Into<T>,
    F2: FnOnce(T) -> Result<U, ReadError>,
{
    if input.remaining() < std::mem::size_of::<S>() {
        Err(MsgPackReadError::Incomplete)
    } else {
        Ok(to_value(read(input).into())?)
    }
}

fn compose_feed<R, F1, F2, S, T, U>(
    input: &mut R,
    read: F1,
    to_value: F2,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    F1: Fn(&mut R) -> S,
    S: Into<T>,
    F2: FnOnce(T) -> Option<Result<U, ReadError>>,
{
    if input.remaining() < std::mem::size_of::<S>() {
        Err(MsgPackReadError::Incomplete)
    } else {
        feed!(to_value(read(input).into()));
        Ok(())
    }
}

fn read_string<R, F, T>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    len: u32,
    to_value: F,
) -> Result<T, MsgPackReadError>
where
    R: Buf,
    F: FnOnce(Cow<'_, str>) -> Result<T, ReadError>,
{
    let len = usize::try_from(len).expect("u32 did not fit into usize");
    str_buf.clear();
    str_buf.put(reader.take(len));
    if str_buf.len() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        let string = std::str::from_utf8(str_buf.as_ref())?;
        Ok(to_value(Cow::Borrowed(string))?)
    }
}

fn feed_string<R, F, T>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    len: u32,
    to_value: F,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    F: FnOnce(Cow<'_, str>) -> Option<Result<T, ReadError>>,
{
    let len = usize::try_from(len).expect("u32 did not fit into usize");
    str_buf.clear();
    str_buf.put(reader.take(len));
    if str_buf.len() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        let string = std::str::from_utf8(str_buf.as_ref())?;
        feed!(to_value(Cow::Borrowed(string)));
        Ok(())
    }
}

fn read_blob<R>(reader: &mut R, len: u32) -> Result<Vec<u8>, MsgPackReadError>
where
    R: Buf,
{
    let len = usize::try_from(len).expect("u32 did not fit into usize");
    let bytes = reader.copy_to_bytes(len);
    if bytes.len() < len {
        Err(MsgPackReadError::Incomplete)
    } else {
        let blob = Vec::from(bytes.as_ref());
        Ok(blob)
    }
}

fn read_sub_record<R, Rec>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    attrs: u32,
    recognizer: &mut Rec,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    match read_record(reader, str_buf, attrs, recognizer) {
        Ok(Some(_)) => Err(MsgPackReadError::UncomsumedData),
        Err(e) => Err(e),
        _ => Ok(()),
    }
}

fn read_record<R, Rec>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    attrs: u32,
    recognizer: &mut Rec,
) -> Result<Option<Rec::Target>, MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    for _ in 0..attrs {
        let name_len = read_str_len(&mut reader.reader())?;
        feed_string(reader, str_buf, name_len, |name| {
            recognizer.feed_event(ReadEvent::StartAttribute(name))
        })?;
        push_value_dynamic(reader, str_buf, recognizer)?;
        feed!(recognizer.feed_event(ReadEvent::EndAttribute));
    }
    let (body_len, is_map) = read_body_len(reader)?;
    feed!(recognizer.feed_event(ReadEvent::StartBody));
    if is_map {
        read_map_body(reader, str_buf, body_len, recognizer)?;
    } else {
        read_array_body(reader, str_buf, body_len, recognizer)?;
    }
    recognizer
        .feed_event(ReadEvent::EndRecord)
        .transpose()
        .map_err(Into::into)
}

fn push_value_dynamic<R, Rec>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    recognizer: &mut Rec,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    let marker = read_marker(reader)?;
    push_value(reader, str_buf, recognizer, marker)
}

fn push_value<R, Rec>(
    reader: &mut R,
    str_buf: &mut BytesMut,
    recognizer: &mut Rec,
    marker: Marker,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    match marker {
        Marker::Null => feed(recognizer.feed_event(ReadEvent::Extant)),
        Marker::True => feed(recognizer.feed_event(true.into())),
        Marker::False => feed(recognizer.feed_event(false.into())),
        Marker::FixPos(n) => feed(recognizer.feed_event(n.into())),
        Marker::FixNeg(n) => feed(recognizer.feed_event(n.into())),
        Marker::I8 => compose_feed(reader, Buf::get_i8, |n: i8| recognizer.feed_event(n.into())),
        Marker::I16 => compose_feed(reader, Buf::get_i16, |n: i16| {
            recognizer.feed_event(n.into())
        }),
        Marker::I32 => compose_feed(reader, Buf::get_i32, |n: i32| {
            recognizer.feed_event(n.into())
        }),
        Marker::I64 => compose_feed(reader, Buf::get_i64, |n: i64| {
            recognizer.feed_event(n.into())
        }),
        Marker::U8 => compose_feed(reader, Buf::get_u8, |n: u8| recognizer.feed_event(n.into())),
        Marker::U16 => compose_feed(reader, Buf::get_u16, |n: u16| {
            recognizer.feed_event(n.into())
        }),
        Marker::U32 => compose_feed(reader, Buf::get_u32, |n: u32| {
            recognizer.feed_event(n.into())
        }),
        Marker::U64 => compose_feed(reader, Buf::get_u64, |n: u64| {
            recognizer.feed_event(n.into())
        }),
        Marker::F32 => compose_feed(reader, Buf::get_f32, |n: f32| {
            recognizer.feed_event(n.into())
        }),
        Marker::F64 => compose_feed(reader, Buf::get_f64, |n: f64| {
            recognizer.feed_event(n.into())
        }),
        Marker::FixStr(len) => feed_string(reader, str_buf, len as u32, |t| {
            recognizer.feed_event(t.into())
        }),
        Marker::Str8 => {
            let len = if reader.remaining() < std::mem::size_of::<u8>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u8() as u32
            };
            feed_string(reader, str_buf, len, |t| recognizer.feed_event(t.into()))
        }
        Marker::Str16 => {
            let len = if reader.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            feed_string(reader, str_buf, len, |t| recognizer.feed_event(t.into()))
        }
        Marker::Str32 => {
            let len = if reader.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32()
            };
            feed_string(reader, str_buf, len, |t| recognizer.feed_event(t.into()))
        }
        Marker::Bin8 => {
            let len = if reader.remaining() < std::mem::size_of::<u8>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u8() as u32
            };
            let blob = read_blob(reader, len)?;
            feed(recognizer.feed_event(blob.into()))
        }
        Marker::Bin16 => {
            let len = if reader.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            let blob = read_blob(reader, len)?;
            feed(recognizer.feed_event(blob.into()))
        }
        Marker::Bin32 => {
            let len = if reader.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32() as u32
            };
            let blob = read_blob(reader, len)?;
            feed(recognizer.feed_event(blob.into()))
        }
        Marker::FixMap(n) => read_sub_record(reader, str_buf, n as u32, recognizer),
        Marker::Map16 => {
            let len = if reader.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u16() as u32
            };
            read_sub_record(reader, str_buf, len, recognizer)
        }
        Marker::Map32 => {
            let len = if reader.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                reader.get_u32()
            };
            read_sub_record(reader, str_buf, len, recognizer)
        }
        marker if is_ext(marker) => feed(match read_ext(reader, marker)? {
            Either::Left(n) => recognizer.feed_event(n.into()),
            Either::Right(n) => recognizer.feed_event(n.into()),
        }),
        ow => Err(MsgPackReadError::InvalidMarker(ow)),
    }
}

const SLOT_MARKER: Marker = Marker::FixArray(2);

fn read_array_body<R, Rec>(
    input: &mut R,
    str_buf: &mut BytesMut,
    items: u32,
    recognizer: &mut Rec,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    for _ in 0..items {
        let marker = read_marker(input)?;
        if marker == SLOT_MARKER {
            push_value_dynamic(input, str_buf, recognizer)?;
            feed!(recognizer.feed_event(ReadEvent::Slot));
            push_value_dynamic(input, str_buf, recognizer)?;
        } else {
            push_value(input, str_buf, recognizer, marker)?;
        }
    }
    Ok(())
}

fn read_map_body<R, Rec>(
    input: &mut R,
    str_buf: &mut BytesMut,
    items: u32,
    recognizer: &mut Rec,
) -> Result<(), MsgPackReadError>
where
    R: Buf,
    Rec: Recognizer,
{
    for _ in 0..items {
        push_value_dynamic(input, str_buf, recognizer)?;
        feed!(recognizer.feed_event(ReadEvent::Slot));
        push_value_dynamic(input, str_buf, recognizer)?;
    }
    Ok(())
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
            let len = if input.remaining() < std::mem::size_of::<u16>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u16() as u32
            };
            Ok((len, marker == Marker::Map16))
        }
        marker @ Marker::Map32 | marker @ Marker::Array32 => {
            let len = if input.remaining() < std::mem::size_of::<u32>() {
                return Err(MsgPackReadError::Incomplete);
            } else {
                input.get_u32()
            };
            Ok((len, marker == Marker::Map32))
        }
        ow => Err(MsgPackReadError::InvalidMarker(ow)),
    }
}
