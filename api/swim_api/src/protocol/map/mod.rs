// Copyright 2015-2021 Swim Inc.
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

use crate::error::{FrameIoError, InvalidFrame};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use swim_form::{
    structural::{read::recognizer::RecognizerReadable, write::StructuralWritable},
    Form,
};
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, RecognizerDecoder};
use tokio_util::codec::{Decoder, Encoder};

mod parser;
#[cfg(test)]
mod tests;

pub use parser::extract_header;

/// An operation that can be applied to a map lane. This type is used by map uplinks and downlinks
/// to describe alterations to the lane.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Form)]
pub enum MapOperation<K, V> {
    #[form(tag = "update")]
    Update {
        key: K,
        #[form(body)]
        value: V,
    },
    #[form(tag = "remove")]
    Remove {
        #[form(header)]
        key: K,
    },
    #[form(tag = "clear")]
    Clear,
}

pub type RawMapOperation = MapOperation<Bytes, BytesMut>;
pub type RawMapOperationMut = MapOperation<BytesMut, BytesMut>;

#[derive(Debug, Default, Clone, Copy)]
pub struct MapOperationEncoder;
#[derive(Debug, Default, Clone, Copy)]
pub struct RawMapOperationEncoder;

pub enum MapOperationDecoderState<K, V> {
    ReadingHeader,
    ReadingKey {
        remaining: usize,
        value_size: Option<usize>,
    },
    AfterKey {
        key: Option<K>,
        remaining: usize,
        value_size: Option<usize>,
    },
    ReadingValue {
        key: Option<K>,
        remaining: usize,
    },
    AfterValue {
        key_value: Option<(K, V)>,
        remaining: usize,
    },
    Discarding {
        error: Option<AsyncParseError>,
        remaining: usize,
    },
}

pub struct MapOperationDecoder<K: RecognizerReadable, V: RecognizerReadable> {
    state: MapOperationDecoderState<K, V>,
    key_recognizer: RecognizerDecoder<K::Rec>,
    value_recognizer: RecognizerDecoder<V::Rec>,
}

impl<K: RecognizerReadable, V: RecognizerReadable> Default for MapOperationDecoder<K, V> {
    fn default() -> Self {
        Self {
            state: MapOperationDecoderState::ReadingHeader,
            key_recognizer: RecognizerDecoder::new(K::make_recognizer()),
            value_recognizer: RecognizerDecoder::new(V::make_recognizer()),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawMapOperationDecoder;

const UPDATE: u8 = 0;
const REMOVE: u8 = 1;
const CLEAR: u8 = 2;
const TAKE: u8 = 3;
const DROP: u8 = 4;

use super::{LEN_SIZE, TAG_SIZE};

const OVERSIZE_KEY: &str = "Key too large.";
const OVERSIZE_VALUE: &str = "Value too large.";
const BAD_TAG: &str = "Invalid map operation tag: ";

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<MapOperation<K, V>> for RawMapOperationEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: MapOperation<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            MapOperation::Update { key, value } => {
                let key_bytes = key.as_ref();
                let value_bytes = value.as_ref();
                dst.reserve(TAG_SIZE + 2 * LEN_SIZE + key_bytes.len() + value_bytes.len());
                dst.put_u8(UPDATE);
                let key_len = u64::try_from(key_bytes.len()).expect(OVERSIZE_KEY);
                let value_len = u64::try_from(value_bytes.len()).expect(OVERSIZE_VALUE);
                dst.put_u64(key_len);
                dst.put_u64(value_len);
                dst.put(key_bytes);
                dst.put(value_bytes);
            }
            MapOperation::Remove { key } => {
                let key_bytes = key.as_ref();
                dst.reserve(TAG_SIZE + LEN_SIZE + key_bytes.len());
                dst.put_u8(REMOVE);
                let key_len = u64::try_from(key_bytes.len()).expect("Key too large.");
                dst.put_u64(key_len);
                dst.put(key_bytes);
            }
            MapOperation::Clear => {
                dst.reserve(TAG_SIZE);
                dst.put_u8(CLEAR);
            }
        }
        Ok(())
    }
}

impl Decoder for RawMapOperationDecoder {
    type Item = RawMapOperationMut;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            src.reserve(TAG_SIZE);
            return Ok(None);
        }
        match src.as_ref()[0] {
            UPDATE => {
                if src.remaining() < TAG_SIZE + 2 * LEN_SIZE {
                    let required = TAG_SIZE + 2 * LEN_SIZE;
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                let mut header = &src.as_ref()[1..2 * LEN_SIZE + 1];
                let key_len = header.get_u64() as usize;
                let value_len = header.get_u64() as usize;
                let required = TAG_SIZE + 2 * LEN_SIZE + key_len + value_len;
                if src.remaining() < required {
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                src.advance(TAG_SIZE + 2 * LEN_SIZE);
                let key = src.split_to(key_len);
                let value = src.split_to(value_len);
                Ok(Some(RawMapOperationMut::Update { key, value }))
            }
            REMOVE => {
                if src.remaining() < TAG_SIZE + LEN_SIZE {
                    let required = TAG_SIZE + LEN_SIZE;
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                let mut header = &src.as_ref()[1..LEN_SIZE + 1];
                let key_len = header.get_u64() as usize;
                let required = TAG_SIZE + LEN_SIZE + key_len;
                if src.remaining() < required {
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                src.advance(TAG_SIZE + LEN_SIZE);
                let key = src.split_to(key_len);
                Ok(Some(RawMapOperationMut::Remove { key }))
            }
            CLEAR => {
                src.advance(TAG_SIZE);
                Ok(Some(RawMapOperationMut::Clear))
            }
            ow => Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                problem: Text::from(format!("{}{}", BAD_TAG, ow)),
            })),
        }
    }
}

impl<K: RecognizerReadable, V: RecognizerReadable> Decoder for MapOperationDecoder<K, V> {
    type Item = MapOperation<K, V>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let MapOperationDecoder {
            state,
            key_recognizer,
            value_recognizer,
        } = self;
        loop {
            match state {
                MapOperationDecoderState::ReadingHeader => {
                    if src.remaining() < TAG_SIZE {
                        src.reserve(TAG_SIZE);
                        break Ok(None);
                    }
                    match src.as_ref()[0] {
                        UPDATE => {
                            let required = TAG_SIZE + 2 * LEN_SIZE;
                            if src.remaining() < required {
                                src.reserve(required - src.remaining());
                                break Ok(None);
                            }
                            let mut header = &src.as_ref()[1..2 * LEN_SIZE + 1];
                            let key_len = header.get_u64() as usize;
                            let value_len = header.get_u64() as usize;
                            src.advance(TAG_SIZE + 2 * LEN_SIZE);
                            *state = MapOperationDecoderState::ReadingKey {
                                remaining: key_len,
                                value_size: Some(value_len),
                            };
                        }
                        REMOVE => {
                            let required = TAG_SIZE + LEN_SIZE;
                            if src.remaining() < required {
                                src.reserve(required - src.remaining());
                                break Ok(None);
                            }
                            let mut header = &src.as_ref()[1..LEN_SIZE + 1];
                            let key_len = header.get_u64() as usize;
                            src.advance(TAG_SIZE + LEN_SIZE);
                            *state = MapOperationDecoderState::ReadingKey {
                                remaining: key_len,
                                value_size: None,
                            };
                        }
                        CLEAR => {
                            src.advance(1);
                            break Ok(Some(MapOperation::Clear));
                        }
                        ow => {
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid map operation tag: {}", ow)),
                            }));
                        }
                    }
                }
                MapOperationDecoderState::ReadingKey {
                    remaining,
                    value_size,
                } => {
                    let (new_remaining, rem, decode_result) =
                        super::consume_bounded(remaining, src, key_recognizer);
                    match decode_result {
                        Ok(Some(result)) => {
                            src.unsplit(rem);
                            *state = MapOperationDecoderState::AfterKey {
                                key: Some(result),
                                remaining: *remaining,
                                value_size: *value_size,
                            }
                        }
                        Ok(None) => {
                            break Ok(None);
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                *state = MapOperationDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = MapOperationDecoderState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining + value_size.unwrap_or_default(),
                                }
                            }
                        }
                    }
                }
                MapOperationDecoderState::AfterKey {
                    key,
                    remaining,
                    value_size,
                } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        if let Some(value_size) = value_size.take() {
                            *state = MapOperationDecoderState::ReadingValue {
                                key: key.take(),
                                remaining: value_size,
                            };
                        } else {
                            let op = key.take().map(|key| MapOperation::Remove { key });
                            *state = MapOperationDecoderState::ReadingHeader;
                            break Ok(op);
                        }
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                MapOperationDecoderState::ReadingValue { key, remaining } => {
                    let (new_remaining, rem, decode_result) =
                        super::consume_bounded(remaining, src, value_recognizer);
                    match decode_result {
                        Ok(Some(value)) => {
                            src.unsplit(rem);
                            *state = MapOperationDecoderState::AfterValue {
                                key_value: key.take().map(move |k| (k, value)),
                                remaining: *remaining,
                            }
                        }
                        Ok(None) => {
                            break Ok(None);
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                *state = MapOperationDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = MapOperationDecoderState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                MapOperationDecoderState::AfterValue {
                    key_value,
                    remaining,
                } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = key_value
                            .take()
                            .map(|(key, value)| MapOperation::Update { key, value });
                        *state = MapOperationDecoderState::ReadingHeader;
                        break Ok(result);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                MapOperationDecoderState::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = MapOperationDecoderState::ReadingHeader;
                        break Err(err.into());
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
            }
        }
    }
}

impl<K: StructuralWritable, V: StructuralWritable> Encoder<MapOperation<K, V>>
    for MapOperationEncoder
{
    type Error = std::io::Error;

    fn encode(&mut self, item: MapOperation<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(TAG_SIZE);
        match item {
            MapOperation::Update { key, value } => {
                dst.put_u8(UPDATE);
                super::write_recon_kv(dst, &key, &value);
            }
            MapOperation::Remove { key } => {
                dst.put_u8(REMOVE);
                super::write_recon(dst, &key);
            }
            MapOperation::Clear => {
                dst.put_u8(CLEAR);
            }
        }
        Ok(())
    }
}

/// Reprsentation of map lane messages (used to form the body of Recond messages when operating)
/// on downlinks. This extends [`MapOperation`] with `Take` (retain the first `n` items) and `Drop`
/// (remove teh first `n` items). We never use these internally but must support them for communicating
/// with other implementations.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Form)]
pub enum MapMessage<K, V> {
    #[form(tag = "update")]
    Update {
        key: K,
        #[form(body)]
        value: V,
    },
    #[form(tag = "remove")]
    Remove {
        #[form(header)]
        key: K,
    },
    #[form(tag = "clear")]
    Clear,
    #[form(tag = "take")]
    Take(#[form(header_body)] u64),
    #[form(tag = "drop")]
    Drop(#[form(header_body)] u64),
}

impl<K, V> From<MapOperation<K, V>> for MapMessage<K, V> {
    fn from(op: MapOperation<K, V>) -> Self {
        match op {
            MapOperation::Update { key, value } => MapMessage::Update { key, value },
            MapOperation::Remove { key } => MapMessage::Remove { key },
            MapOperation::Clear => MapMessage::Clear,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MapMessageEncoder<Inner>(Inner);

impl<Inner> MapMessageEncoder<Inner> {
    pub fn new(inner: Inner) -> Self {
        MapMessageEncoder(inner)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MapMessageDecoder<Inner>(Inner);

impl<Inner> MapMessageDecoder<Inner> {
    pub fn new(inner: Inner) -> Self {
        MapMessageDecoder(inner)
    }
}

impl<K, V, Inner> Encoder<MapMessage<K, V>> for MapMessageEncoder<Inner>
where
    Inner: Encoder<MapOperation<K, V>>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: MapMessage<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let MapMessageEncoder(inner) = self;
        match item {
            MapMessage::Update { key, value } => {
                inner.encode(MapOperation::Update { key, value }, dst)
            }
            MapMessage::Remove { key } => inner.encode(MapOperation::Remove { key }, dst),
            MapMessage::Clear => inner.encode(MapOperation::Clear, dst),
            MapMessage::Take(n) => {
                dst.reserve(TAG_SIZE + LEN_SIZE);
                dst.put_u8(TAKE);
                dst.put_u64(n);
                Ok(())
            }
            MapMessage::Drop(n) => {
                dst.reserve(TAG_SIZE + LEN_SIZE);
                dst.put_u8(DROP);
                dst.put_u64(n);
                Ok(())
            }
        }
    }
}

impl<K, V, Inner> Decoder for MapMessageDecoder<Inner>
where
    Inner: Decoder<Item = MapOperation<K, V>>,
{
    type Item = MapMessage<K, V>;

    type Error = Inner::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let MapMessageDecoder(inner) = self;
        if src.remaining() < TAG_SIZE {
            src.reserve(TAG_SIZE);
            return Ok(None);
        }
        match src.as_ref()[0] {
            tag @ (TAKE | DROP) => {
                let required = TAG_SIZE + LEN_SIZE;
                if src.remaining() < required {
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                src.advance(TAG_SIZE);
                let n = src.get_u64();
                Ok(Some(if tag == TAKE {
                    MapMessage::Take(n)
                } else {
                    MapMessage::Drop(n)
                }))
            }
            _ => {
                let result = inner.decode(src)?;
                Ok(result.map(Into::into))
            }
        }
    }
}
