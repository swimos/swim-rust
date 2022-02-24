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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, RecognizerDecoder};
use tokio_util::codec::{Decoder, Encoder};

#[cfg(test)]
mod tests;

use crate::error::{FrameIoError, InvalidFrame};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MapOperation<K, V> {
    Update { key: K, value: V },
    Remove { key: K },
    Clear,
}

pub type RawMapOperation = MapOperation<Bytes, Bytes>;

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

use super::{LEN_SIZE, TAG_SIZE};

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<MapOperation<K, V>> for RawMapOperationEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: MapOperation<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            MapOperation::Update { key, value } => {
                let key_bytes = key.as_ref();
                let value_bytes = value.as_ref();
                dst.reserve(TAG_SIZE + 2 * LEN_SIZE + key_bytes.len() + value_bytes.len());
                dst.put_u8(UPDATE);
                let key_len = u64::try_from(key_bytes.len()).expect("Key too large.");
                let value_len = u64::try_from(value_bytes.len()).expect("Value too large.");
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
    type Item = RawMapOperation;

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
                let key = src.split_to(key_len).freeze();
                let value = src.split_to(value_len).freeze();
                Ok(Some(RawMapOperation::Update { key, value }))
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
                let key = src.split_to(key_len).freeze();
                Ok(Some(RawMapOperation::Remove { key }))
            }
            CLEAR => {
                src.advance(TAG_SIZE);
                Ok(Some(RawMapOperation::Clear))
            }
            ow => Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                problem: Text::from(format!("Invalid map operation tag: {}", ow)),
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
                            key_recognizer.reset();
                            value_recognizer.reset();
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
                            key_recognizer.reset();
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
