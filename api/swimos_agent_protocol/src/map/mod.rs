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

use bytes::{Buf, BufMut, BytesMut};
use swimos_api::error::{FrameIoError, InvalidFrame};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::{
    parser::{AsyncParseError, RecognizerDecoder},
    write_recon,
};
use swimos_utilities::encoding::consume_bounded;
use tokio_util::codec::{Decoder, Encoder};

mod parser;
#[cfg(test)]
mod tests;

pub use parser::{extract_header, extract_header_str};

use crate::{MapMessage, MapOperation};

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

const LEN_SIZE: usize = std::mem::size_of::<u64>();
const TAG_SIZE: usize = std::mem::size_of::<u8>();

const OVERSIZE_KEY: &str = "Key too large.";
const OVERSIZE_RECORD: &str = "Record too large.";
const BAD_TAG: &str = "Invalid map operation tag: ";
const BAD_RECORD_SIZE: &str = "Invalid record size: ";
const BAD_KEY_SIZE: &str = "Invalid key size: ";

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<MapOperation<K, V>> for RawMapOperationEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: MapOperation<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            MapOperation::Update { key, value } => {
                let key_bytes = key.as_ref();
                let value_bytes = value.as_ref();
                let total_len = key_bytes.len() + value_bytes.len() + LEN_SIZE + TAG_SIZE;
                dst.reserve(total_len + LEN_SIZE);
                dst.put_u64(u64::try_from(total_len).expect(OVERSIZE_RECORD));
                dst.put_u8(UPDATE);
                let key_len = u64::try_from(key_bytes.len()).expect(OVERSIZE_KEY);
                dst.put_u64(key_len);
                dst.put(key_bytes);
                dst.put(value_bytes);
            }
            MapOperation::Remove { key } => {
                let key_bytes = key.as_ref();
                let total_len = key_bytes.len() + TAG_SIZE;
                dst.reserve(total_len + LEN_SIZE);
                dst.put_u64(u64::try_from(total_len).expect(OVERSIZE_RECORD));
                dst.put_u8(REMOVE);
                dst.put(key_bytes);
            }
            MapOperation::Clear => {
                dst.reserve(LEN_SIZE + TAG_SIZE);
                dst.put_u64(TAG_SIZE as u64);
                dst.put_u8(CLEAR);
            }
        }
        Ok(())
    }
}

impl Decoder for RawMapOperationDecoder {
    type Item = MapOperation<BytesMut, BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < LEN_SIZE + TAG_SIZE {
            src.reserve(LEN_SIZE + TAG_SIZE);
            return Ok(None);
        }
        let mut header = src.as_ref();
        let total_len = header.get_u64() as usize;
        let tag = header.get_u8();
        match tag {
            UPDATE => {
                if total_len < LEN_SIZE + TAG_SIZE {
                    return Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::from(format!("{}{}", BAD_RECORD_SIZE, total_len)),
                    }));
                }
                let required = LEN_SIZE + total_len;
                if src.remaining() < required {
                    return Ok(None);
                }
                src.advance(LEN_SIZE);
                let mut frame = src.split_to(total_len);
                frame.advance(TAG_SIZE);
                let key_len = frame.get_u64() as usize;

                if key_len + LEN_SIZE + TAG_SIZE > total_len {
                    return Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::from(format!("{}{}", BAD_KEY_SIZE, key_len)),
                    }));
                }

                let key = frame.split_to(key_len);

                Ok(Some(MapOperation::Update { key, value: frame }))
            }
            REMOVE => {
                if total_len < TAG_SIZE {
                    return Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::from(format!("{}{}", BAD_RECORD_SIZE, total_len)),
                    }));
                }
                let required = LEN_SIZE + total_len;
                if src.remaining() < required {
                    return Ok(None);
                }
                src.advance(LEN_SIZE);
                let mut frame = src.split_to(total_len);
                frame.advance(TAG_SIZE);

                Ok(Some(MapOperation::Remove { key: frame }))
            }
            CLEAR => {
                if total_len == TAG_SIZE {
                    src.advance(LEN_SIZE + TAG_SIZE);
                    Ok(Some(MapOperation::Clear))
                } else {
                    Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::from(format!("{}{}", BAD_RECORD_SIZE, total_len)),
                    }))
                }
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
                    if src.remaining() < LEN_SIZE + TAG_SIZE {
                        src.reserve(LEN_SIZE + TAG_SIZE);
                        break Ok(None);
                    }
                    let mut header = src.as_ref();
                    let total_len = header.get_u64() as usize;
                    let tag = header.get_u8();
                    match tag {
                        UPDATE => {
                            let required = TAG_SIZE + 2 * LEN_SIZE;
                            if src.remaining() < required {
                                src.reserve(required - src.remaining());
                                break Ok(None);
                            }
                            let key_len = header.get_u64() as usize;
                            let value_len = if let Some(l) =
                                total_len.checked_sub(key_len + LEN_SIZE + TAG_SIZE)
                            {
                                l
                            } else {
                                break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                    problem: Text::from(format!("{}{}", BAD_KEY_SIZE, key_len)),
                                }));
                            };

                            src.advance(TAG_SIZE + 2 * LEN_SIZE);
                            *state = MapOperationDecoderState::ReadingKey {
                                remaining: key_len,
                                value_size: Some(value_len),
                            };
                        }
                        REMOVE => {
                            let key_len = if let Some(l) = total_len.checked_sub(TAG_SIZE) {
                                l
                            } else {
                                break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                    problem: Text::from(format!(
                                        "{}{}",
                                        BAD_RECORD_SIZE, total_len
                                    )),
                                }));
                            };
                            src.advance(LEN_SIZE + TAG_SIZE);
                            *state = MapOperationDecoderState::ReadingKey {
                                remaining: key_len,
                                value_size: None,
                            };
                        }
                        CLEAR => {
                            src.advance(TAG_SIZE + LEN_SIZE);
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
                    let (consumed, decode_result) =
                        consume_bounded(*remaining, src, key_recognizer);
                    *remaining -= consumed;
                    match decode_result {
                        Ok(Some(result)) => {
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
                            let rem = src.remaining();
                            if rem >= *remaining {
                                src.advance(*remaining);
                                *state = MapOperationDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                src.clear();
                                *state = MapOperationDecoderState::Discarding {
                                    remaining: *remaining - rem,
                                    error: Some(e),
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
                    let (consumed, decode_result) =
                        consume_bounded(*remaining, src, value_recognizer);
                    *remaining -= consumed;
                    match decode_result {
                        Ok(Some(value)) => {
                            *state = MapOperationDecoderState::AfterValue {
                                key_value: key.take().map(move |k| (k, value)),
                                remaining: *remaining,
                            }
                        }
                        Ok(None) => {
                            break Ok(None);
                        }
                        Err(e) => {
                            let rem = src.remaining();
                            if rem >= *remaining {
                                src.advance(*remaining);
                                *state = MapOperationDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                src.clear();
                                *state = MapOperationDecoderState::Discarding {
                                    remaining: *remaining - rem,
                                    error: Some(e),
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
                dst.reserve(2 * LEN_SIZE + TAG_SIZE);
                let body_len_offset = dst.remaining();
                dst.put_u64(0);
                dst.put_u8(0);
                dst.put_u64(0);
                let key_len = write_recon(dst, &key);
                let value_len = write_recon(dst, &value);
                let total_len = key_len + value_len + LEN_SIZE + TAG_SIZE;
                let mut rewound = &mut dst.as_mut()[body_len_offset..];
                rewound.put_u64(u64::try_from(total_len).expect(OVERSIZE_KEY));
                rewound.put_u8(UPDATE);
                rewound.put_u64(u64::try_from(key_len).expect(OVERSIZE_RECORD));
            }
            MapOperation::Remove { key } => {
                dst.reserve(LEN_SIZE + TAG_SIZE);
                let body_len_offset = dst.remaining();
                dst.put_u64(0);
                dst.put_u8(REMOVE);
                let key_len = write_recon(dst, &key);
                let total_len = key_len + TAG_SIZE;
                let mut rewound = &mut dst.as_mut()[body_len_offset..];
                rewound.put_u64(u64::try_from(total_len).expect(OVERSIZE_KEY));
            }
            MapOperation::Clear => {
                dst.put_u64(TAG_SIZE as u64);
                dst.put_u8(CLEAR);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct MessageEncoder<Inner>(Inner);

#[derive(Debug, Default, Clone, Copy)]
struct MessageDecoder<Inner>(Inner);

impl<K, V, Inner> Encoder<MapMessage<K, V>> for MessageEncoder<Inner>
where
    Inner: Encoder<MapOperation<K, V>>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: MapMessage<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let MessageEncoder(inner) = self;
        match item {
            MapMessage::Update { key, value } => {
                inner.encode(MapOperation::Update { key, value }, dst)
            }
            MapMessage::Remove { key } => inner.encode(MapOperation::Remove { key }, dst),
            MapMessage::Clear => inner.encode(MapOperation::Clear, dst),
            MapMessage::Take(n) => {
                dst.reserve(TAG_SIZE + 2 * LEN_SIZE);
                dst.put_u64((TAG_SIZE + LEN_SIZE) as u64);
                dst.put_u8(TAKE);
                dst.put_u64(n);
                Ok(())
            }
            MapMessage::Drop(n) => {
                dst.reserve(TAG_SIZE + 2 * LEN_SIZE);
                dst.put_u64((TAG_SIZE + LEN_SIZE) as u64);
                dst.put_u8(DROP);
                dst.put_u64(n);
                Ok(())
            }
        }
    }
}

impl<K, V, Inner> Decoder for MessageDecoder<Inner>
where
    Inner: Decoder<Item = MapOperation<K, V>, Error = FrameIoError>,
{
    type Item = MapMessage<K, V>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let MessageDecoder(inner) = self;
        if src.remaining() < TAG_SIZE + LEN_SIZE {
            src.reserve(TAG_SIZE + LEN_SIZE);
            return Ok(None);
        }
        let mut header = src.as_ref();
        let total_len = header.get_u64() as usize;
        match header.get_u8() {
            tag @ (TAKE | DROP) => {
                if total_len != TAG_SIZE + LEN_SIZE {
                    return Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                        problem: Text::new(BAD_RECORD_SIZE),
                    }));
                }
                let required = TAG_SIZE + 2 * LEN_SIZE;
                if src.remaining() < required {
                    src.reserve(required - src.remaining());
                    return Ok(None);
                }
                src.advance(TAG_SIZE + LEN_SIZE);
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

#[derive(Debug, Default, Clone, Copy)]
pub struct RawMapMessageEncoder {
    inner: MessageEncoder<RawMapOperationEncoder>,
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<MapMessage<K, V>> for RawMapMessageEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: MapMessage<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawMapMessageDecoder {
    inner: MessageDecoder<RawMapOperationDecoder>,
}

impl Decoder for RawMapMessageDecoder {
    type Item = MapMessage<BytesMut, BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MapMessageEncoder {
    inner: MessageEncoder<MapOperationEncoder>,
}

impl<K: StructuralWritable, V: StructuralWritable> Encoder<MapMessage<K, V>> for MapMessageEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: MapMessage<K, V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

pub struct MapMessageDecoder<K: RecognizerReadable, V: RecognizerReadable> {
    inner: MessageDecoder<MapOperationDecoder<K, V>>,
}

impl<K: RecognizerReadable, V: RecognizerReadable> Default for MapMessageDecoder<K, V> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K: RecognizerReadable, V: RecognizerReadable> Decoder for MapMessageDecoder<K, V> {
    type Item = MapMessage<K, V>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}
