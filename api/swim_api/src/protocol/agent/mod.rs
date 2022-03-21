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
use swim_form::structural::write::StructuralWritable;
use swim_model::Text;
use swim_recon::parser::AsyncParseError;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{FrameIoError, InvalidFrame};

use super::map::{MapOperation, MapOperationEncoder, RawMapOperationDecoder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneRequest<T> {
    Command(T),
    Sync(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneResponseKind {
    StandardEvent,
    SyncEvent(u64),
}

impl Default for LaneResponseKind {
    fn default() -> Self {
        LaneResponseKind::StandardEvent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueLaneResponse<T> {
    kind: LaneResponseKind,
    value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapLaneResponse<K, V> {
    Event {
        kind: LaneResponseKind,
        operation: MapOperation<K, V>,
    },
    SyncComplete(u64),
}

const COMMAND: u8 = 0;
const SYNC: u8 = 1;
const SYNC_COMPLETE: u8 = 2;
const EVENT: u8 = 0;

const TAG_LEN: usize = 0;
const BODY_LEN: usize = std::mem::size_of::<u64>();
const ID_LEN: usize = std::mem::size_of::<u64>();

#[derive(Debug, Clone, Copy, Default)]
pub struct LaneRequestEncoder;

impl<B: AsRef<[u8]>> Encoder<LaneRequest<B>> for LaneRequestEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneRequest<B>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            LaneRequest::Command(cmd) => {
                let bytes = cmd.as_ref();
                dst.reserve(TAG_LEN + BODY_LEN + bytes.len());
                dst.put_u8(COMMAND);
                dst.put_u64(bytes.len() as u64);
                dst.put(bytes);
            }
            LaneRequest::Sync(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC);
                dst.put_u64(id);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum LaneRequestDecoderState<T> {
    ReadingHeader,
    ReadingBody {
        remaining: usize,
    },
    AfterBody {
        message: Option<T>,
        remaining: usize,
    },
    Discarding {
        error: Option<FrameIoError>,
        remaining: usize,
    },
}

impl<T> Default for LaneRequestDecoderState<T> {
    fn default() -> Self {
        LaneRequestDecoderState::ReadingHeader
    }
}
#[derive(Debug)]
pub struct LaneRequestDecoder<T, D> {
    state: LaneRequestDecoderState<T>,
    inner: D,
}

impl<T, D> Decoder for LaneRequestDecoder<T, D>
where
    D: Decoder<Item = T>,
    D::Error: Into<FrameIoError>,
{
    type Item = LaneRequest<T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let LaneRequestDecoder { state, inner } = self;
        loop {
            match state {
                LaneRequestDecoderState::ReadingHeader => {
                    if src.remaining() < TAG_LEN + ID_LEN {
                        src.reserve(TAG_LEN + ID_LEN);
                        break Ok(None);
                    }
                    match src.get_u8() {
                        COMMAND => {
                            let len = src.get_u64();
                            *state = LaneRequestDecoderState::ReadingBody {
                                remaining: len as usize,
                            };
                        }
                        SYNC => {
                            let id = src.get_u64();
                            break Ok(Some(LaneRequest::Sync(id)));
                        }
                        t => {
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid agent request tag: {}", t)),
                            }))
                        }
                    }
                }
                LaneRequestDecoderState::ReadingBody { remaining } => {
                    let (new_remaining, rem, decode_result) =
                        super::consume_bounded(remaining, src, inner);
                    match decode_result {
                        Ok(Some(result)) => {
                            src.unsplit(rem);
                            *state = LaneRequestDecoderState::AfterBody {
                                message: Some(result),
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
                                *state = LaneRequestDecoderState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = LaneRequestDecoderState::Discarding {
                                    error: Some(e.into()),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                LaneRequestDecoderState::AfterBody { message, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = message.take();
                        *state = LaneRequestDecoderState::ReadingHeader;
                        break Ok(result.map(LaneRequest::Command));
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                LaneRequestDecoderState::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error
                            .take()
                            .unwrap_or_else(|| AsyncParseError::UnconsumedInput.into());
                        *state = LaneRequestDecoderState::ReadingHeader;
                        break Err(err);
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

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueLaneResponseEncoder;

impl<T> Encoder<ValueLaneResponse<T>> for ValueLaneResponseEncoder
where
    T: StructuralWritable,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: ValueLaneResponse<T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let ValueLaneResponse { kind, value } = item;
        match kind {
            LaneResponseKind::StandardEvent => {
                dst.reserve(TAG_LEN);
                dst.put_u8(EVENT);
            }
            LaneResponseKind::SyncEvent(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC_COMPLETE);
                dst.put_u64(id);
            }
        }
        super::write_recon(dst, &value);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueLaneResponseDecoder;

impl Decoder for ValueLaneResponseDecoder {
    type Item = ValueLaneResponse<Bytes>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < TAG_LEN {
            src.reserve(TAG_LEN + ID_LEN);
            return Ok(None);
        }
        let mut input = src.as_ref();
        match input.get_u8() {
            EVENT => {
                if input.remaining() < BODY_LEN {
                    src.reserve(TAG_LEN + BODY_LEN);
                    return Ok(None);
                }
                let len = input.get_u64() as usize;
                if input.remaining() < len {
                    src.reserve(TAG_LEN + BODY_LEN + len);
                    return Ok(None);
                }
                src.advance(TAG_LEN + BODY_LEN);
                let body = src.split_to(len).freeze();
                Ok(Some(ValueLaneResponse {
                    kind: LaneResponseKind::StandardEvent,
                    value: body,
                }))
            }
            SYNC_COMPLETE => {
                if input.remaining() < ID_LEN + BODY_LEN {
                    src.reserve(TAG_LEN + ID_LEN + BODY_LEN);
                    return Ok(None);
                }
                let id = input.get_u64();
                let len = input.get_u64() as usize;
                if input.remaining() < len {
                    src.reserve(TAG_LEN + ID_LEN + BODY_LEN + len);
                    return Ok(None);
                }
                src.advance(TAG_LEN + ID_LEN + BODY_LEN);
                let body = src.split_to(len).freeze();
                Ok(Some(ValueLaneResponse {
                    kind: LaneResponseKind::SyncEvent(id),
                    value: body,
                }))
            }
            t => Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                problem: Text::from(format!("Invalid agent response tag: {}", t)),
            })),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MapLaneResponseEncoder {
    inner: MapOperationEncoder,
}

impl<K, V> Encoder<MapLaneResponse<K, V>> for MapLaneResponseEncoder
where
    K: StructuralWritable,
    V: StructuralWritable,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: MapLaneResponse<K, V>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            MapLaneResponse::Event {
                kind: LaneResponseKind::StandardEvent,
                operation,
            } => {
                dst.reserve(TAG_LEN);
                dst.put_u8(EVENT);
                self.inner.encode(operation, dst)?;
            }
            MapLaneResponse::Event {
                kind: LaneResponseKind::SyncEvent(id),
                operation,
            } => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC);
                dst.put_u64(id);
                self.inner.encode(operation, dst)?;
            }
            MapLaneResponse::SyncComplete(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC_COMPLETE);
                dst.put_u64(id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum MapLaneResponseDecoderState {
    ReadingHeader,
    ReadingBody(LaneResponseKind),
}

impl Default for MapLaneResponseDecoderState {
    fn default() -> Self {
        MapLaneResponseDecoderState::ReadingHeader
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MapLaneResponseDecoder {
    inner: RawMapOperationDecoder,
    state: MapLaneResponseDecoderState,
}

impl Decoder for MapLaneResponseDecoder {
    type Item = MapLaneResponse<Bytes, Bytes>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let MapLaneResponseDecoder { inner, state } = self;
        loop {
            match state {
                MapLaneResponseDecoderState::ReadingHeader => {
                    let mut input = src.as_ref();
                    if input.remaining() < TAG_LEN {
                        src.reserve(TAG_LEN);
                        return Ok(None);
                    }
                    match input.get_u8() {
                        EVENT => {
                            src.advance(TAG_LEN);
                            *state = MapLaneResponseDecoderState::ReadingBody(
                                LaneResponseKind::StandardEvent,
                            );
                        }
                        SYNC => {
                            if input.remaining() < ID_LEN {
                                break Ok(None);
                            }
                            let id = input.get_u64();
                            src.advance(TAG_LEN + ID_LEN);
                            *state = MapLaneResponseDecoderState::ReadingBody(
                                LaneResponseKind::SyncEvent(id),
                            );
                        }
                        SYNC_COMPLETE => {
                            if input.remaining() < ID_LEN {
                                break Ok(None);
                            }
                            let id = input.get_u64();
                            src.advance(TAG_LEN + ID_LEN);
                            break Ok(Some(MapLaneResponse::SyncComplete(id)));
                        }
                        t => {
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid agent response tag: {}", t)),
                            }));
                        }
                    }
                }
                MapLaneResponseDecoderState::ReadingBody(kind) => {
                    break Ok(inner.decode(src)?.map(|operation| MapLaneResponse::Event {
                        kind: std::mem::take(kind),
                        operation,
                    }));
                }
            }
        }
    }
}
