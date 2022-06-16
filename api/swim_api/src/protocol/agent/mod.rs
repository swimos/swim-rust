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

use std::fmt::Debug;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use swim_form::structural::write::StructuralWritable;
use swim_model::Text;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use crate::error::{FrameIoError, InvalidFrame};

use super::{
    map::{
        MapMessageEncoder, MapOperation, MapOperationEncoder, RawMapOperationDecoder,
        RawMapOperationEncoder,
    },
    WithLengthBytesCodec,
};

#[cfg(test)]
mod tests;

/// Message type for communication between the agent runtime and agent implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneRequest<T> {
    /// A command to alter the state of the lane.
    Command(T),
    /// Request the a synchronization with the lane (responses will be tagged with the provided ID).
    Sync(Uuid),
}

/// Possible message types for communication from an agent implementation to the agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneResponseKind {
    /// Notification of a change in the state of a lane.
    StandardEvent,
    /// Response to a sync requst from the runtime.
    SyncEvent(Uuid),
}

impl Default for LaneResponseKind {
    fn default() -> Self {
        LaneResponseKind::StandardEvent
    }
}

/// Value lane message type for communication between the agent implementation and agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueLaneResponse<T> {
    pub kind: LaneResponseKind, //Kind of the message (whether it is part of a synchronization).
    pub value: T,               //The body of the message.
}

impl<T> ValueLaneResponse<T> {
    pub fn event(value: T) -> Self {
        ValueLaneResponse {
            kind: LaneResponseKind::StandardEvent,
            value,
        }
    }

    pub fn synced(id: Uuid, value: T) -> Self {
        ValueLaneResponse {
            kind: LaneResponseKind::SyncEvent(id),
            value,
        }
    }
}

/// Map lane message type for communication between the agent implementation and agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapLaneResponse<K, V> {
    /// An event (either part of a syncrhonization or not).
    Event {
        kind: LaneResponseKind,
        operation: MapOperation<K, V>,
    },
    /// Indicates that a synchronization has completed.
    SyncComplete(Uuid),
}

const COMMAND: u8 = 0;
const SYNC: u8 = 1;
const SYNC_COMPLETE: u8 = 2;
const EVENT: u8 = 0;

const TAG_LEN: usize = 1;
const BODY_LEN: usize = std::mem::size_of::<u64>();
const ID_LEN: usize = std::mem::size_of::<u128>();

#[derive(Debug, Clone, Copy, Default)]
pub struct LaneRequestEncoder<Inner> {
    inner: Inner,
}

impl LaneRequestEncoder<WithLengthBytesCodec> {
    pub fn value() -> Self {
        LaneRequestEncoder {
            inner: WithLengthBytesCodec,
        }
    }
}

impl LaneRequestEncoder<MapMessageEncoder<RawMapOperationEncoder>> {
    pub fn map() -> Self {
        LaneRequestEncoder {
            inner: Default::default(),
        }
    }
}

impl<T, Inner> Encoder<LaneRequest<T>> for LaneRequestEncoder<Inner>
where
    Inner: Encoder<T>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: LaneRequest<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            LaneRequest::Command(cmd) => {
                let LaneRequestEncoder { inner, .. } = self;
                dst.reserve(TAG_LEN);
                dst.put_u8(COMMAND);
                inner.encode(cmd, dst)?;
            }
            LaneRequest::Sync(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC);
                dst.put_u128(id.as_u128());
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum LaneRequestDecoderState {
    ReadingHeader,
    ReadingBody,
}

impl Default for LaneRequestDecoderState {
    fn default() -> Self {
        LaneRequestDecoderState::ReadingHeader
    }
}
#[derive(Debug, Default)]
pub struct LaneRequestDecoder<D> {
    state: LaneRequestDecoderState,
    inner: D,
}

impl<D> LaneRequestDecoder<D> {
    pub fn new(decoder: D) -> Self {
        LaneRequestDecoder {
            state: Default::default(),
            inner: decoder,
        }
    }
}

impl<D> Decoder for LaneRequestDecoder<D>
where
    D: Decoder,
    D::Error: Into<FrameIoError>,
{
    type Item = LaneRequest<D::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let LaneRequestDecoder { state, inner } = self;
        loop {
            match state {
                LaneRequestDecoderState::ReadingHeader => {
                    if src.remaining() < TAG_LEN {
                        src.reserve(TAG_LEN);
                        break Ok(None);
                    }
                    match src.as_ref()[0] {
                        COMMAND => {
                            src.advance(TAG_LEN);
                            *state = LaneRequestDecoderState::ReadingBody;
                        }
                        SYNC => {
                            if src.remaining() < TAG_LEN + ID_LEN {
                                src.reserve(TAG_LEN + ID_LEN);
                                break Ok(None);
                            }
                            src.advance(TAG_LEN);
                            let id = Uuid::from_u128(src.get_u128());
                            break Ok(Some(LaneRequest::Sync(id)));
                        }
                        t => {
                            src.advance(TAG_LEN);
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid agent request tag: {}", t)),
                            }));
                        }
                    }
                }
                LaneRequestDecoderState::ReadingBody => {
                    break match inner.decode(src) {
                        Ok(Some(value)) => {
                            *state = LaneRequestDecoderState::ReadingHeader;
                            Ok(Some(LaneRequest::Command(value)))
                        }
                        Ok(None) => Ok(None),
                        Err(e) => {
                            *state = LaneRequestDecoderState::ReadingHeader;
                            Err(e.into())
                        }
                    };
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
                dst.put_u128(id.as_u128());
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
            src.reserve(TAG_LEN);
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
                let id = Uuid::from_u128(input.get_u128());
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
                dst.put_u128(id.as_u128());
                self.inner.encode(operation, dst)?;
            }
            MapLaneResponse::SyncComplete(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC_COMPLETE);
                dst.put_u128(id.as_u128());
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
                        break Ok(None);
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
                            let id = Uuid::from_u128(input.get_u128());
                            src.advance(TAG_LEN + ID_LEN);
                            *state = MapLaneResponseDecoderState::ReadingBody(
                                LaneResponseKind::SyncEvent(id),
                            );
                        }
                        SYNC_COMPLETE => {
                            if input.remaining() < ID_LEN {
                                break Ok(None);
                            }
                            let id = Uuid::from_u128(input.get_u128());
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
                    let inner_result = inner.decode(src);
                    break match inner_result {
                        Ok(Some(operation)) => {
                            let result = Ok(Some(MapLaneResponse::Event {
                                kind: std::mem::take(kind),
                                operation,
                            }));
                            *state = MapLaneResponseDecoderState::ReadingHeader;
                            result
                        }
                        Ok(None) => Ok(None),
                        Err(err) => {
                            *state = MapLaneResponseDecoderState::ReadingHeader;
                            Err(err)
                        }
                    };
                }
            }
        }
    }
}
