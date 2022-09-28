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

use bytes::{Buf, BufMut, BytesMut};
use swim_model::Text;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use crate::error::{FrameIoError, InvalidFrame};

use super::{
    map::{
        MapMessageEncoder, MapOperation, MapOperationEncoder, RawMapOperationDecoder,
        RawMapOperationEncoder,
    },
    WithLenReconEncoder, WithLengthBytesCodec,
};

#[cfg(test)]
mod tests;

/// Message type for communication between the agent runtime and agent implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneRequest<T> {
    /// A command to alter the state of the lane.
    Command(T),
    /// Indicates that the lane initialization phase is complete.
    InitComplete,
    /// Request a synchronization with the lane (responses will be tagged with the provided ID).
    Sync(Uuid),
}

/// Message type for communication from the agent implentation to the agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneResponse<T> {
    /// An event to be broadast to all uplinks.
    StandardEvent(T),
    /// Indicates that the lane has been initalized.
    Initialized,
    /// An event to be sent to a specific uplink.
    SyncEvent(Uuid, T),
    /// Signal that an uplink has a consistent view of a lane.
    Synced(Uuid),
}

impl<T> LaneResponse<T> {
    pub fn synced(id: Uuid) -> Self {
        LaneResponse::Synced(id)
    }

    pub fn event(body: T) -> Self {
        LaneResponse::StandardEvent(body)
    }

    pub fn sync_event(id: Uuid, body: T) -> Self {
        LaneResponse::SyncEvent(id, body)
    }
}

pub type MapLaneResponse<K, V> = LaneResponse<MapOperation<K, V>>;

const COMMAND: u8 = 0;
const SYNC: u8 = 1;
const SYNC_COMPLETE: u8 = 2;
const EVENT: u8 = 3;
const INIT_DONE: u8 = 4;
const INITIALIZED: u8 = 5;

const TAG_LEN: usize = 1;
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
            LaneRequest::InitComplete => {
                dst.reserve(TAG_LEN);
                dst.put_u8(INIT_DONE);
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
                        INIT_DONE => {
                            src.advance(TAG_LEN);
                            break Ok(Some(LaneRequest::InitComplete));
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
pub struct LaneResponseEncoder<Inner> {
    inner: Inner,
}

impl<T, Inner> Encoder<LaneResponse<T>> for LaneResponseEncoder<Inner>
where
    Inner: Encoder<T>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: LaneResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let LaneResponseEncoder { inner } = self;
        match item {
            LaneResponse::StandardEvent(body) => {
                dst.reserve(TAG_LEN);
                dst.put_u8(EVENT);
                inner.encode(body, dst)?;
            }
            LaneResponse::Initialized => {
                dst.reserve(TAG_LEN);
                dst.put_u8(INITIALIZED);
            }
            LaneResponse::SyncEvent(id, body) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC);
                dst.put_u128(id.as_u128());
                inner.encode(body, dst)?;
            }
            LaneResponse::Synced(id) => {
                dst.reserve(TAG_LEN + ID_LEN);
                dst.put_u8(SYNC_COMPLETE);
                dst.put_u128(id.as_u128());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum LaneResponseDecoderState {
    Header,
    Std,
    Sync(Uuid),
}

impl Default for LaneResponseDecoderState {
    fn default() -> Self {
        LaneResponseDecoderState::Header
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LaneResponseDecoder<Inner> {
    state: LaneResponseDecoderState,
    inner: Inner,
}

impl<Inner> Decoder for LaneResponseDecoder<Inner>
where
    Inner: Decoder,
    FrameIoError: From<Inner::Error>,
{
    type Item = LaneResponse<Inner::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let LaneResponseDecoder { state, inner } = self;
        loop {
            match *state {
                LaneResponseDecoderState::Header => {
                    let mut bytes = src.as_ref();
                    if bytes.len() < TAG_LEN {
                        src.reserve(TAG_LEN);
                        return Ok(None);
                    }
                    let tag = bytes.get_u8();
                    match tag {
                        EVENT => {
                            src.advance(TAG_LEN);
                            *state = LaneResponseDecoderState::Std;
                        }
                        INITIALIZED => {
                            src.advance(TAG_LEN);
                            return Ok(Some(LaneResponse::Initialized));
                        }
                        SYNC => {
                            if bytes.len() < ID_LEN {
                                src.reserve(ID_LEN);
                                return Ok(None);
                            }
                            let id = Uuid::from_u128(bytes.get_u128());
                            src.advance(TAG_LEN + ID_LEN);
                            *state = LaneResponseDecoderState::Sync(id);
                        }
                        SYNC_COMPLETE => {
                            if bytes.len() < ID_LEN {
                                src.reserve(ID_LEN);
                                return Ok(None);
                            }
                            let id = Uuid::from_u128(bytes.get_u128());
                            src.advance(TAG_LEN + ID_LEN);
                            return Ok(Some(LaneResponse::Synced(id)));
                        }
                        t => {
                            return Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid agent response tag: {}", t)),
                            }));
                        }
                    }
                }
                LaneResponseDecoderState::Std => {
                    return Ok(inner.decode(src)?.map(LaneResponse::StandardEvent));
                }
                LaneResponseDecoderState::Sync(id) => {
                    return Ok(inner
                        .decode(src)?
                        .map(move |item| LaneResponse::SyncEvent(id, item)));
                }
            }
        }
    }
}

pub type ValueLaneResponseEncoder = LaneResponseEncoder<WithLenReconEncoder>;
pub type ValueLaneResponseDecoder = LaneResponseDecoder<WithLengthBytesCodec>;

pub type MapLaneResponseEncoder = LaneResponseEncoder<MapOperationEncoder>;
pub type MapLaneResponseDecoder = LaneResponseDecoder<RawMapOperationDecoder>;
