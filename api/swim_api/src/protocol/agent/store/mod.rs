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

use bytes::{Buf, BufMut, BytesMut};
use swim_model::Text;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::{FrameIoError, InvalidFrame},
    protocol::{
        map::{MapOperation, MapOperationEncoder, RawMapOperationDecoder},
        WithLenReconEncoder, WithLengthBytesCodec,
    },
};

use super::{LaneResponse, COMMAND, EVENT, INITIALIZED, INIT_DONE, TAG_LEN};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreInitMessage<T> {
    /// A command to alter the state of the lane.
    Command(T),
    /// Indicates that the lane initialization phase is complete.
    InitComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreInitialized;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreResponse<T> {
    pub message: T,
}

impl<T> StoreResponse<T> {
    pub fn new(message: T) -> Self {
        StoreResponse { message }
    }
}

pub type MapStoreResponse<K, V> = StoreResponse<MapOperation<K, V>>;

impl<T> From<StoreResponse<T>> for LaneResponse<T> {
    fn from(response: StoreResponse<T>) -> Self {
        let StoreResponse { message } = response;
        LaneResponse::StandardEvent(message)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreInitMessageEncoder<Inner> {
    inner: Inner,
}

impl<Inner> StoreInitMessageEncoder<Inner> {
    pub fn new(inner: Inner) -> Self {
        StoreInitMessageEncoder { inner }
    }
}

impl StoreInitMessageEncoder<WithLengthBytesCodec> {
    pub fn value() -> Self {
        StoreInitMessageEncoder {
            inner: WithLengthBytesCodec,
        }
    }
}

impl<T, Inner> Encoder<StoreInitMessage<T>> for StoreInitMessageEncoder<Inner>
where
    Inner: Encoder<T>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: StoreInitMessage<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            StoreInitMessage::Command(cmd) => {
                let StoreInitMessageEncoder { inner } = self;
                dst.reserve(TAG_LEN);
                dst.put_u8(COMMAND);
                inner.encode(cmd, dst)?;
            }
            StoreInitMessage::InitComplete => {
                dst.reserve(TAG_LEN);
                dst.put_u8(INIT_DONE);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum StoreInitMessageDecoderState {
    ReadingHeader,
    ReadingBody,
}

impl Default for StoreInitMessageDecoderState {
    fn default() -> Self {
        StoreInitMessageDecoderState::ReadingHeader
    }
}
#[derive(Debug, Default)]
pub struct StoreInitMessageDecoder<D> {
    state: StoreInitMessageDecoderState,
    inner: D,
}

impl<D> StoreInitMessageDecoder<D> {
    pub fn new(decoder: D) -> Self {
        StoreInitMessageDecoder {
            state: Default::default(),
            inner: decoder,
        }
    }
}

impl<D> Decoder for StoreInitMessageDecoder<D>
where
    D: Decoder,
    D::Error: Into<FrameIoError>,
{
    type Item = StoreInitMessage<D::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let StoreInitMessageDecoder { state, inner } = self;
        loop {
            match state {
                StoreInitMessageDecoderState::ReadingHeader => {
                    if src.remaining() < TAG_LEN {
                        src.reserve(TAG_LEN);
                        break Ok(None);
                    }
                    match src.as_ref()[0] {
                        COMMAND => {
                            src.advance(TAG_LEN);
                            *state = StoreInitMessageDecoderState::ReadingBody;
                        }
                        INIT_DONE => {
                            src.advance(TAG_LEN);
                            break Ok(Some(StoreInitMessage::InitComplete));
                        }
                        t => {
                            src.advance(TAG_LEN);
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!(
                                    "Invalid store initialization tag: {}",
                                    t
                                )),
                            }));
                        }
                    }
                }
                StoreInitMessageDecoderState::ReadingBody => {
                    break match inner.decode(src) {
                        Ok(Some(value)) => {
                            *state = StoreInitMessageDecoderState::ReadingHeader;
                            Ok(Some(StoreInitMessage::Command(value)))
                        }
                        Ok(None) => Ok(None),
                        Err(e) => {
                            *state = StoreInitMessageDecoderState::ReadingHeader;
                            Err(e.into())
                        }
                    };
                }
            }
        }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct StoreInitializedCodec;

impl Encoder<StoreInitialized> for StoreInitializedCodec {
    type Error = std::io::Error;

    fn encode(&mut self, _item: StoreInitialized, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_u8(INITIALIZED);
        Ok(())
    }
}

impl Decoder for StoreInitializedCodec {
    type Item = StoreInitialized;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < TAG_LEN {
            src.reserve(TAG_LEN);
            Ok(None)
        } else {
            let tag = src.get_u8();
            if tag == INITIALIZED {
                Ok(Some(StoreInitialized))
            } else {
                Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                    problem: Text::from(format!("Invalid store initialized tag: {}", tag)),
                }))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreResponseEncoder<Inner> {
    inner: Inner,
}

impl<Inner> StoreResponseEncoder<Inner> {
    pub fn new(inner: Inner) -> Self {
        StoreResponseEncoder { inner }
    }
}

impl<T, Inner> Encoder<StoreResponse<T>> for StoreResponseEncoder<Inner>
where
    Inner: Encoder<T>,
{
    type Error = Inner::Error;

    fn encode(&mut self, item: StoreResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let StoreResponseEncoder { inner } = self;
        let StoreResponse { message } = item;
        dst.reserve(TAG_LEN);
        dst.put_u8(EVENT);
        inner.encode(message, dst)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum StoreResponseDecoderState {
    Header,
    Message,
}

impl Default for StoreResponseDecoderState {
    fn default() -> Self {
        StoreResponseDecoderState::Header
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreResponseDecoder<Inner> {
    state: StoreResponseDecoderState,
    inner: Inner,
}

impl<Inner> StoreResponseDecoder<Inner> {
    pub fn new(inner: Inner) -> Self {
        StoreResponseDecoder {
            state: Default::default(),
            inner,
        }
    }
}

impl<Inner> Decoder for StoreResponseDecoder<Inner>
where
    Inner: Decoder,
    FrameIoError: From<Inner::Error>,
{
    type Item = StoreResponse<Inner::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let StoreResponseDecoder { state, inner } = self;
        loop {
            match *state {
                StoreResponseDecoderState::Header => {
                    if src.remaining() <= TAG_LEN {
                        break Ok(None);
                    } else {
                        let tag = src.get_u8();
                        if tag == EVENT {
                            *state = StoreResponseDecoderState::Message;
                        } else {
                            break Err(FrameIoError::BadFrame(InvalidFrame::InvalidHeader {
                                problem: Text::from(format!("Invalid store response tag: {}", tag)),
                            }));
                        }
                    }
                }
                StoreResponseDecoderState::Message => {
                    let result = inner.decode(src);
                    if !matches!(result, Ok(None)) {
                        *state = StoreResponseDecoderState::Header;
                    }
                    return Ok(result?.map(|message| StoreResponse { message }));
                }
            }
        }
    }
}

pub type ValueStoreResponseEncoder = StoreResponseEncoder<WithLenReconEncoder>;
pub type ValueStoreResponseDecoder = StoreResponseDecoder<WithLengthBytesCodec>;

pub type MapStoreResponseEncoder = StoreResponseEncoder<MapOperationEncoder>;
pub type MapStoreResponseDecoder = StoreResponseDecoder<RawMapOperationDecoder>;
