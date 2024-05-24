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

use std::fmt::Debug;

use crate::{
    map::{RawMapMessageDecoder, RawMapMessageEncoder},
    LaneRequest, LaneResponse, MapLaneResponse, MapMessage, MapOperation, COMMAND, EVENT, ID_LEN,
    INITIALIZED, INIT_DONE, SYNC, SYNC_COMPLETE, TAG_LEN,
};
use bytes::{Buf, BufMut, BytesMut};
use swimos_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::{WithLenRecognizerDecoder, WithLenReconEncoder};
use swimos_utilities::encoding::WithLengthBytesCodec;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use swimos_api::error::{FrameIoError, InvalidFrame};

use crate::map::{MapMessageDecoder, MapOperationDecoder};

use super::map::{
    MapMessageEncoder, MapOperationEncoder, RawMapOperationDecoder, RawMapOperationEncoder,
};
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, Default)]
struct LaneRequestEncoder<Inner> {
    inner: Inner,
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

#[derive(Debug, Default)]
enum LaneRequestDecoderState {
    #[default]
    ReadingHeader,
    ReadingBody,
}

#[derive(Debug, Default)]
struct LaneRequestDecoder<D> {
    state: LaneRequestDecoderState,
    inner: D,
}

impl<D> LaneRequestDecoder<D> {
    fn new(decoder: D) -> Self {
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
                                problem: Text::from(format!("Invalid lane request tag: {}", t)),
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
struct LaneResponseEncoder<Inner> {
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

#[derive(Debug, Clone, Copy, Default)]
enum LaneResponseDecoderState {
    #[default]
    Header,
    Std,
    Sync(Uuid),
}

#[derive(Debug, Clone, Copy, Default)]
struct LaneResponseDecoder<Inner> {
    state: LaneResponseDecoderState,
    inner: Inner,
}

impl<Inner> LaneResponseDecoder<Inner> {
    fn new(inner: Inner) -> Self {
        LaneResponseDecoder {
            state: Default::default(),
            inner,
        }
    }
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
                                problem: Text::from(format!("Invalid lane response tag: {}", t)),
                            }));
                        }
                    }
                }
                LaneResponseDecoderState::Std => {
                    let result = inner.decode(src);
                    if !matches!(result, Ok(None)) {
                        *state = LaneResponseDecoderState::Header;
                    }
                    return Ok(result?.map(LaneResponse::StandardEvent));
                }
                LaneResponseDecoderState::Sync(id) => {
                    let result = inner.decode(src);
                    if !matches!(result, Ok(None)) {
                        *state = LaneResponseDecoderState::Header;
                    }
                    return Ok(result?.map(move |item| LaneResponse::SyncEvent(id, item)));
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValueLaneResponseEncoder {
    inner: LaneResponseEncoder<WithLenReconEncoder>,
}

impl<T: StructuralWritable> Encoder<LaneResponse<T>> for ValueLaneResponseEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawValueLaneResponseEncoder {
    inner: LaneResponseEncoder<WithLengthBytesCodec>,
}

impl<T: AsRef<[u8]>> Encoder<LaneResponse<T>> for RawValueLaneResponseEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MapLaneResponseEncoder {
    inner: LaneResponseEncoder<MapOperationEncoder>,
}

impl<K: StructuralWritable, V: StructuralWritable> Encoder<MapLaneResponse<K, V>>
    for MapLaneResponseEncoder
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: MapLaneResponse<K, V>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawMapLaneResponseEncoder {
    inner: LaneResponseEncoder<RawMapOperationEncoder>,
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<MapLaneResponse<K, V>> for RawMapLaneResponseEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: MapLaneResponse<K, V>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawValueLaneResponseDecoder {
    inner: LaneResponseDecoder<WithLengthBytesCodec>,
}

impl Decoder for RawValueLaneResponseDecoder {
    type Item = LaneResponse<BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct ValueLaneResponseDecoder<T: RecognizerReadable> {
    inner: LaneResponseDecoder<WithLenRecognizerDecoder<T::Rec>>,
}

impl<T: RecognizerReadable> Default for ValueLaneResponseDecoder<T> {
    fn default() -> Self {
        Self {
            inner: LaneResponseDecoder::new(WithLenRecognizerDecoder::new(T::make_recognizer())),
        }
    }
}

impl<T: RecognizerReadable> Decoder for ValueLaneResponseDecoder<T> {
    type Item = LaneResponse<T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawMapLaneResponseDecoder {
    inner: LaneResponseDecoder<RawMapOperationDecoder>,
}

impl Decoder for RawMapLaneResponseDecoder {
    type Item = LaneResponse<MapOperation<BytesMut, BytesMut>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct MapLaneResponseDecoder<K: RecognizerReadable, V: RecognizerReadable> {
    inner: LaneResponseDecoder<MapOperationDecoder<K, V>>,
}

impl<K: RecognizerReadable, V: RecognizerReadable> Default for MapLaneResponseDecoder<K, V> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<K: RecognizerReadable, V: RecognizerReadable> Decoder for MapLaneResponseDecoder<K, V> {
    type Item = LaneResponse<MapOperation<K, V>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawValueLaneRequestEncoder {
    inner: LaneRequestEncoder<WithLengthBytesCodec>,
}

impl<T: AsRef<[u8]>> Encoder<LaneRequest<T>> for RawValueLaneRequestEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneRequest<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawMapLaneRequestEncoder {
    inner: LaneRequestEncoder<RawMapMessageEncoder>,
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<LaneRequest<MapMessage<K, V>>>
    for RawMapLaneRequestEncoder
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: LaneRequest<MapMessage<K, V>>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ValueLaneRequestEncoder {
    inner: LaneRequestEncoder<WithLenReconEncoder>,
}

impl<T: StructuralWritable> Encoder<LaneRequest<T>> for ValueLaneRequestEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneRequest<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MapLaneRequestEncoder {
    inner: LaneRequestEncoder<MapMessageEncoder>,
}

impl<K: StructuralWritable, V: StructuralWritable> Encoder<LaneRequest<MapMessage<K, V>>>
    for MapLaneRequestEncoder
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: LaneRequest<MapMessage<K, V>>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default)]
pub struct RawValueLaneRequestDecoder {
    inner: LaneRequestDecoder<WithLengthBytesCodec>,
}

impl Decoder for RawValueLaneRequestDecoder {
    type Item = LaneRequest<BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct ValueLaneRequestDecoder<T: RecognizerReadable> {
    inner: LaneRequestDecoder<WithLenRecognizerDecoder<T::Rec>>,
}

impl<T: RecognizerReadable> Default for ValueLaneRequestDecoder<T> {
    fn default() -> Self {
        Self {
            inner: LaneRequestDecoder::new(WithLenRecognizerDecoder::new(T::make_recognizer())),
        }
    }
}

impl<T: RecognizerReadable> Decoder for ValueLaneRequestDecoder<T> {
    type Item = LaneRequest<T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug, Default)]
pub struct RawMapLaneRequestDecoder {
    inner: LaneRequestDecoder<RawMapMessageDecoder>,
}

impl Decoder for RawMapLaneRequestDecoder {
    type Item = LaneRequest<MapMessage<BytesMut, BytesMut>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct MapLaneRequestDecoder<K: RecognizerReadable, V: RecognizerReadable> {
    inner: LaneRequestDecoder<MapMessageDecoder<K, V>>,
}

impl<K: RecognizerReadable, V: RecognizerReadable> Default for MapLaneRequestDecoder<K, V> {
    fn default() -> Self {
        Self {
            inner: LaneRequestDecoder::new(MapMessageDecoder::default()),
        }
    }
}

impl<K: RecognizerReadable, V: RecognizerReadable> Decoder for MapLaneRequestDecoder<K, V> {
    type Item = LaneRequest<MapMessage<K, V>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}
