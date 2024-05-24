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

//! Protocol used by the runtime to communicate with stores.
//!
//! TODO Non-transient lanes also implicitly contain a store. They should
//! ultimately use the initialization component of this protocol. Currently,
//! they have initialization messages built into the lane protocol.
//!
//! There are two phases to the communication between the runtime and the agent.
//!
//! 1. Initialization
//! 2. Agent running.
//!
//! The initialization phase occurs when the agent is starting. After all items
//! complete this phase, the agent running phase starts.
//!
//! During the initialization phase:
//!
//! 1. The runtime sends one or more [`StoreInitMessage`] commands which transmit
//! the state of the item to the agent.
//! 2. The runtime sends a single [`StoreInitMessage`] `InitComplete` message.
//! 3. The store or lane responds with the [`StoreInitialized`] message.
//! 4. Both parties switch to the protocol for the Agent Running phase.
//!
//! During the agent running phase:
//!
//! 1. The runtime does not send messages to the agent and may drop the channel.
//! 2. The store or land sends [`StoreResponse`] messages each time its state
//! changes which are persisted by the runtime.

use bytes::{Buf, BufMut, BytesMut};
use swimos_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::{parser::RecognizerDecoder, WithLenReconEncoder};
use swimos_utilities::encoding::WithLengthBytesCodec;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    map::{
        MapMessageDecoder, MapOperationEncoder, RawMapMessageDecoder, RawMapMessageEncoder,
        RawMapOperationDecoder,
    },
    MapMessage, MapOperation, StoreInitMessage, StoreInitialized, StoreResponse,
};
use swimos_api::error::{FrameIoError, InvalidFrame};

use crate::{COMMAND, EVENT, INITIALIZED, INIT_DONE, TAG_LEN};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, Default)]
struct StoreInitMessageEncoder<Inner> {
    inner: Inner,
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

#[derive(Debug, Default)]
enum StoreInitMessageDecoderState {
    #[default]
    ReadingHeader,
    ReadingBody,
}

#[derive(Debug, Default)]
struct StoreInitMessageDecoder<D> {
    state: StoreInitMessageDecoderState,
    inner: D,
}

impl<D> StoreInitMessageDecoder<D> {
    fn new(decoder: D) -> Self {
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

#[derive(Debug, Clone, Copy, Default)]
enum StoreResponseDecoderState {
    #[default]
    Header,
    Message,
}

#[derive(Debug, Clone, Copy, Default)]
struct StoreResponseDecoder<Inner> {
    state: StoreResponseDecoderState,
    inner: Inner,
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

#[derive(Default, Debug)]
pub struct ValueStoreResponseEncoder {
    inner: StoreResponseEncoder<WithLenReconEncoder>,
}

impl<T: StructuralWritable> Encoder<StoreResponse<T>> for ValueStoreResponseEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: StoreResponse<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Default, Debug)]
pub struct RawValueStoreResponseDecoder {
    inner: StoreResponseDecoder<WithLengthBytesCodec>,
}

impl Decoder for RawValueStoreResponseDecoder {
    type Item = StoreResponse<BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Default, Debug)]
pub struct MapStoreResponseEncoder {
    inner: StoreResponseEncoder<MapOperationEncoder>,
}

impl<K: StructuralWritable, V: StructuralWritable> Encoder<StoreResponse<MapOperation<K, V>>>
    for MapStoreResponseEncoder
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: StoreResponse<MapOperation<K, V>>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Default, Debug)]
pub struct RawMapStoreResponseDecoder {
    inner: StoreResponseDecoder<RawMapOperationDecoder>,
}

impl Decoder for RawMapStoreResponseDecoder {
    type Item = StoreResponse<MapOperation<BytesMut, BytesMut>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Default, Debug)]
pub struct RawValueInitEncoder {
    inner: StoreInitMessageEncoder<WithLengthBytesCodec>,
}

impl<B: AsRef<[u8]>> Encoder<StoreInitMessage<B>> for RawValueInitEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: StoreInitMessage<B>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Default, Debug)]
pub struct RawMapInitEncoder {
    inner: StoreInitMessageEncoder<RawMapMessageEncoder>,
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Encoder<StoreInitMessage<MapMessage<K, V>>>
    for RawMapInitEncoder
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: StoreInitMessage<MapMessage<K, V>>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default)]
pub struct RawValueInitDecoder {
    inner: StoreInitMessageDecoder<WithLengthBytesCodec>,
}

impl Decoder for RawValueInitDecoder {
    type Item = StoreInitMessage<BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct ValueInitDecoder<T: RecognizerReadable> {
    inner: StoreInitMessageDecoder<RecognizerDecoder<T::Rec>>,
}

impl<T: RecognizerReadable> Default for ValueInitDecoder<T> {
    fn default() -> Self {
        Self {
            inner: StoreInitMessageDecoder::new(RecognizerDecoder::new(T::make_recognizer())),
        }
    }
}

impl<T: RecognizerReadable> Decoder for ValueInitDecoder<T> {
    type Item = StoreInitMessage<T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug, Default)]
pub struct RawMapInitDecoder {
    inner: StoreInitMessageDecoder<RawMapMessageDecoder>,
}

impl Decoder for RawMapInitDecoder {
    type Item = StoreInitMessage<MapMessage<BytesMut, BytesMut>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

pub struct MapInitDecoder<K: RecognizerReadable, V: RecognizerReadable> {
    inner: StoreInitMessageDecoder<MapMessageDecoder<K, V>>,
}

impl<K: RecognizerReadable, V: RecognizerReadable> Default for MapInitDecoder<K, V> {
    fn default() -> Self {
        Self {
            inner: StoreInitMessageDecoder::new(MapMessageDecoder::default()),
        }
    }
}

impl<K: RecognizerReadable, V: RecognizerReadable> Decoder for MapInitDecoder<K, V> {
    type Item = StoreInitMessage<MapMessage<K, V>>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}
