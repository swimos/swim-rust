// Copyright 2015-2024 Swim Inc.
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

use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::{WithLenRecognizerDecoder, WithLenReconEncoder};
use swimos_utilities::encoding::{TryFromUtf8Bytes, WithLengthBytesCodec};
use tokio_util::codec::{Decoder, Encoder};

use swimos_api::{address::Address, error::FrameIoError};

use crate::model::CommandMessage;

#[cfg(test)]
mod tests;

/// Tokio encoder for command messages.
#[derive(Debug, Default, Clone, Copy)]
struct CommandEncoder<E> {
    body_encoder: E,
}

#[derive(Debug, Default)]
enum DecoderState<S> {
    #[default]
    Init, // Initial state before any bytes consumed.
    ReadingRegistration(FrameFlags), // The flags indicate a new registration for an endpoint.
    ReadingRegisteredHeader(FrameFlags), // The flags indicate a message for a registered endpoint.
    ReadingAddressedHeader(FrameFlags), // The flags indicate a message for a specified endpoint.
    ReadingAddressedBody(Address<S>, bool), // Reading the body for message to a specified endpoint.
    ReadingRegisteredBody(u16, bool), // Reading the body for a message to a registered endpoint. 
}

/// Tokio decoder for command messages.
#[derive(Debug)]
struct CommandDecoder<S, D> {
    state: DecoderState<S>,
    body_decoder: D,
}

impl<S, D: Default> Default for CommandDecoder<S, D> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            body_decoder: Default::default(),
        }
    }
}

const FLAGS_LEN: usize = 1;
const LEN_LEN: usize = 8;
const ID_LEN: usize = 2;

bitflags! {
    #[derive(Debug, Copy, Clone)]
    struct FrameFlags: u8 {
        const REGISTRATION = 0b0001;
        const REGISTERED = 0b0010;
        const HAS_HOST = 0b0100;
        const OVERWRITE_PERMITTED = 0b1000;
    }
}

fn put_address<S>(dst: &mut bytes::BytesMut, address: &Address<S>)
where
    S: AsRef<str>,
{
    let Address { host, node, lane } = address;
    let node_str = node.as_ref();
    let lane_str = lane.as_ref();
    if let Some(h) = host {
        let host_str = h.as_ref();
        dst.put_u64(host_str.len() as u64);
        dst.put_u64(node_str.len() as u64);
        dst.put_u64(lane_str.len() as u64);
        dst.put(host_str.as_bytes());
    } else {
        dst.put_u64(node_str.len() as u64);
        dst.put_u64(lane_str.len() as u64);
    }
    dst.put(node_str.as_bytes());
    dst.put(lane_str.as_bytes());
}

impl<S, T, E> Encoder<CommandMessage<S, T>> for CommandEncoder<E>
where
    S: AsRef<str>,
    E: Encoder<T>,
{
    type Error = E::Error;

    fn encode(
        &mut self,
        item: CommandMessage<S, T>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let CommandEncoder { body_encoder } = self;
        let mut flags = FrameFlags::empty();
        let mut required = FLAGS_LEN;
        match &item {
            CommandMessage::Register { address: Address { host, node, lane }, .. } => {
                flags.insert(FrameFlags::REGISTRATION);
                required += 2 * LEN_LEN + ID_LEN + node.as_ref().len() + lane.as_ref().len();
                if let Some(h) = host {
                    flags.insert(FrameFlags::HAS_HOST);
                    required += LEN_LEN + h.as_ref().len();
                }
            },
            CommandMessage::Addressed { target: Address { host, node, lane }, overwrite_permitted, .. } => {
                if *overwrite_permitted {
                    flags.insert(FrameFlags::OVERWRITE_PERMITTED);
                }
                required += 2 * LEN_LEN + node.as_ref().len() + lane.as_ref().len();
                if let Some(h) = host {
                    flags.insert(FrameFlags::HAS_HOST);
                    required += LEN_LEN + h.as_ref().len();
                }
            },
            CommandMessage::Registered { overwrite_permitted, .. } => {
                flags.insert(FrameFlags::REGISTERED);
                if *overwrite_permitted {
                    flags.insert(FrameFlags::OVERWRITE_PERMITTED);
                }
                required += ID_LEN;
            },
        }
        dst.reserve(required);
        dst.put_u8(flags.bits());
        match item {
            CommandMessage::Register { address, id } => {
                put_address(dst, &address);
                dst.put_u16(id);
                Ok(())
            },
            CommandMessage::Addressed { target, command, .. } => {
                put_address(dst, &target);
                body_encoder.encode(command, dst)
            },
            CommandMessage::Registered { target, command, .. } => {
                dst.put_u16(target);
                body_encoder.encode(command, dst)
            },
        }
    }
}

const MIN_REQUIRED: usize = 2 * LEN_LEN;
const MAX_REQUIRED: usize = 3 * LEN_LEN;

impl<S, D> Decoder for CommandDecoder<S, D>
where
    S: TryFromUtf8Bytes + std::fmt::Debug,
    D: Decoder,
    FrameIoError: From<D::Error>,
{
    type Item = CommandMessage<S, D::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let CommandDecoder {
            state,
            body_decoder,
            ..
        } = self;
        loop {
            match std::mem::take(state) {
                DecoderState::Init => {
                    let remaining = src.remaining();
                    if remaining < FLAGS_LEN {
                        break Ok(None);
                    }
                    let flags = FrameFlags::from_bits_truncate(src.get_u8());
                    if flags.contains(FrameFlags::REGISTRATION) {
                        *state = DecoderState::ReadingRegistration(flags);
                    } else if flags.contains(FrameFlags::REGISTERED) {
                        *state = DecoderState::ReadingRegisteredHeader(flags);
                    } else {
                        *state = DecoderState::ReadingAddressedHeader(flags);
                    }
                }
                DecoderState::ReadingRegistration(flags) => {
                    let remaining = src.remaining();
                    let req = if flags.contains(FrameFlags::HAS_HOST) {
                        MAX_REQUIRED
                    } else {
                        MIN_REQUIRED
                    };
                    if remaining < req {
                        *state = DecoderState::ReadingAddressedHeader(flags);
                        break Ok(None);
                    }
                    let mut bytes = src.as_ref();
                    let has_host = flags.contains(FrameFlags::HAS_HOST);
                    
                    let host_len = if has_host {
                        bytes.get_u64() as usize
                    } else {
                        0
                    };

                    let node_len = bytes.get_u64() as usize;
                    let lane_len = bytes.get_u64() as usize;

                    if bytes.remaining() < host_len + node_len + lane_len + ID_LEN {
                        *state = DecoderState::ReadingAddressedHeader(flags);
                        break Ok(None);
                    }
                    let host = if has_host {
                        src.advance(MAX_REQUIRED);
                        Some(try_extract_utf8(src, host_len)?)
                    } else {
                        src.advance(MIN_REQUIRED);
                        None
                    };

                    let node = try_extract_utf8(src, node_len)?;
                    let lane = try_extract_utf8(src, lane_len)?;
                    let address = Address::new(host, node, lane);
                    let id = src.get_u16();
                    break Ok(Some(CommandMessage::Register { address, id }));
                }
                DecoderState::ReadingRegisteredHeader(flags) => {
                    let remaining = src.remaining();
                    if remaining < ID_LEN {
                        *state = DecoderState::ReadingRegisteredHeader(flags);
                        break Ok(None);
                    }
                    let id = src.get_u16();
                    *state = DecoderState::ReadingRegisteredBody(
                        id,
                        flags.contains(FrameFlags::OVERWRITE_PERMITTED),
                    );
                }
                DecoderState::ReadingAddressedHeader(flags) => {
                    let remaining = src.remaining();
                    if remaining < MIN_REQUIRED {
                        *state = DecoderState::ReadingAddressedHeader(flags);
                        break Ok(None);
                    }
                    let mut bytes = src.as_ref();
                    let has_host = flags.contains(FrameFlags::HAS_HOST);
                    let overwrite_permitted = flags.contains(FrameFlags::OVERWRITE_PERMITTED);

                    let host_len = if has_host {
                        if remaining < MAX_REQUIRED {
                            *state = DecoderState::ReadingAddressedHeader(flags);
                            break Ok(None);
                        }
                        bytes.get_u64() as usize
                    } else {
                        0
                    };

                    let node_len = bytes.get_u64() as usize;
                    let lane_len = bytes.get_u64() as usize;

                    if bytes.remaining() < host_len + node_len + lane_len {
                        *state = DecoderState::ReadingAddressedHeader(flags);
                        break Ok(None);
                    }
                    let host = if has_host {
                        src.advance(MAX_REQUIRED);
                        Some(try_extract_utf8(src, host_len)?)
                    } else {
                        src.advance(MIN_REQUIRED);
                        None
                    };

                    let node = try_extract_utf8(src, node_len)?;
                    let lane = try_extract_utf8(src, lane_len)?;

                    *state = DecoderState::ReadingAddressedBody(
                        Address::new(host, node, lane),
                        overwrite_permitted,
                    );
                }
                DecoderState::ReadingAddressedBody(target, overwrite_permitted) => {
                    break match body_decoder.decode(src) {
                        Ok(Some(body)) => {
                            Ok(Some(CommandMessage::Addressed { target, command: body, overwrite_permitted }))
                        }
                        Ok(_) => {
                            *state = DecoderState::ReadingAddressedBody(target, overwrite_permitted);
                            Ok(None)
                        }
                        Err(e) => Err(e.into()),
                    };
                }
                DecoderState::ReadingRegisteredBody(target, overwrite_permitted) => {
                    break match body_decoder.decode(src) {
                        Ok(Some(body)) => {
                            Ok(Some(CommandMessage::Registered { target, command: body, overwrite_permitted }))
                        }
                        Ok(_) => {
                            *state = DecoderState::ReadingRegisteredBody(target, overwrite_permitted);
                            Ok(None)
                        }
                        Err(e) => Err(e.into()),
                    };
                }
            }
        }
    }
}

fn try_extract_utf8<S: TryFromUtf8Bytes>(
    src: &mut BytesMut,
    len: usize,
) -> Result<S, FrameIoError> {
    S::try_from_utf8_bytes(src.split_to(len).freeze()).map_err(|_| {
        FrameIoError::BadFrame(swimos_api::error::InvalidFrame::InvalidHeader {
            problem: Text::new("Ad-hoc message header contained invalid UTF8."),
        })
    })
}

/// Encoder for command messages with recon bodies.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommandMessageEncoder {
    inner: CommandEncoder<WithLenReconEncoder>,
}

impl<S: AsRef<str>, T: StructuralWritable> Encoder<CommandMessage<S, T>> for CommandMessageEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: CommandMessage<S, T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

/// Encoder for command messages with byte array bodies.
#[derive(Debug, Default, Clone, Copy)]
pub struct RawCommandMessageEncoder {
    inner: CommandEncoder<WithLengthBytesCodec>,
}

impl<S: AsRef<str>, T: AsRef<[u8]>> Encoder<CommandMessage<S, T>> for RawCommandMessageEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: CommandMessage<S, T>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

/// Decoder for command messages with Recon bodies.
pub struct CommandMessageDecoder<S, T: RecognizerReadable> {
    inner: CommandDecoder<S, WithLenRecognizerDecoder<T::Rec>>,
}

impl<S, T: RecognizerReadable> Default for CommandMessageDecoder<S, T> {
    fn default() -> Self {
        Self {
            inner: CommandDecoder {
                state: DecoderState::default(),
                body_decoder: WithLenRecognizerDecoder::new(T::make_recognizer()),
            },
        }
    }
}

impl<S, T> Decoder for CommandMessageDecoder<S, T>
where
    S: TryFromUtf8Bytes + Debug,
    T: RecognizerReadable,
{
    type Item = CommandMessage<S, T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}


/// Decoder for command messages with raw byte bodies. (The returned body could contain
/// a reference to the read from buffer).
#[derive(Debug)]
pub struct RawCommandMessageDecoder<S> {
    inner: CommandDecoder<S, WithLengthBytesCodec>,
}

impl<S> Default for RawCommandMessageDecoder<S> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<S> Decoder for RawCommandMessageDecoder<S>
where
    S: TryFromUtf8Bytes + std::fmt::Debug,
{
    type Item = CommandMessage<S, BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}
