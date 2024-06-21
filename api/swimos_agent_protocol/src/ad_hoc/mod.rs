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

use bytes::{Buf, BufMut, BytesMut};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::{WithLenRecognizerDecoder, WithLenReconEncoder};
use swimos_utilities::encoding::{TryFromUtf8Bytes, WithLengthBytesCodec};
use tokio_util::codec::{Decoder, Encoder};

use swimos_api::{address::Address, error::FrameIoError};

use crate::AdHocCommand;

#[cfg(test)]
mod tests;

#[derive(Debug, Default, Clone, Copy)]
struct CommandEncoder<E> {
    body_encoder: E,
}

#[derive(Debug, Default)]
enum DecoderState<S> {
    #[default]
    ReadingHeader,
    ReadingBody(Address<S>, bool),
}

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

const OPT_TAG_LEN: usize = 1;
const LEN_LEN: usize = 8;

impl<S, T, E> Encoder<AdHocCommand<S, T>> for CommandEncoder<E>
where
    S: AsRef<str>,
    E: Encoder<T>,
{
    type Error = E::Error;

    fn encode(
        &mut self,
        item: AdHocCommand<S, T>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let CommandEncoder { body_encoder } = self;
        let AdHocCommand {
            address: Address { host, node, lane },
            command,
            overwrite_permitted,
        } = item;
        let node_str = node.as_ref();
        let lane_str = lane.as_ref();
        let required_base = OPT_TAG_LEN + 2 * LEN_LEN + node_str.len() + lane_str.len();
        match host {
            Some(host) => {
                let host_str = host.as_ref();
                let required = required_base + LEN_LEN + host_str.len();
                dst.reserve(required);
                let tag = if overwrite_permitted { 1 | 1 << 1 } else { 1 };
                dst.put_u8(tag);
                dst.put_u64(host_str.len() as u64);
                dst.put_u64(node_str.len() as u64);
                dst.put_u64(lane_str.len() as u64);
                dst.put(host_str.as_bytes());
            }
            None => {
                dst.reserve(required_base);
                if overwrite_permitted {
                    dst.put_u8(1 << 1);
                } else {
                    dst.put_u8(0);
                }
                dst.put_u64(node_str.len() as u64);
                dst.put_u64(lane_str.len() as u64);
            }
        }
        dst.put(node_str.as_bytes());
        dst.put(lane_str.as_bytes());
        body_encoder.encode(command, dst)
    }
}

const MIN_REQUIRED: usize = OPT_TAG_LEN + 2 * LEN_LEN;
const MAX_REQUIRED: usize = OPT_TAG_LEN + 3 * LEN_LEN;

impl<S, D> Decoder for CommandDecoder<S, D>
where
    S: TryFromUtf8Bytes + std::fmt::Debug,
    D: Decoder,
    FrameIoError: From<D::Error>,
{
    type Item = AdHocCommand<S, D::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let CommandDecoder {
            state,
            body_decoder,
            ..
        } = self;
        loop {
            match std::mem::take(state) {
                DecoderState::ReadingHeader => {
                    let remaining = src.remaining();
                    if remaining < MIN_REQUIRED {
                        break Ok(None);
                    }
                    let mut bytes = src.as_ref();
                    let tag = bytes.get_u8();
                    let has_host = tag & 1 != 0;
                    let overwrite_permitted = (tag >> 1) & 1 != 0;

                    let host_len = if has_host {
                        if remaining < MAX_REQUIRED {
                            break Ok(None);
                        }
                        bytes.get_u64() as usize
                    } else {
                        0
                    };

                    let node_len = bytes.get_u64() as usize;
                    let lane_len = bytes.get_u64() as usize;

                    if bytes.remaining() < host_len + node_len + lane_len {
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
                    *state = DecoderState::ReadingBody(
                        Address::new(host, node, lane),
                        overwrite_permitted,
                    );
                }
                DecoderState::ReadingBody(address, overwrite_permitted) => {
                    break match body_decoder.decode(src) {
                        Ok(Some(body)) => {
                            Ok(Some(AdHocCommand::new(address, body, overwrite_permitted)))
                        }
                        Ok(_) => {
                            *state = DecoderState::ReadingBody(address, overwrite_permitted);
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

#[derive(Debug, Default, Clone, Copy)]
pub struct AdHocCommandEncoder {
    inner: CommandEncoder<WithLenReconEncoder>,
}

impl<S: AsRef<str>, T: StructuralWritable> Encoder<AdHocCommand<S, T>> for AdHocCommandEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: AdHocCommand<S, T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RawAdHocCommandEncoder {
    inner: CommandEncoder<WithLengthBytesCodec>,
}

impl<S: AsRef<str>, T: AsRef<[u8]>> Encoder<AdHocCommand<S, T>> for RawAdHocCommandEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: AdHocCommand<S, T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

pub struct AdHocCommandDecoder<S, T: RecognizerReadable> {
    inner: CommandDecoder<S, WithLenRecognizerDecoder<T::Rec>>,
}

impl<S, T: RecognizerReadable> Default for AdHocCommandDecoder<S, T> {
    fn default() -> Self {
        Self {
            inner: CommandDecoder {
                state: DecoderState::default(),
                body_decoder: WithLenRecognizerDecoder::new(T::make_recognizer()),
            },
        }
    }
}

impl<S, T> Decoder for AdHocCommandDecoder<S, T>
where
    S: TryFromUtf8Bytes + Debug,
    T: RecognizerReadable,
{
    type Item = AdHocCommand<S, T>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}

#[derive(Debug)]
pub struct RawAdHocCommandDecoder<S> {
    inner: CommandDecoder<S, WithLengthBytesCodec>,
}

impl<S> Default for RawAdHocCommandDecoder<S> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<S> Decoder for RawAdHocCommandDecoder<S>
where
    S: TryFromUtf8Bytes + std::fmt::Debug,
{
    type Item = AdHocCommand<S, BytesMut>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.inner.decode(src)
    }
}
