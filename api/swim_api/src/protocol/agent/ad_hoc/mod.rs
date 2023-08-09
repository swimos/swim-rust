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

use bytes::{Buf, BufMut};
use swim_model::{address::Address, BytesStr, Text, TryFromUtf8Bytes};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::FrameIoError;

#[cfg(test)]
mod tests;

/// Message type for agents to send ad hoc commands to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdHocCommand<S, T> {
    pub address: Address<S>,
    pub command: T,
    pub overwrite_permitted: bool,
}

impl<S, T> AdHocCommand<S, T> {

    /// #Arguments
    /// * `address` - The target lane for the the command.
    /// * `command` - The body of the command message.
    /// * `overwrite_permitted` - Controls the behaviour of command handling in the case of back-pressure.
    /// If this is true, the command maybe be overwritten by a subsequent command to the same target (and so
    /// will never be sent). If false, the command will be queued instead.
    pub fn new(address: Address<S>, command: T, overwrite_permitted: bool) -> Self {
        AdHocCommand {
            address,
            command,
            overwrite_permitted,
        }
    }
}

#[derive(Debug, Default)]
pub struct AdHocCommandEncoder<E> {
    body_encoder: E,
}

impl<E> AdHocCommandEncoder<E> {
    pub fn new(body_encoder: E) -> Self {
        AdHocCommandEncoder { body_encoder }
    }
}

#[derive(Debug, Default)]
enum DecoderState<S> {
    #[default]
    ReadingHeader,
    ReadingBody(Address<S>, bool),
}

#[derive(Debug)]
pub struct AdHocCommandDecoder<S, D> {
    state: DecoderState<S>,
    body_decoder: D,
}

impl<S, D: Default> Default for AdHocCommandDecoder<S, D> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            body_decoder: Default::default(),
        }
    }
}

impl<S, D> AdHocCommandDecoder<S, D> {
    pub fn new(body_decoder: D) -> Self {
        AdHocCommandDecoder {
            body_decoder,
            state: Default::default(),
        }
    }
}

const OPT_TAG_LEN: usize = 1;
const LEN_LEN: usize = 8;

impl<S, T, E> Encoder<AdHocCommand<S, T>> for AdHocCommandEncoder<E>
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
        let AdHocCommandEncoder { body_encoder } = self;
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

impl<S, D> Decoder for AdHocCommandDecoder<S, D>
where
    S: TryFromUtf8Bytes + std::fmt::Debug,
    D: Decoder,
    FrameIoError: From<D::Error>,
{
    type Item = AdHocCommand<S, D::Item>;

    type Error = FrameIoError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let AdHocCommandDecoder {
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
                        match S::try_from_utf8_bytes(src.split_to(host_len).freeze()) {
                            Ok(host) => Some(host),
                            Err(_) => break Err(bad_utf8()),
                        }
                    } else {
                        src.advance(MIN_REQUIRED);
                        None
                    };

                    let node = match S::try_from_utf8_bytes(src.split_to(node_len).freeze()) {
                        Ok(node) => node,
                        Err(_) => break Err(bad_utf8()),
                    };
                    let lane = match S::try_from_utf8_bytes(src.split_to(lane_len).freeze()) {
                        Ok(lane) => lane,
                        Err(_) => break Err(bad_utf8()),
                    };
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

fn bad_utf8() -> FrameIoError {
    FrameIoError::BadFrame(crate::error::InvalidFrame::InvalidHeader {
        problem: Text::new("Ad-hoc message header contained invalid UTF8."),
    })
}

impl<T1, T2> PartialEq<AdHocCommand<&str, T1>> for AdHocCommand<Text, T2>
where
    T2: PartialEq<T1>,
{
    fn eq(&self, other: &AdHocCommand<&str, T1>) -> bool {
        self.address == other.address
            && self.command == other.command
            && self.overwrite_permitted == other.overwrite_permitted
    }
}

impl<T1, T2> PartialEq<AdHocCommand<&str, T1>> for AdHocCommand<BytesStr, T2>
where
    T2: PartialEq<T1>,
{
    fn eq(&self, other: &AdHocCommand<&str, T1>) -> bool {
        self.address == other.address
            && self.command == other.command
            && self.overwrite_permitted == other.overwrite_permitted
    }
}
