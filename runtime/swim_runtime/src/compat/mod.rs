// Copyright 2015-2021 SWIM.AI inc.
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
use std::convert::TryFrom;
use std::str::Utf8Error;
use swim_form::structural::read::recognizer::Recognizer;
use swim_form::structural::read::ReadError;
use swim_model::Text;
use swim_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

pub enum AgentOperation<T> {
    Link,
    Sync,
    Unlink,
    Command(T),
}

pub struct AgentMessage<T> {
    source: Uuid,
    lane: Text,
    envelope: AgentOperation<T>,
}

type RawAgentMessage<'a> = AgentMessage<&'a str>;

pub struct RawAgentMessageEncoder;

const OP_SHIFT: usize = 62;
const OP_MASK: u64 = 11 << OP_SHIFT;
const LINK: u64 = 0b00;
const SYNC: u64 = 0b01;
const UNLINK: u64 = 0b10;
const COMMAND: u64 = 0b11;

impl<'a> Encoder<RawAgentMessage<'a>> for RawAgentMessageEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RawAgentMessage<'a>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let RawAgentMessage {
            source,
            lane,
            envelope,
        } = item;
        dst.put_u128(source.as_u128());
        let len = u32::try_from(lane.len()).expect("Lane name to long.");
        dst.put_u32(len);
        match envelope {
            AgentOperation::Link => {
                dst.put_u64(LINK << OP_SHIFT);
                dst.put_slice(lane.as_bytes());
            }
            AgentOperation::Sync => {
                dst.put_u64(SYNC << OP_SHIFT);
                dst.put_slice(lane.as_bytes());
            }
            AgentOperation::Unlink => {
                dst.put_u64(UNLINK << OP_SHIFT);
                dst.put_slice(lane.as_bytes());
            }
            AgentOperation::Command(body) => {
                let body_len = body.len() as u64;
                if body_len & OP_MASK != 0 {
                    panic!("Body too large.")
                }
                dst.put_u64(body_len | (COMMAND << OP_SHIFT));
                dst.put_slice(lane.as_bytes());
                dst.put_slice(body.as_bytes());
            }
        }
        Ok(())
    }
}

enum State<T> {
    ReadingHeader,
    ReadingBody {
        source: Uuid,
        lane: Text,
        remaining: usize,
    },
    AfterBody {
        message: Option<AgentMessage<T>>,
        remaining: usize,
    },
}

pub struct AgentMessageDecoder<T, R> {
    state: State<T>,
    recognizer: RecognizerDecoder<R>,
}

impl<T, R> AgentMessageDecoder<T, R> {
    pub fn new(recognizer: R) -> Self {
        AgentMessageDecoder {
            state: State::ReadingHeader,
            recognizer: RecognizerDecoder::new(recognizer),
        }
    }
}

const HEADER_INIT_LEN: usize = 28;

#[derive(Error, Debug)]
pub enum AgentMessageDecodeError {
    #[error("Error reading from the source: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid lane name: {0}")]
    LaneName(#[from] Utf8Error),
    #[error("Invalid message body: {0}")]
    Body(#[from] AsyncParseError),
}

impl AgentMessageDecodeError {
    fn incomplete() -> Self {
        AgentMessageDecodeError::Body(AsyncParseError::Parser(ParseError::Structure(
            ReadError::IncompleteRecord,
        )))
    }
}

impl<T, R> Decoder for AgentMessageDecoder<T, R>
where
    R: Recognizer<Target = T>,
{
    type Item = AgentMessage<T>;
    type Error = AgentMessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let AgentMessageDecoder {
            state, recognizer, ..
        } = self;
        loop {
            match state {
                State::ReadingHeader => {
                    if src.remaining() < HEADER_INIT_LEN {
                        src.reserve(HEADER_INIT_LEN);
                        break Ok(None);
                    }
                    let source = src.get_u128();
                    let lane_len = src.get_u32() as usize;
                    let body_len_and_tag = src.get_u64();
                    let tag = (body_len_and_tag & OP_MASK) >> OP_SHIFT;
                    if src.remaining() < lane_len {
                        src.reserve(lane_len as usize);
                        break Ok(None);
                    }
                    src.advance(HEADER_INIT_LEN);
                    let lane = Text::new(std::str::from_utf8(&src.as_ref()[0..lane_len])?);
                    src.advance(lane_len);
                    match tag {
                        LINK => {
                            break Ok(Some(AgentMessage {
                                source: Uuid::from_u128(source),
                                lane,
                                envelope: AgentOperation::Link,
                            }));
                        }
                        SYNC => {
                            break Ok(Some(AgentMessage {
                                source: Uuid::from_u128(source),
                                lane,
                                envelope: AgentOperation::Sync,
                            }));
                        }
                        UNLINK => {
                            break Ok(Some(AgentMessage {
                                source: Uuid::from_u128(source),
                                lane,
                                envelope: AgentOperation::Unlink,
                            }));
                        }
                        _ => {
                            let body_len = (body_len_and_tag & !OP_MASK) as usize;
                            *state = State::ReadingBody {
                                source: Uuid::from_u128(source),
                                lane,
                                remaining: body_len,
                            };
                        }
                    }
                }
                State::ReadingBody {
                    source,
                    lane,
                    remaining,
                } => {
                    let to_split = (*remaining).min(src.remaining());
                    let rem = src.split_off(to_split);
                    let buf_remaining = src.remaining();
                    let end_of_message = *remaining <= buf_remaining;
                    let decode_result = recognizer.decode(src)?;
                    let new_remaining = src.remaining();
                    let consumed = buf_remaining - new_remaining;
                    *remaining -= consumed;
                    if let Some(result) = decode_result {
                        src.unsplit(rem);
                        *state = State::AfterBody {
                            message: Some(AgentMessage {
                                source: *source,
                                lane: std::mem::take(lane),
                                envelope: AgentOperation::Command(result),
                            }),
                            remaining: *remaining,
                        }
                    } else if end_of_message {
                        let eof_result = recognizer.decode_eof(src)?;
                        let new_remaining = src.remaining();
                        let consumed = buf_remaining - new_remaining;
                        *remaining -= consumed;
                        src.unsplit(rem);
                        break if let Some(result) = eof_result {
                            Ok(Some(AgentMessage {
                                source: *source,
                                lane: std::mem::take(lane),
                                envelope: AgentOperation::Command(result),
                            }))
                        } else {
                            Err(AgentMessageDecodeError::incomplete())
                        };
                    } else {
                        break Ok(None);
                    }
                }
                State::AfterBody { message, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = message.take();
                        *state = State::ReadingHeader;
                        break Ok(result);
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
