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

use std::convert::TryFrom;
use std::str::Utf8Error;
use bytes::{BytesMut, BufMut, Buf};
use swim_model::{Text, Value};
use uuid::Uuid;
use tokio_util::codec::{Decoder, Encoder};
use swim_form::structural::read::recognizer::Recognizer;
use swim_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use thiserror::Error;
use swim_form::structural::read::ReadError;

pub enum AgentEnvelope {
    Link,
    Sync,
    Unlink,
    Command(Value),
}

pub struct AgentMessage<T> {
    source: Uuid,
    lane: Text,
    envelope: T,
}

type RawAgentMessage<'a> = AgentMessage<&'a str>;

pub struct RawAgentMessageEncoder;

impl<'a> Encoder<RawAgentMessage<'a>> for RawAgentMessageEncoder {
    type Error = std::io::Error;

    fn encode(&mut self,
              item: RawAgentMessage<'a>,
              dst: &mut BytesMut) -> Result<(), Self::Error> {
        let RawAgentMessage { source, lane, envelope } = item;
        dst.put_u128(source.as_u128());
        let len = u32::try_from(lane.len()).expect("Lane name to long.");
        dst.put_u32(len);
        dst.put_u64(envelope.len() as u64);
        dst.put_slice(lane.as_bytes());
        dst.put_slice(envelope.as_bytes());
        Ok(())
    }
}

enum State<T> {
    ReadingHeader,
    ReadingBody {
        lane: Text,
        remaining: usize,
    },
    AfterBody {
        lane: Text,
        body: Option<T>,
        remaining: usize,
    }
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
        AgentMessageDecodeError::Body(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)))
    }

}

impl <T, R> Decoder for AgentMessageDecoder<T, R>
where
    R: Recognizer<Target = T>,
{
    type Item = T;
    type Error = AgentMessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let AgentMessageDecoder { state, recognizer, .. } = self;
        loop {
            match state {
                State::ReadingHeader => {
                    if src.remaining() < HEADER_INIT_LEN {
                        src.reserve(HEADER_INIT_LEN);
                        break Ok(None);
                    }
                    let source = src.get_u128();
                    let lane_len = src.get_u32() as usize;
                    let body_len = src.get_u64() as usize;
                    if src.remaining() < lane_len {
                        src.reserve(lane_len as usize);
                        break Ok(None);
                    }
                    src.advance(HEADER_INIT_LEN);
                    let lane = Text::new(std::str::from_utf8(&src.as_ref()[0..lane_len])?);
                    src.advance(lane_len);
                    *state = State::ReadingBody { lane, remaining: body_len }
                },
                State::ReadingBody { lane, remaining } => {
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
                            lane: std::mem::take(lane),
                            body: Some(result),
                            remaining: *remaining,
                        }
                    } else if end_of_message {
                        let eof_result = recognizer.decode_eof(src)?;
                        let new_remaining = src.remaining();
                        let consumed = buf_remaining - new_remaining;
                        *remaining -= consumed;
                        src.unsplit(rem);
                        break if let Some(result) = eof_result {
                            Ok(Some(result))
                        } else {
                            Err(AgentMessageDecodeError::incomplete())
                        };
                    } else {
                        break Ok(None);
                    }
                }
                State::AfterBody { lane, body, remaining } => {
                    todo!()
                }
            }
        }
    }
}