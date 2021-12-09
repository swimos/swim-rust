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

use crate::routing::{InvalidRoutingAddr, RoutingAddr, TaggedEnvelope};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::unfold;
use futures::Stream;
use std::convert::TryFrom;
use std::fmt::Write;
use std::str::Utf8Error;
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_form::structural::read::ReadError;
use swim_form::structural::write::StructuralWritable;
use swim_model::path::RelativePath;
use swim_model::{Text, Value};
use swim_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use swim_recon::printer::print_recon_compact;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, RequestKind};
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};
use uuid::Uuid;

mod routing;
#[cfg(test)]
mod tests;

pub use routing::RouteSender;

/// Operations that can be performed on an agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation<T> {
    Link,
    Sync,
    Unlink,
    Command(T),
}

/// Notifications that can be produced by an agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Notification<T> {
    Linked,
    Synced,
    Unlinked,
    Event(T),
}

/// Type of messages that can be sent to an agent/from a downlink..
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestMessage<T> {
    pub origin: RoutingAddr,
    pub path: RelativePath,
    pub envelope: Operation<T>,
}

/// Type of messages that can be sent by an agent/received by a downlink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseMessage<T> {
    pub origin: RoutingAddr,
    pub path: RelativePath,
    pub envelope: Notification<T>,
}

impl<T> RequestMessage<T> {
    pub fn link(source: RoutingAddr, path: RelativePath) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Link,
        }
    }

    pub fn sync(source: RoutingAddr, path: RelativePath) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Sync,
        }
    }

    pub fn unlink(source: RoutingAddr, path: RelativePath) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Unlink,
        }
    }

    pub fn command(source: RoutingAddr, path: RelativePath, body: T) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Command(body),
        }
    }

    pub fn kind(&self) -> RequestKind {
        match &self.envelope {
            Operation::Link => RequestKind::Link,
            Operation::Sync => RequestKind::Sync,
            Operation::Unlink => RequestKind::Unlink,
            Operation::Command(_) => RequestKind::Command,
        }
    }

    pub fn body(&self) -> Option<&T> {
        match &self.envelope {
            Operation::Command(body) => Some(body),
            _ => None,
        }
    }
}

impl<T> ResponseMessage<T> {
    pub fn linked(target: RoutingAddr, path: RelativePath) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Linked,
        }
    }

    pub fn synced(target: RoutingAddr, path: RelativePath) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Synced,
        }
    }

    pub fn unlinked(target: RoutingAddr, path: RelativePath) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Unlinked,
        }
    }

    pub fn event(target: RoutingAddr, path: RelativePath, body: T) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Event(body),
        }
    }
}

/// An agent message where the body is uninterpreted (represented as raw bytes).
pub type RawRequestMessage<'a> = RequestMessage<&'a [u8]>;

/// An agent message where the body is uninterpreted (represented as raw bytes).
pub type RawResponseMessage = ResponseMessage<Bytes>;

/// Tokio [`Encoder`] to encode a [`RawRequestMessage`] as a byte stream.
#[derive(Debug)]
pub struct RawRequestMessageEncoder;

/// Tokio [`Encoder`] to encode an [`ResponseMessage`] as a byte stream.
#[derive(Debug)]
pub struct ResponseMessageEncoder;

const OP_SHIFT: usize = 61;
const OP_MASK: u64 = 0b111 << OP_SHIFT;

const LINK: u64 = 0b000;
const SYNC: u64 = 0b001;
const UNLINK: u64 = 0b010;
const COMMAND: u64 = 0b011;

const LINKED: u64 = 0b100;
const SYNCED: u64 = 0b101;
const UNLINKED: u64 = 0b110;
const EVENT: u64 = 0b111;

impl<'a> Encoder<RawRequestMessage<'a>> for RawRequestMessageEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: RawRequestMessage<'a>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let RawRequestMessage {
            origin: source,
            path: RelativePath { node, lane },
            envelope,
        } = item;
        dst.reserve(HEADER_INIT_LEN + lane.len());
        dst.put_u128(source.uuid().as_u128());
        let node_len = u32::try_from(node.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        match envelope {
            Operation::Link => {
                dst.put_u64(LINK << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Operation::Sync => {
                dst.put_u64(SYNC << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Operation::Unlink => {
                dst.put_u64(UNLINK << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Operation::Command(body) => {
                let body_len = body.len() as u64;
                if body_len & OP_MASK != 0 {
                    panic!("Body too large.")
                }
                dst.put_u64(body_len | (COMMAND << OP_SHIFT));
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
                dst.reserve(body.len());
                dst.put_slice(body);
            }
        }
        Ok(())
    }
}

const RESERVE_INIT: usize = 256;
const RESERVE_MULT: usize = 2;

impl<T> Encoder<ResponseMessage<T>> for ResponseMessageEncoder
where
    T: StructuralWritable,
{
    type Error = std::io::Error;

    fn encode(&mut self, item: ResponseMessage<T>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let ResponseMessage {
            origin: source,
            path: RelativePath { node, lane },
            envelope,
        } = item;
        dst.reserve(HEADER_INIT_LEN + lane.len());
        dst.put_u128(source.uuid().as_u128());
        let node_len = u32::try_from(node.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        match envelope {
            Notification::Linked => {
                dst.put_u64(LINKED << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Notification::Synced => {
                dst.put_u64(SYNCED << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Notification::Unlinked => {
                dst.put_u64(UNLINKED << OP_SHIFT);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
            }
            Notification::Event(body) => {
                let body_len_offset = dst.remaining();
                dst.put_u64(0);
                dst.put_slice(node.as_bytes());
                dst.put_slice(lane.as_bytes());
                let body_offset = dst.remaining();

                let mut next_res =
                    RESERVE_INIT.max(dst.remaining_mut().saturating_mul(RESERVE_MULT));
                loop {
                    if write!(dst, "{}", print_recon_compact(&body)).is_err() {
                        dst.truncate(body_offset);
                        dst.reserve(next_res);
                        next_res = next_res.saturating_mul(RESERVE_MULT);
                    } else {
                        break;
                    }
                }
                let body_len = (dst.remaining() - body_offset) as u64;
                if body_len & OP_MASK != 0 {
                    panic!("Body too large.")
                }
                let mut rewound = &mut dst.as_mut()[body_len_offset..];
                rewound.put_u64(body_len | (EVENT << OP_SHIFT));
            }
        }
        Ok(())
    }
}

enum State<T> {
    ReadingHeader,
    ReadingBody {
        source: RoutingAddr,
        path: RelativePath,
        remaining: usize,
    },
    AfterBody {
        message: Option<RequestMessage<T>>,
        remaining: usize,
    },
    Discarding {
        error: Option<AsyncParseError>,
        remaining: usize,
    },
}

/// Tokio [`Decoder`] that can read an [`AgentMessage`] from a stream of bytes, using a
/// [`RecognizerDecoder`] to interpret the body.
pub struct AgentMessageDecoder<T, R> {
    state: State<T>,
    recognizer: RecognizerDecoder<R>,
}

/// Tokio [`Decoder`] that can read an [`RawResponseMessage`] from a stream of bytes, using a
/// [`RecognizerDecoder`] to interpret the body.
#[derive(Default, Debug, Clone, Copy)]
pub struct RawResponseMessageDecoder;

impl<T, R> AgentMessageDecoder<T, R> {
    pub fn new(recognizer: R) -> Self {
        AgentMessageDecoder {
            state: State::ReadingHeader,
            recognizer: RecognizerDecoder::new(recognizer),
        }
    }
}

const HEADER_INIT_LEN: usize = 32;

#[derive(Error, Debug)]
pub enum MessageDecodeError {
    #[error("Error reading from the source: {0}")]
    Io(#[from] std::io::Error),
    #[error("Routing address is message is invalid.")]
    Id(#[from] InvalidRoutingAddr),
    #[error("Invalid lane name: {0}")]
    LaneName(#[from] Utf8Error),
    #[error("Invalid message body: {0}")]
    Body(#[from] AsyncParseError),
}

impl From<ReadError> for MessageDecodeError {
    fn from(e: ReadError) -> Self {
        MessageDecodeError::Body(AsyncParseError::Parser(ParseError::Structure(e)))
    }
}

impl MessageDecodeError {
    fn incomplete() -> Self {
        MessageDecodeError::Body(AsyncParseError::Parser(ParseError::Structure(
            ReadError::IncompleteRecord,
        )))
    }
}

impl<T, R> Decoder for AgentMessageDecoder<T, R>
where
    R: Recognizer<Target = T>,
{
    type Item = RequestMessage<T>;
    type Error = MessageDecodeError;

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
                    let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
                    let source = header.get_u128();
                    let id = RoutingAddr::try_from(Uuid::from_u128(source))?;
                    let node_len = header.get_u32() as usize;
                    let lane_len = header.get_u32() as usize;
                    let body_len_and_tag = header.get_u64();
                    let tag = (body_len_and_tag & OP_MASK) >> OP_SHIFT;
                    if src.remaining() < HEADER_INIT_LEN + node_len + lane_len {
                        src.reserve(node_len + lane_len as usize);
                        break Ok(None);
                    }
                    src.advance(HEADER_INIT_LEN);
                    let node = Text::new(std::str::from_utf8(&src.as_ref()[0..node_len])?);
                    src.advance(node_len);
                    let lane = Text::new(std::str::from_utf8(&src.as_ref()[0..lane_len])?);
                    src.advance(lane_len);
                    let path = RelativePath::new(node, lane);
                    match tag {
                        LINK => {
                            break Ok(Some(RequestMessage {
                                origin: id,
                                path,
                                envelope: Operation::Link,
                            }));
                        }
                        SYNC => {
                            break Ok(Some(RequestMessage {
                                origin: id,
                                path,
                                envelope: Operation::Sync,
                            }));
                        }
                        UNLINK => {
                            break Ok(Some(RequestMessage {
                                origin: id,
                                path,
                                envelope: Operation::Unlink,
                            }));
                        }
                        _ => {
                            let body_len = (body_len_and_tag & !OP_MASK) as usize;
                            *state = State::ReadingBody {
                                source: id,
                                path,
                                remaining: body_len,
                            };
                            recognizer.reset();
                        }
                    }
                }
                State::ReadingBody {
                    source,
                    path,
                    remaining,
                } => {
                    let to_split = (*remaining).min(src.remaining());
                    let rem = src.split_off(to_split);
                    let buf_remaining = src.remaining();
                    let end_of_message = *remaining <= buf_remaining;
                    let decode_result = recognizer.decode(src);
                    let new_remaining = src.remaining();
                    let consumed = buf_remaining - new_remaining;
                    *remaining -= consumed;
                    match decode_result {
                        Ok(Some(result)) => {
                            src.unsplit(rem);
                            *state = State::AfterBody {
                                message: Some(RequestMessage {
                                    origin: *source,
                                    path: std::mem::take(path),
                                    envelope: Operation::Command(result),
                                }),
                                remaining: *remaining,
                            }
                        }
                        Ok(None) => {
                            if end_of_message {
                                let eof_result = recognizer.decode_eof(src)?;
                                let new_remaining = src.remaining();
                                let consumed = buf_remaining - new_remaining;
                                *remaining -= consumed;
                                src.unsplit(rem);
                                break if let Some(result) = eof_result {
                                    Ok(Some(RequestMessage {
                                        origin: *source,
                                        path: std::mem::take(path),
                                        envelope: Operation::Command(result),
                                    }))
                                } else {
                                    Err(MessageDecodeError::incomplete())
                                };
                            } else {
                                break Ok(None);
                            }
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                break Err(e.into());
                            } else {
                                *state = State::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
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
                State::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = State::ReadingHeader;
                        break Err(err.into());
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

impl Decoder for RawResponseMessageDecoder {
    type Item = RawResponseMessage;
    type Error = MessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < HEADER_INIT_LEN {
            src.reserve(HEADER_INIT_LEN - src.remaining() + 1);
            return Ok(None);
        }
        let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
        let target = header.get_u128();
        let target = if let Ok(id) = RoutingAddr::try_from(Uuid::from_u128(target)) {
            id
        } else {
            return Err(MessageDecodeError::Io(
                std::io::ErrorKind::InvalidData.into(),
            ));
        };
        let node_len = header.get_u32() as usize;
        let lane_len = header.get_u32() as usize;
        let body_len_and_tag = header.get_u64();
        let body_len = (body_len_and_tag & !OP_MASK) as usize;
        let required = HEADER_INIT_LEN + node_len + lane_len + body_len;
        if src.remaining() < required {
            src.reserve(required - src.remaining());
            return Ok(None);
        }
        src.advance(HEADER_INIT_LEN);
        let node = if let Ok(lane_name) = std::str::from_utf8(&src.as_ref()[0..node_len]) {
            Text::new(lane_name)
        } else {
            return Err(MessageDecodeError::Io(
                std::io::ErrorKind::InvalidData.into(),
            ));
        };
        src.advance(node_len);
        let lane = if let Ok(lane_name) = std::str::from_utf8(&src.as_ref()[0..lane_len]) {
            Text::new(lane_name)
        } else {
            return Err(MessageDecodeError::Io(
                std::io::ErrorKind::InvalidData.into(),
            ));
        };
        src.advance(lane_len);
        let path = RelativePath::new(node, lane);
        let tag = (body_len_and_tag & OP_MASK) >> OP_SHIFT;
        match tag {
            LINKED => Ok(Some(RawResponseMessage::linked(target, path))),
            SYNCED => Ok(Some(RawResponseMessage::synced(target, path))),
            UNLINKED => Ok(Some(RawResponseMessage::unlinked(target, path))),
            _ => {
                let body = src.split_to(body_len).freeze();
                Ok(Some(RawResponseMessage::event(target, path, body)))
            }
        }
    }
}

pub fn read_messages<R, T>(
    reader: R,
) -> impl Stream<Item = Result<RequestMessage<T>, MessageDecodeError>>
where
    R: AsyncRead + Unpin,
    T: RecognizerReadable,
{
    let decoder = AgentMessageDecoder::<T, _>::new(T::make_recognizer());
    FramedRead::new(reader, decoder)
}

fn fail(name: &str) -> MessageDecodeError {
    let err = ReadError::UnexpectedAttribute(Text::new(name));
    MessageDecodeError::Body(AsyncParseError::Parser(ParseError::Structure(err)))
}

impl<T: RecognizerReadable> TryFrom<TaggedEnvelope> for RequestMessage<T> {
    type Error = MessageDecodeError;

    fn try_from(value: TaggedEnvelope) -> Result<Self, Self::Error> {
        let TaggedEnvelope(addr, env) = value;
        match env {
            Envelope::Link {
                node_uri, lane_uri, ..
            } => Ok(RequestMessage::link(
                addr,
                RelativePath::new(node_uri, lane_uri),
            )),
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => Ok(RequestMessage::sync(
                addr,
                RelativePath::new(node_uri, lane_uri),
            )),
            Envelope::Unlink {
                node_uri, lane_uri, ..
            } => Ok(RequestMessage::unlink(
                addr,
                RelativePath::new(node_uri, lane_uri),
            )),
            Envelope::Command {
                node_uri,
                lane_uri,
                body,
                ..
            } => {
                let interpreted_body = T::try_from_structure(body.unwrap_or(Value::Extant))?;
                Ok(RequestMessage::command(
                    addr,
                    RelativePath::new(node_uri, lane_uri),
                    interpreted_body,
                ))
            }
            Envelope::Linked { .. } => Err(fail("linked")),
            Envelope::Synced { .. } => Err(fail("synced")),
            Envelope::Unlinked { .. } => Err(fail("unlinked")),
            Envelope::Event { .. } => Err(fail("event")),
            Envelope::Auth { .. } => Err(fail("auth")),
            Envelope::DeAuth { .. } => Err(fail("deauth")),
        }
    }
}

pub fn messages_from_envelopes<S, T>(
    envelopes: S,
) -> impl Stream<Item = Result<RequestMessage<T>, MessageDecodeError>>
where
    S: Stream<Item = TaggedEnvelope> + Unpin,
    T: RecognizerReadable,
{
    envelopes.map(TryFrom::try_from)
}

pub fn stop_on_failed<T, S>(
    stream: S,
    on_err: Option<promise::Sender<MessageDecodeError>>,
) -> impl Stream<Item = RequestMessage<T>>
where
    S: Stream<Item = Result<RequestMessage<T>, MessageDecodeError>>,
{
    unfold(
        (Box::pin(stream), on_err),
        |(mut stream, mut on_err)| async move {
            loop {
                let result = stream.next().await?;
                match result {
                    Ok(msg) => {
                        break Some((msg, (stream, on_err)));
                    }
                    Err(MessageDecodeError::Body(_)) => {}
                    Err(ow) => {
                        if let Some(tx) = on_err.take() {
                            let _ = tx.provide(ow);
                        }
                        break None;
                    }
                }
            }
        },
    )
}
