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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Stream;
use std::convert::TryFrom;
use std::fmt::{Debug, Write};
use std::str::Utf8Error;
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_form::structural::read::ReadError;
use swim_form::structural::write::StructuralWritable;
use swim_model::{Text, Value};
use swim_recon::parser::{AsyncParseError, ParseError, RecognizerDecoder};
use swim_recon::printer::print_recon_compact;
use swim_warp::envelope::{Envelope, RequestKind};
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, Encoder, FramedRead};
use uuid::Uuid;

use crate::bytes_str::BytesStr;

#[cfg(test)]
mod tests;

/// Operations that can be performed on an agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation<T> {
    Link,
    Sync,
    Unlink,
    Command(T),
}

/// Notifications that can be produced by an agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notification<T, U> {
    Linked,
    Synced,
    Unlinked(Option<U>),
    Event(T),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Path<P> {
    pub node: P,
    pub lane: P,
}

impl Path<Text> {
    pub fn text(node: &str, lane: &str) -> Self {
        Path::new(Text::new(node), Text::new(lane))
    }
}

impl<P> Path<P> {
    pub fn new(node: P, lane: P) -> Self {
        Path { node, lane }
    }
}

impl Path<BytesStr> {
    pub fn from_static_strs(node: &'static str, lane: &'static str) -> Self {
        Path {
            node: BytesStr::from_static_str(node),
            lane: BytesStr::from_static_str(lane),
        }
    }
}

/// Type of messages that can be sent to an agent/from a downlink..
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestMessage<P, T> {
    pub origin: Uuid,
    pub path: Path<P>,
    pub envelope: Operation<T>,
}

/// Type of messages that can be sent by an agent/received by a downlink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseMessage<P, T, U> {
    pub origin: Uuid,
    pub path: Path<P>,
    pub envelope: Notification<T, U>,
}

impl<P, T> RequestMessage<P, T> {
    pub fn link(source: Uuid, path: Path<P>) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Link,
        }
    }

    pub fn sync(source: Uuid, path: Path<P>) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Sync,
        }
    }

    pub fn unlink(source: Uuid, path: Path<P>) -> Self {
        RequestMessage {
            origin: source,
            path,
            envelope: Operation::Unlink,
        }
    }

    pub fn command(source: Uuid, path: Path<P>, body: T) -> Self {
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

impl<P, T, U> ResponseMessage<P, T, U> {
    pub fn linked(target: Uuid, path: Path<P>) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Linked,
        }
    }

    pub fn synced(target: Uuid, path: Path<P>) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Synced,
        }
    }

    pub fn unlinked(target: Uuid, path: Path<P>, body: Option<U>) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Unlinked(body),
        }
    }

    pub fn event(target: Uuid, path: Path<P>, body: T) -> Self {
        ResponseMessage {
            origin: target,
            path,
            envelope: Notification::Event(body),
        }
    }
}

/// An agent message where the body is uninterpreted (represented as raw bytes).
pub type RawRequestMessage<'a, P> = RequestMessage<P, &'a [u8]>;
pub type BytesRequestMessage = RequestMessage<BytesStr, Bytes>;

/// An agent message where the body is uninterpreted (represented as raw bytes).
pub type RawResponseMessage<'a, P> = ResponseMessage<P, &'a [u8], &'a [u8]>;
pub type BytesResponseMessage = ResponseMessage<BytesStr, Bytes, Bytes>;

/// Tokio [`Encoder`] to encode a [`RawRequestMessage`] as a byte stream.
#[derive(Debug, Default)]
pub struct RawRequestMessageEncoder;

/// Tokio [`Encoder`] to encode a [`RawResponseMessage`] as a byte stream.
#[derive(Debug, Default)]
pub struct RawResponseMessageEncoder;

#[derive(Debug)]
/// Tokio [`Encoder`] to encode an [`ResponseMessage`] as a byte stream.
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

impl<'a, P, B> Encoder<&'a RequestMessage<P, B>> for RawRequestMessageEncoder
where
    P: AsRef<str>,
    B: AsRef<[u8]>,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: &'a RequestMessage<P, B>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let RequestMessage {
            origin: source,
            path: Path { node, lane },
            envelope,
        } = item;
        let node_str = node.as_ref();
        let lane_str = lane.as_ref();
        dst.reserve(HEADER_INIT_LEN + lane_str.len() + node_str.len());
        dst.put_u128(source.as_u128());
        let node_len = u32::try_from(node_str.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane_str.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        match envelope {
            Operation::Link => {
                dst.put_u64(LINK << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Operation::Sync => {
                dst.put_u64(SYNC << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Operation::Unlink => {
                dst.put_u64(UNLINK << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Operation::Command(body) => {
                let body_bytes = body.as_ref();
                let body_len = body_bytes.len() as u64;
                if body_len & OP_MASK != 0 {
                    panic!("Body too large.")
                }
                dst.put_u64(body_len | (COMMAND << OP_SHIFT));
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
                dst.reserve(body_bytes.len());
                dst.put_slice(body_bytes);
            }
        }
        Ok(())
    }
}

impl<P, B> Encoder<RequestMessage<P, B>> for RawRequestMessageEncoder
where
    P: AsRef<str>,
    B: AsRef<[u8]>,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: RequestMessage<P, B>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(&item, dst)
    }
}

impl<'a, P, B1, B2> Encoder<&'a ResponseMessage<P, B1, B2>> for RawResponseMessageEncoder
where
    P: AsRef<str>,
    B1: AsRef<[u8]>,
    B2: AsRef<[u8]>,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: &'a ResponseMessage<P, B1, B2>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let ResponseMessage {
            origin: source,
            path: Path { node, lane },
            envelope,
        } = item;
        let node_str = node.as_ref();
        let lane_str = lane.as_ref();
        dst.reserve(HEADER_INIT_LEN + lane_str.len() + node_str.len());
        dst.put_u128(source.as_u128());
        let node_len = u32::try_from(node_str.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane_str.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        match envelope {
            Notification::Linked => {
                dst.put_u64(LINKED << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Notification::Synced => {
                dst.put_u64(SYNCED << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Notification::Unlinked(body) => {
                let body_len = body.as_ref().map(|b| b.as_ref().len()).unwrap_or_default();
                dst.put_u64(body_len as u64 | (UNLINKED << OP_SHIFT));
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
                dst.reserve(body_len);
                if let Some(body) = body {
                    dst.put_slice(body.as_ref());
                }
            }
            Notification::Event(body) => {
                let body_bytes = body.as_ref();
                let body_len = body_bytes.len() as u64;
                if body_len & OP_MASK != 0 {
                    panic!("Body too large.")
                }
                dst.put_u64(body_len | (EVENT << OP_SHIFT));
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
                dst.reserve(body_bytes.len());
                dst.put_slice(body_bytes);
            }
        }
        Ok(())
    }
}

impl<P, B1, B2> Encoder<ResponseMessage<P, B1, B2>> for RawResponseMessageEncoder
where
    P: AsRef<str>,
    B1: AsRef<[u8]>,
    B2: AsRef<[u8]>,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: ResponseMessage<P, B1, B2>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(&item, dst)
    }
}

const RESERVE_INIT: usize = 256;
const RESERVE_MULT: usize = 2;

impl<P, T, U> Encoder<ResponseMessage<P, T, U>> for ResponseMessageEncoder
where
    P: AsRef<str>,
    T: StructuralWritable,
    U: AsRef<[u8]>,
{
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: ResponseMessage<P, T, U>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let ResponseMessage {
            origin: source,
            path: Path { node, lane },
            envelope,
        } = item;
        let node_str = node.as_ref();
        let lane_str = lane.as_ref();
        dst.reserve(HEADER_INIT_LEN + node_str.len() + lane_str.len());
        dst.put_u128(source.as_u128());
        let node_len = u32::try_from(node_str.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane_str.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        match envelope {
            Notification::Linked => {
                dst.put_u64(LINKED << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Notification::Synced => {
                dst.put_u64(SYNCED << OP_SHIFT);
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
            }
            Notification::Unlinked(body) => {
                let body_len = body.as_ref().map(|b| b.as_ref().len()).unwrap_or_default();
                dst.put_u64(body_len as u64 | (UNLINKED << OP_SHIFT));
                dst.put_slice(node_str.as_bytes());
                dst.put_slice(lane_str.as_bytes());
                dst.reserve(body_len);
                if let Some(body) = body {
                    dst.put_slice(body.as_ref());
                }
            }
            Notification::Event(body) => {
                put_with_body(node.as_ref(), lane.as_ref(), EVENT, &body, dst);
            }
        }
        Ok(())
    }
}

fn put_with_body<T: StructuralWritable>(
    node: &str,
    lane: &str,
    code: u64,
    body: &T,
    dst: &mut BytesMut,
) {
    let body_len_offset = dst.remaining();
    dst.put_u64(0);
    dst.put_slice(node.as_bytes());
    dst.put_slice(lane.as_bytes());
    let body_offset = dst.remaining();

    let mut next_res = RESERVE_INIT.max(dst.remaining_mut().saturating_mul(RESERVE_MULT));
    loop {
        if write!(dst, "{}", print_recon_compact(body)).is_err() {
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
    rewound.put_u64(body_len | (code << OP_SHIFT));
}

#[derive(Debug)]
enum RequestState<P, T> {
    ReadingHeader,
    ReadingBody {
        source: Uuid,
        path: Path<P>,
        remaining: usize,
    },
    AfterBody {
        message: Option<RequestMessage<P, T>>,
        remaining: usize,
    },
    Discarding {
        error: Option<AsyncParseError>,
        remaining: usize,
    },
}

enum ResponseState<P, T, U> {
    ReadingHeader,
    ReadingBody {
        is_event: bool,
        source: Uuid,
        path: Path<P>,
        remaining: usize,
    },
    AfterBody {
        message: Option<ResponseMessage<P, T, U>>,
        remaining: usize,
    },
    Discarding {
        error: Option<AsyncParseError>,
        remaining: usize,
    },
}

/// Tokio [`Decoder`] that can read an [`RequestMessage`] from a stream of bytes, using a
/// [`RecognizerDecoder`] to interpret the body.
pub struct AgentMessageDecoder<T, R> {
    state: RequestState<Text, T>,
    recognizer: RecognizerDecoder<R>,
}

impl<T: Debug, R> Debug for AgentMessageDecoder<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentMessageDecoder")
            .field("state", &self.state)
            .field("recognizer", &"...")
            .finish()
    }
}

/// Tokio [`Decoder`] that can read an [`ResponseMessage`] from a stream of bytes, using a
/// [`RecognizerDecoder`] to interpret the body.
pub struct ClientMessageDecoder<T, R> {
    state: ResponseState<Text, T, Bytes>,
    recognizer: RecognizerDecoder<R>,
}

/// Tokio [`Decoder`] that can read an [`RawResponseMessage`] from a stream of bytes.
#[derive(Default, Debug, Clone, Copy)]
pub struct RawResponseMessageDecoder;

/// Tokio [`Decoder`] that can read an [`RawRequestMessage`] from a stream of bytes.
#[derive(Default, Debug, Clone, Copy)]
pub struct RawRequestMessageDecoder;

impl<T, R> AgentMessageDecoder<T, R> {
    pub fn new(recognizer: R) -> Self {
        AgentMessageDecoder {
            state: RequestState::ReadingHeader,
            recognizer: RecognizerDecoder::new(recognizer),
        }
    }
}

impl<T, R> ClientMessageDecoder<T, R> {
    pub fn new(recognizer: R) -> Self {
        ClientMessageDecoder {
            state: ResponseState::ReadingHeader,
            recognizer: RecognizerDecoder::new(recognizer),
        }
    }
}

const HEADER_INIT_LEN: usize = 32;

#[derive(Error, Debug)]
pub enum MessageDecodeError {
    #[error("Error reading from the source: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid lane name: {0}")]
    LaneName(#[from] Utf8Error),
    #[error("Unexpecetd message tag code: {0}")]
    UnexpectedCode(u64),
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
    type Item = RequestMessage<Text, T>;
    type Error = MessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let AgentMessageDecoder {
            state, recognizer, ..
        } = self;
        loop {
            match state {
                RequestState::ReadingHeader => {
                    if src.remaining() < HEADER_INIT_LEN {
                        src.reserve(HEADER_INIT_LEN);
                        break Ok(None);
                    }
                    let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
                    let source = header.get_u128();
                    let id = Uuid::from_u128(source);
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
                    let path = Path::new(node, lane);
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
                        COMMAND => {
                            let body_len = (body_len_and_tag & !OP_MASK) as usize;
                            *state = RequestState::ReadingBody {
                                source: id,
                                path,
                                remaining: body_len,
                            };
                        }
                        _ => {
                            break Err(MessageDecodeError::UnexpectedCode(tag));
                        }
                    }
                }
                RequestState::ReadingBody {
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
                            *state = RequestState::AfterBody {
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
                                let final_remaining = src.remaining();
                                let consumed = new_remaining - final_remaining;
                                *remaining -= consumed;
                                src.unsplit(rem);
                                let result = if let Some(result) = eof_result {
                                    Ok(Some(RequestMessage {
                                        origin: *source,
                                        path: std::mem::take(path),
                                        envelope: Operation::Command(result),
                                    }))
                                } else {
                                    Err(MessageDecodeError::incomplete())
                                };
                                *state = RequestState::ReadingHeader;
                                break result;
                            } else {
                                break Ok(None);
                            }
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                *state = RequestState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = RequestState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                RequestState::AfterBody { message, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = message.take();
                        *state = RequestState::ReadingHeader;
                        break Ok(result);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                RequestState::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = RequestState::ReadingHeader;
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

impl<T, R> Decoder for ClientMessageDecoder<T, R>
where
    R: Recognizer<Target = T>,
{
    type Item = ResponseMessage<Text, T, Bytes>;
    type Error = MessageDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let ClientMessageDecoder {
            state, recognizer, ..
        } = self;
        loop {
            match state {
                ResponseState::ReadingHeader => {
                    if src.remaining() < HEADER_INIT_LEN {
                        src.reserve(HEADER_INIT_LEN);
                        break Ok(None);
                    }
                    let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
                    let source = header.get_u128();
                    let id = Uuid::from_u128(source);
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
                    let path = Path::new(node, lane);
                    match tag {
                        LINKED => {
                            break Ok(Some(ResponseMessage {
                                origin: id,
                                path,
                                envelope: Notification::Linked,
                            }));
                        }
                        SYNCED => {
                            break Ok(Some(ResponseMessage {
                                origin: id,
                                path,
                                envelope: Notification::Synced,
                            }));
                        }
                        UNLINKED => {
                            let body_len = (body_len_and_tag & !OP_MASK) as usize;
                            *state = ResponseState::ReadingBody {
                                is_event: false,
                                source: id,
                                path,
                                remaining: body_len,
                            };
                        }
                        EVENT => {
                            let body_len = (body_len_and_tag & !OP_MASK) as usize;
                            *state = ResponseState::ReadingBody {
                                is_event: true,
                                source: id,
                                path,
                                remaining: body_len,
                            };
                        }
                        _ => {
                            break Err(MessageDecodeError::UnexpectedCode(tag));
                        }
                    }
                }
                ResponseState::ReadingBody {
                    is_event,
                    source,
                    path,
                    remaining,
                } if *is_event => {
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
                            *state = ResponseState::AfterBody {
                                message: Some(ResponseMessage {
                                    origin: *source,
                                    path: std::mem::take(path),
                                    envelope: Notification::Event(result),
                                }),
                                remaining: *remaining,
                            }
                        }
                        Ok(None) => {
                            if end_of_message {
                                let eof_result = recognizer.decode_eof(src)?;
                                let final_remaining = src.remaining();
                                let consumed = new_remaining - final_remaining;
                                *remaining -= consumed;
                                src.unsplit(rem);
                                let result = if let Some(result) = eof_result {
                                    Ok(Some(ResponseMessage {
                                        origin: *source,
                                        path: std::mem::take(path),
                                        envelope: Notification::Event(result),
                                    }))
                                } else {
                                    Err(MessageDecodeError::incomplete())
                                };
                                *state = ResponseState::ReadingHeader;
                                break result;
                            } else {
                                break Ok(None);
                            }
                        }
                        Err(e) => {
                            *remaining -= new_remaining;
                            src.unsplit(rem);
                            src.advance(new_remaining);
                            if *remaining == 0 {
                                *state = ResponseState::ReadingHeader;
                                break Err(e.into());
                            } else {
                                *state = ResponseState::Discarding {
                                    error: Some(e),
                                    remaining: *remaining,
                                }
                            }
                        }
                    }
                }
                ResponseState::ReadingBody {
                    source,
                    path,
                    remaining,
                    ..
                } => {
                    if *remaining > src.remaining() {
                        src.reserve(*remaining - src.remaining());
                        break Ok(None);
                    } else {
                        let body = if *remaining == 0 {
                            None
                        } else {
                            Some(src.split_to(*remaining).freeze())
                        };
                        let result = Ok(Some(ResponseMessage::unlinked(
                            *source,
                            std::mem::take(path),
                            body,
                        )));
                        *state = ResponseState::ReadingHeader;
                        break result;
                    }
                }
                ResponseState::AfterBody { message, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let result = message.take();
                        *state = ResponseState::ReadingHeader;
                        break Ok(result);
                    } else {
                        *remaining -= src.remaining();
                        src.clear();
                        break Ok(None);
                    }
                }
                ResponseState::Discarding { error, remaining } => {
                    if src.remaining() >= *remaining {
                        src.advance(*remaining);
                        let err = error.take().unwrap_or(AsyncParseError::UnconsumedInput);
                        *state = ResponseState::ReadingHeader;
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
    type Item = ResponseMessage<BytesStr, Bytes, Bytes>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < HEADER_INIT_LEN {
            src.reserve(HEADER_INIT_LEN - src.remaining() + 1);
            return Ok(None);
        }
        let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
        let target = header.get_u128();
        let target = Uuid::from_u128(target);
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
        let node_bytes = src.split_to(node_len).freeze();
        let node = BytesStr::try_from(node_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;

        let lane_bytes = src.split_to(lane_len).freeze();
        let lane = BytesStr::try_from(lane_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;

        let path = Path::new(node, lane);
        let tag = (body_len_and_tag & OP_MASK) >> OP_SHIFT;
        match tag {
            LINKED => Ok(Some(BytesResponseMessage::linked(target, path))),
            SYNCED => Ok(Some(BytesResponseMessage::synced(target, path))),
            UNLINKED => {
                let body = if body_len == 0 {
                    None
                } else {
                    Some(src.split_to(body_len).freeze())
                };
                Ok(Some(BytesResponseMessage::unlinked(target, path, body)))
            }
            _ => {
                let body = src.split_to(body_len).freeze();
                Ok(Some(BytesResponseMessage::event(target, path, body)))
            }
        }
    }
}

impl Decoder for RawRequestMessageDecoder {
    type Item = RequestMessage<BytesStr, Bytes>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.remaining() < HEADER_INIT_LEN {
            src.reserve(HEADER_INIT_LEN - src.remaining() + 1);
            return Ok(None);
        }
        let mut header = &src.as_ref()[0..HEADER_INIT_LEN];
        let origin = header.get_u128();
        let origin = Uuid::from_u128(origin);
        let node_len = header.get_u32() as usize;
        let lane_len = header.get_u32() as usize;
        let body_len_and_tag = header.get_u64();
        let body_len = (body_len_and_tag & !OP_MASK) as usize;
        let required = HEADER_INIT_LEN + node_len + lane_len + body_len;
        if src.remaining() < required {
            src.reserve(required);
            return Ok(None);
        }
        src.advance(HEADER_INIT_LEN);
        let node_bytes = src.split_to(node_len).freeze();
        let node = BytesStr::try_from(node_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;

        let lane_bytes = src.split_to(lane_len).freeze();
        let lane = BytesStr::try_from(lane_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidData))?;

        let path = Path::new(node, lane);
        let tag = (body_len_and_tag & OP_MASK) >> OP_SHIFT;
        match tag {
            LINK => Ok(Some(RequestMessage::link(origin, path))),
            SYNC => Ok(Some(RequestMessage::sync(origin, path))),
            UNLINK => Ok(Some(RequestMessage::unlink(origin, path))),
            _ => {
                let body = src.split_to(body_len).freeze();
                Ok(Some(RequestMessage::command(origin, path, body)))
            }
        }
    }
}

pub fn read_messages<R, T>(
    reader: R,
) -> impl Stream<Item = Result<RequestMessage<Text, T>, MessageDecodeError>>
where
    R: AsyncRead + Unpin,
    T: RecognizerReadable,
{
    let decoder = AgentMessageDecoder::<T, _>::new(T::make_recognizer());
    FramedRead::new(reader, decoder)
}

#[derive(Debug, Clone, Copy)]
pub struct EnvelopeEncoder(pub Uuid);

/// Temporary shim to allow the existing agent and downlink implementations to write in to a byte
/// channel using the same format as the request and response message encoders. This encoding is
/// lossy (the rate and priority fields of the envelopes are ignored and bodies are discarded for
/// most types of envelope). Attempting to encode auth/death envelopes will result in a a panic.
impl EnvelopeEncoder {
    pub fn new(addr: Uuid) -> Self {
        EnvelopeEncoder(addr)
    }
}

impl Encoder<Envelope> for EnvelopeEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: Envelope, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let EnvelopeEncoder(source) = self;
        let (node, lane, code, body) = match item {
            Envelope::Auth { .. } | Envelope::DeAuth { .. } => {
                panic!("Unexpected auth/death envelope.");
            }
            Envelope::Link {
                node_uri, lane_uri, ..
            } => (node_uri, lane_uri, LINK, None),
            Envelope::Sync {
                node_uri, lane_uri, ..
            } => (node_uri, lane_uri, SYNC, None),
            Envelope::Unlink {
                node_uri, lane_uri, ..
            } => (node_uri, lane_uri, UNLINK, None),
            Envelope::Command {
                node_uri,
                lane_uri,
                body,
            } => (
                node_uri,
                lane_uri,
                COMMAND,
                Some(body.unwrap_or(Value::Extant)),
            ),
            Envelope::Linked {
                node_uri, lane_uri, ..
            } => (node_uri, lane_uri, LINKED, None),
            Envelope::Synced {
                node_uri, lane_uri, ..
            } => (node_uri, lane_uri, SYNCED, None),
            Envelope::Unlinked {
                node_uri,
                lane_uri,
                body,
            } => (
                node_uri,
                lane_uri,
                UNLINKED,
                Some(body.unwrap_or(Value::Extant)),
            ),
            Envelope::Event {
                node_uri,
                lane_uri,
                body,
            } => (
                node_uri,
                lane_uri,
                EVENT,
                Some(body.unwrap_or(Value::Extant)),
            ),
        };
        dst.reserve(HEADER_INIT_LEN + lane.len() + node.len());
        dst.put_u128(source.as_u128());
        let node_len = u32::try_from(node.len()).expect("Node name to long.");
        let lane_len = u32::try_from(lane.len()).expect("Lane name to long.");
        dst.put_u32(node_len);
        dst.put_u32(lane_len);
        if let Some(body) = body {
            put_with_body(node.as_str(), lane.as_str(), code, &body, dst);
        } else {
            dst.put_u64(code << OP_SHIFT);
            dst.put_slice(node.as_bytes());
            dst.put_slice(lane.as_bytes());
        }
        Ok(())
    }
}
