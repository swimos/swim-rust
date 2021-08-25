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

use crate::errors::{CloseError, Error, ErrorKind, ProtocolError};
use crate::protocol::{
    apply_mask, CloseCode, CloseCodeParseErr, CloseReason, ControlCode, DataCode, FrameHeader,
    Message, OpCode, OpCodeParseErr,
};
use crate::protocol::{HeaderFlags, Role};
use crate::WebSocketStream;
use bytes::BufMut;
use bytes::{Buf, BytesMut};
use either::Either;
use nanorand::{WyRand, RNG};
use std::convert::TryFrom;
use std::str::Utf8Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CONTROL_FRAME_LEN: &str = "Control frame length greater than 125";
const CONST_STARTED: &str = "Continuation already started";
const CONST_NOT_STARTED: &str = "Continuation not started";
const ILLEGAL_CLOSE_CODE: &str = "Received a reserved close code";

const U16_MAX: usize = u16::MAX as usize;

pub enum CodecItem {
    Binary,
    Text,
    Ping(BytesMut),
    Pong,
    Close((Option<CloseReason>, BytesMut)),
}

bitflags::bitflags! {
    pub struct CodecFlags: u8 {
        const R_CONT    = 0b0000_0001;
        const W_CONT    = 0b0000_0010;
        // If high 'text' else 'binary
        const CONT_TYPE = 0b0000_1000;

        // If high then `server` else `client
        const ROLE     = 0b0000_0100;

        // Below are the reserved bits used by the negotiated extensions
        const RSV1      = 0b0100_0000;
        const RSV2      = 0b0010_0000;
        const RSV3      = 0b0001_0000;
        const RESERVED  = Self::RSV1.bits | Self::RSV2.bits | Self::RSV3.bits;
    }
}

#[allow(warnings)]
impl CodecFlags {
    pub fn is_rsv1(&self) -> bool {
        self.contains(CodecFlags::RSV1)
    }

    pub fn is_rsv2(&self) -> bool {
        self.contains(CodecFlags::RSV2)
    }

    pub fn is_rsv3(&self) -> bool {
        self.contains(CodecFlags::RSV3)
    }
}

pub struct ReadError {
    pub close_with: Option<CloseReason>,
    pub error: Error,
}

macro_rules! read_err_from {
    ($from:ident) => {
        impl From<$from> for ReadError {
            fn from(e: $from) -> Self {
                let cause = format!("{}", e);
                ReadError {
                    close_with: Some(CloseReason {
                        code: CloseCode::Protocol,
                        description: Some(cause.clone()),
                    }),
                    error: Error::with_cause(ErrorKind::Protocol, cause),
                }
            }
        }
    };
}

read_err_from!(OpCodeParseErr);
read_err_from!(CloseCodeParseErr);
read_err_from!(ProtocolError);
read_err_from!(Utf8Error);

impl From<std::io::Error> for ReadError {
    fn from(e: std::io::Error) -> Self {
        let cause = format!("{}", e);
        ReadError {
            close_with: None,
            error: Error::with_cause(ErrorKind::Protocol, cause),
        }
    }
}

impl ReadError {
    fn with<E>(msg: &'static str, source: E) -> ReadError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        ReadError {
            close_with: Some(CloseReason {
                code: CloseCode::Protocol,
                description: Some(msg.to_string()),
            }),
            error: Error::with_cause(ErrorKind::Protocol, source),
        }
    }
}

pub struct FramedIo<S> {
    io: S,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    rand: WyRand,
    flags: CodecFlags,
    max_size: usize,
}

impl<S> FramedIo<S>
where
    S: WebSocketStream,
{
    pub fn new(io: S, read_buffer: BytesMut, role: Role, max_size: usize) -> Self {
        let role_flag = match role {
            Role::Client => CodecFlags::empty(),
            Role::Server => CodecFlags::ROLE,
        };
        let flags = CodecFlags::from(role_flag);

        FramedIo {
            io,
            read_buffer,
            write_buffer: BytesMut::default(),
            rand: Default::default(),
            flags,
            max_size,
        }
    }
}

impl<S> FramedIo<S>
where
    S: WebSocketStream,
{
    pub async fn read_next(&mut self, read_into: &mut BytesMut) -> Result<CodecItem, ReadError> {
        let FramedIo {
            io,
            read_buffer,
            flags,
            ..
        } = self;

        loop {
            let (header, payload) = read_header(io, read_buffer, flags, usize::MAX).await?;

            match header.opcode {
                OpCode::DataCode(d) => {
                    read_into.put(payload);

                    match d {
                        DataCode::Continuation => {
                            if header.flags.contains(HeaderFlags::FIN) {
                                let item = if flags.contains(CodecFlags::R_CONT) {
                                    if flags.contains(CodecFlags::CONT_TYPE) {
                                        CodecItem::Text
                                    } else {
                                        CodecItem::Binary
                                    }
                                } else {
                                    return Err(ReadError::with(
                                        CONST_NOT_STARTED,
                                        ProtocolError::ContinuationNotStarted,
                                    ));
                                };

                                flags.remove(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                return Ok(item);
                            } else {
                                if flags.contains(CodecFlags::R_CONT) {
                                    continue;
                                } else {
                                    return Err(ReadError::with(
                                        CONST_NOT_STARTED,
                                        ProtocolError::FrameOverflow,
                                    ));
                                }
                            }
                        }
                        DataCode::Text => {
                            if flags.contains(CodecFlags::R_CONT) {
                                return Err(ReadError::with(
                                    CONST_STARTED,
                                    ProtocolError::ContinuationAlreadyStarted,
                                ));
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(CodecItem::Text);
                            } else {
                                if flags.contains(CodecFlags::R_CONT) {
                                    return Err(ReadError::with(
                                        CONST_STARTED,
                                        ProtocolError::FrameOverflow,
                                    ));
                                } else {
                                    flags.insert(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                    continue;
                                }
                            }
                        }
                        DataCode::Binary => {
                            if flags.contains(CodecFlags::R_CONT) {
                                return Err(ReadError::with(
                                    CONST_STARTED,
                                    ProtocolError::ContinuationAlreadyStarted,
                                ));
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(CodecItem::Binary);
                            } else {
                                if flags.contains(CodecFlags::R_CONT) {
                                    return Err(ReadError::with(
                                        CONTROL_FRAME_LEN,
                                        ProtocolError::FrameOverflow,
                                    ));
                                } else {
                                    debug_assert!(!flags.contains(CodecFlags::CONT_TYPE));
                                    flags.insert(CodecFlags::R_CONT);
                                    continue;
                                }
                            }
                        }
                    }
                }
                OpCode::ControlCode(c) => {
                    return match c {
                        ControlCode::Close => {
                            let reason = if payload.len() < 2 {
                                // todo this isn't very efficient
                                (None, BytesMut::new())
                            } else {
                                match CloseCode::try_from([payload[0], payload[1]])? {
                                    close_code @ CloseCode::Status
                                    | close_code @ CloseCode::Abnormal => {
                                        return Err(ReadError::with(
                                            ILLEGAL_CLOSE_CODE,
                                            ProtocolError::CloseCode(close_code.code()),
                                        ))
                                    }
                                    close_code => {
                                        let close_reason =
                                            std::str::from_utf8(&payload[2..])?.to_string();
                                        let description = if close_reason.is_empty() {
                                            None
                                        } else {
                                            Some(close_reason)
                                        };

                                        let reason = CloseReason::new(close_code, description);

                                        (Some(reason), payload)
                                    }
                                }
                            };

                            Ok(CodecItem::Close(reason))
                        }
                        ControlCode::Ping => {
                            if payload.len() > 125 {
                                return Err(ReadError::with(
                                    CONTROL_FRAME_LEN,
                                    ProtocolError::FrameOverflow,
                                ));
                            } else {
                                Ok(CodecItem::Ping(payload))
                            }
                        }
                        ControlCode::Pong => Ok(CodecItem::Pong),
                    };
                }
            }
        }
    }

    pub async fn write<A>(
        &mut self,
        opcode: OpCode,
        header_flags: HeaderFlags,
        mut payload_ref: A,
    ) -> Result<(), std::io::Error>
    where
        A: AsMut<[u8]>,
    {
        let FramedIo {
            io,
            write_buffer,
            rand,
            flags,
            ..
        } = self;

        let payload = payload_ref.as_mut();
        let payload_len = payload.len();

        let mask = if flags.contains(CodecFlags::ROLE) {
            None
        } else {
            let mask = rand.generate();
            apply_mask(mask, payload);
            Some(mask)
        };

        encode_header(opcode, header_flags, mask, payload_len, write_buffer);

        write_all(io, write_buffer).await?;
        write_buffer.clear();

        write_all(io, payload).await.map_err(Into::into)
    }

    pub async fn write_close(&mut self, reason: CloseReason) -> Result<(), Error> {
        let CloseReason { code, description } = reason;
        let mut payload = u16::from(code).to_be_bytes().to_vec();

        if let Some(description) = description {
            payload.extend_from_slice(description.as_bytes());
        }

        self.write(
            OpCode::ControlCode(ControlCode::Close),
            HeaderFlags::FIN,
            payload,
        )
        .await
        .map_err(|e| Error::with_cause(ErrorKind::Close, e))
    }
}

async fn read_header<S>(
    io: &mut S,
    buf: &mut BytesMut,
    flags: &CodecFlags,
    max_size: usize,
) -> Result<(FrameHeader, BytesMut), ReadError>
where
    S: WebSocketStream,
{
    loop {
        let (header, header_len, payload_len) = match FrameHeader::read_from(buf, flags, max_size)?
        {
            Either::Left(r) => r,
            Either::Right(count) => {
                let len = buf.len();
                buf.resize(len + count, 0u8);
                io.read_exact(&mut buf[len..]).await?;
                continue;
            }
        };

        let frame_len = header_len + payload_len;
        let len = buf.len();

        if buf.len() < frame_len {
            let dif = frame_len - buf.len();
            buf.resize(len + dif, 0u8);
            io.read_exact(&mut buf[len..]).await?;
        }

        buf.advance(header_len);

        let mut payload = buf.split_to(payload_len);
        if let Some(mask) = header.mask {
            apply_mask(mask, &mut payload);
        }

        break Ok((header, payload));
    }
}

async fn write_all<S>(io: &mut S, write_buffer: &[u8]) -> Result<(), std::io::Error>
where
    S: WebSocketStream,
{
    io.write_all(write_buffer).await
}

fn encode_header(
    opcode: OpCode,
    header_flags: HeaderFlags,
    mask: Option<u32>,
    payload_len: usize,
    write_buffer: &mut BytesMut,
) {
    let masked = mask.is_some();
    let (second, mut offset) = if masked { (0x80, 6) } else { (0x0, 2) };

    if payload_len >= U16_MAX {
        offset += 8;
    } else if payload_len > 125 {
        offset += 2;
    }

    let additional = if masked { payload_len + offset } else { offset };

    write_buffer.reserve(additional);
    let first = header_flags.bits() | u8::from(opcode);

    if payload_len < 126 {
        write_buffer.extend_from_slice(&[first, second | payload_len as u8]);
    } else if payload_len <= U16_MAX {
        write_buffer.extend_from_slice(&[first, second | 126]);
        write_buffer.put_u16(payload_len as u16);
    } else {
        write_buffer.extend_from_slice(&[first, second | 127]);
        write_buffer.put_u64(payload_len as u64);
    };

    if let Some(mask) = mask {
        write_buffer.put_u32(mask);
    }
}

pub async fn read_into<S>(
    framed: &mut FramedIo<S>,
    read_buffer: &mut BytesMut,
    closed: &mut bool,
) -> Result<Message, ReadError>
where
    S: WebSocketStream,
{
    match framed.read_next(read_buffer).await? {
        CodecItem::Binary => Ok(Message::Binary),
        CodecItem::Text => Ok(Message::Text),
        CodecItem::Ping(payload) => {
            framed
                .write(
                    OpCode::ControlCode(ControlCode::Pong),
                    HeaderFlags::FIN,
                    payload,
                )
                .await?;
            Ok(Message::Ping)
        }
        CodecItem::Pong => Ok(Message::Pong),
        CodecItem::Close((reason, payload)) => {
            framed
                .write(
                    OpCode::ControlCode(ControlCode::Close),
                    HeaderFlags::FIN,
                    payload,
                )
                .await?;

            *closed = true;
            Ok(Message::Close(reason))
        }
    }
}
