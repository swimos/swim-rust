use crate::framed::{CodecFlags, CodecItem, CONTROL_FRAME_LEN};
use crate::protocol::{
    apply_mask, CloseCode, CloseCodeParseErr, CloseReason, ControlCode, DataCode, FrameHeader,
    HeaderFlags, Message, OpCode, OpCodeParseErr, Role,
};
use crate::WebSocketStream;
use bytes::{BufMut, BytesMut};
use std::str::Utf8Error;
use tokio::io::AsyncReadExt;

use crate::errors::{Error, ErrorKind, ProtocolError};
use crate::framed::{CONST_NOT_STARTED, CONST_STARTED, ILLEGAL_CLOSE_CODE};
use bytes::Buf;
use either::Either;
use futures::AsyncRead;
use nanorand::WyRand;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct ReadError {
    pub close_with: Option<CloseReason>,
    pub error: Error,
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

pub enum FrameDecoder {
    DecodingHeader,
    DecodingPayload(FrameHeader, usize, usize),
}

impl Default for FrameDecoder {
    fn default() -> Self {
        FrameDecoder::DecodingHeader
    }
}

pub enum DecodeResult {
    Incomplete(usize),
    Finished(FrameHeader, BytesMut),
}

impl FrameDecoder {
    pub fn decode(
        &mut self,
        buf: &mut BytesMut,
        is_server: bool,
        rsv_bits: u8,
        max_size: usize,
    ) -> Result<DecodeResult, ReadError> {
        loop {
            match self {
                FrameDecoder::DecodingHeader => {
                    match FrameHeader::read_from(buf, is_server, rsv_bits, max_size)? {
                        Either::Left((header, header_len, payload_len)) => {
                            *self = FrameDecoder::DecodingPayload(header, header_len, payload_len);
                        }
                        Either::Right(count) => return Ok(DecodeResult::Incomplete(count)),
                    }
                }
                FrameDecoder::DecodingPayload(header, header_len, payload_len) => {
                    let frame_len = *header_len + *payload_len;
                    let buf_len = buf.len();

                    if buf_len < frame_len {
                        let dif = frame_len - buf_len;
                        return Ok(DecodeResult::Incomplete(dif));
                    }

                    buf.advance(*header_len);

                    let mut payload = buf.split_to(*payload_len);
                    if let Some(mask) = header.mask {
                        apply_mask(mask, &mut payload);
                    }

                    return Ok(DecodeResult::Finished(*header, payload));
                }
            }
        }
    }
}

struct FramedIo<I> {
    io: I,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    rand: WyRand,
    flags: CodecFlags,
    max_size: usize,
}

impl<I> FramedIo<I>
where
    I: WebSocketStream,
{
    pub fn new(io: I, read_buffer: BytesMut, role: Role, max_size: usize) -> Self {
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

    async fn read_frame(
        &mut self,
        is_server: bool,
        rsv_bits: u8,
        max_size: usize,
    ) -> Result<(FrameHeader, BytesMut), ReadError> {
        let FramedIo {
            io, read_buffer, ..
        } = self;
        let mut decoder = FrameDecoder::default();

        loop {
            match decoder.decode(read_buffer, is_server, rsv_bits, max_size)? {
                DecodeResult::Incomplete(count) => {
                    let len = read_buffer.len();
                    read_buffer.resize(len + count, 0u8);
                    io.read_exact(&mut read_buffer[len..]).await?;
                }
                DecodeResult::Finished(header, payload) => return Ok((header, payload)),
            }
        }
    }

    pub async fn read_next(&mut self, read_into: &mut BytesMut) -> Result<CodecItem, ReadError> {
        let rsv_bits = !self.flags.bits() & 0x70;
        let is_server = self.flags.contains(CodecFlags::ROLE);
        let max_size = self.max_size;

        loop {
            let (header, payload) = self.read_frame(is_server, rsv_bits, max_size).await?;

            match header.opcode {
                OpCode::DataCode(data_code) => {
                    read_into.put(payload);

                    match data_code {
                        DataCode::Continuation => {
                            if header.flags.contains(HeaderFlags::FIN) {
                                let item = if self.flags.contains(CodecFlags::R_CONT) {
                                    if self.flags.contains(CodecFlags::CONT_TYPE) {
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

                                self.flags
                                    .remove(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                return Ok(item);
                            } else {
                                if self.flags.contains(CodecFlags::R_CONT) {
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
                            if self.flags.contains(CodecFlags::R_CONT) {
                                return Err(ReadError::with(
                                    CONST_STARTED,
                                    ProtocolError::ContinuationAlreadyStarted,
                                ));
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(CodecItem::Text);
                            } else {
                                if self.flags.contains(CodecFlags::R_CONT) {
                                    return Err(ReadError::with(
                                        CONST_STARTED,
                                        ProtocolError::FrameOverflow,
                                    ));
                                } else {
                                    self.flags
                                        .insert(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                    continue;
                                }
                            }
                        }
                        DataCode::Binary => {
                            if self.flags.contains(CodecFlags::R_CONT) {
                                return Err(ReadError::with(
                                    CONST_STARTED,
                                    ProtocolError::ContinuationAlreadyStarted,
                                ));
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(CodecItem::Binary);
                            } else {
                                if self.flags.contains(CodecFlags::R_CONT) {
                                    return Err(ReadError::with(
                                        CONTROL_FRAME_LEN,
                                        ProtocolError::FrameOverflow,
                                    ));
                                } else {
                                    debug_assert!(!self.flags.contains(CodecFlags::CONT_TYPE));
                                    self.flags.insert(CodecFlags::R_CONT);
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
                        ControlCode::Pong => {
                            if payload.len() > 125 {
                                return Err(ReadError::with(
                                    CONTROL_FRAME_LEN,
                                    ProtocolError::FrameOverflow,
                                ));
                            } else {
                                Ok(CodecItem::Pong(payload))
                            }
                        }
                    };
                }
            }
        }
    }
}
