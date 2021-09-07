#[cfg(test)]
mod tests;

use crate::errors::{Error, ErrorKind, ProtocolError};
use crate::protocol::{
    apply_mask, CloseCode, CloseReason, ControlCode, DataCode, FrameHeader, HeaderFlags, OpCode,
    Role,
};
use crate::WebSocketStream;
use bytes::Buf;
use bytes::{BufMut, BytesMut};
use either::Either;
use nanorand::{WyRand, RNG};
use std::convert::TryFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, PartialEq)]
pub enum Item {
    Binary,
    Text,
    Ping(BytesMut),
    Pong(BytesMut),
    Close(Option<CloseReason>),
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
    ) -> Result<DecodeResult, Error> {
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

                    let result = DecodeResult::Finished(*header, payload);
                    *self = FrameDecoder::DecodingHeader;

                    return Ok(result);
                }
            }
        }
    }
}

struct FramedRead {
    read_buffer: BytesMut,
    decoder: FrameDecoder,
}

impl FramedRead {
    fn new(read_buffer: BytesMut) -> FramedRead {
        FramedRead {
            read_buffer,
            decoder: FrameDecoder::default(),
        }
    }

    async fn read_frame<I>(
        &mut self,
        io: &mut I,
        is_server: bool,
        rsv_bits: u8,
        max_size: usize,
    ) -> Result<(FrameHeader, BytesMut), Error>
    where
        I: AsyncRead + Unpin,
    {
        let FramedRead {
            read_buffer,
            decoder,
        } = self;

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
}

#[derive(Default)]
struct FramedWrite {
    write_buffer: BytesMut,
    rand: WyRand,
}

impl FramedWrite {
    async fn write<I, A>(
        &mut self,
        io: &mut I,
        flags: &CodecFlags,
        opcode: OpCode,
        header_flags: HeaderFlags,
        mut payload_ref: A,
    ) -> Result<(), std::io::Error>
    where
        I: AsyncWrite + Unpin,
        A: AsMut<[u8]>,
    {
        let FramedWrite { write_buffer, rand } = self;

        let payload = payload_ref.as_mut();

        let mask = if flags.contains(CodecFlags::ROLE) {
            None
        } else {
            let mask = rand.generate();
            apply_mask(mask, payload);
            Some(mask)
        };

        FrameHeader::write_into(write_buffer, opcode, header_flags, mask, payload.len());

        io.write_all(write_buffer).await?;
        write_buffer.clear();

        io.write_all(payload).await
    }
}

pub struct FramedIo<I> {
    io: I,
    reader: FramedRead,
    writer: FramedWrite,
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
            reader: FramedRead::new(read_buffer),
            writer: FramedWrite::default(),
            flags,
            max_size,
        }
    }

    pub fn is_server(&self) -> bool {
        self.flags.contains(CodecFlags::ROLE)
    }

    pub async fn write<A>(
        &mut self,
        opcode: OpCode,
        header_flags: HeaderFlags,
        payload_ref: A,
    ) -> Result<(), std::io::Error>
    where
        A: AsMut<[u8]>,
    {
        let FramedIo {
            io, writer, flags, ..
        } = self;

        writer
            .write(io, flags, opcode, header_flags, payload_ref)
            .await
    }

    pub(crate) async fn read_next(&mut self, read_into: &mut BytesMut) -> Result<Item, Error> {
        let FramedIo {
            io,
            reader,
            flags,
            max_size,
            ..
        } = self;

        let rsv_bits = flags.bits() & 0x8F;
        let is_server = flags.contains(CodecFlags::ROLE);

        loop {
            let (header, payload) = reader
                .read_frame(io, is_server, rsv_bits, *max_size)
                .await?;

            match header.opcode {
                OpCode::DataCode(data_code) => {
                    if read_into.len() + payload.len() > *max_size {
                        return Err(ProtocolError::FrameOverflow.into());
                    }

                    read_into.put(payload);

                    match data_code {
                        DataCode::Continuation => {
                            if header.flags.contains(HeaderFlags::FIN) {
                                let item = if self.flags.contains(CodecFlags::R_CONT) {
                                    if self.flags.contains(CodecFlags::CONT_TYPE) {
                                        Item::Text
                                    } else {
                                        Item::Binary
                                    }
                                } else {
                                    return Err(ProtocolError::ContinuationNotStarted.into());
                                };

                                self.flags
                                    .remove(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                return Ok(item);
                            } else {
                                if self.flags.contains(CodecFlags::R_CONT) {
                                    continue;
                                } else {
                                    return Err(ProtocolError::ContinuationNotStarted.into());
                                }
                            }
                        }
                        DataCode::Text => {
                            if self.flags.contains(CodecFlags::R_CONT) {
                                return Err(ProtocolError::ContinuationAlreadyStarted.into());
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(Item::Text);
                            } else {
                                self.flags
                                    .insert(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                continue;
                            }
                        }
                        DataCode::Binary => {
                            if self.flags.contains(CodecFlags::R_CONT) {
                                return Err(ProtocolError::ContinuationAlreadyStarted.into());
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                return Ok(Item::Binary);
                            } else {
                                debug_assert!(!self.flags.contains(CodecFlags::CONT_TYPE));
                                self.flags.insert(CodecFlags::R_CONT);
                                continue;
                            }
                        }
                    }
                }
                OpCode::ControlCode(c) => {
                    return match c {
                        ControlCode::Close => {
                            let reason = if payload.len() < 2 {
                                None
                            } else {
                                match CloseCode::try_from([payload[0], payload[1]])? {
                                    close_code if close_code.is_illegal() => {
                                        return Err(
                                            ProtocolError::CloseCode(u16::from(close_code)).into()
                                        )
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

                                        Some(reason)
                                    }
                                }
                            };

                            Ok(Item::Close(reason))
                        }
                        ControlCode::Ping => {
                            if payload.len() > 125 {
                                return Err(ProtocolError::FrameOverflow.into());
                            } else {
                                Ok(Item::Ping(payload))
                            }
                        }
                        ControlCode::Pong => {
                            if payload.len() > 125 {
                                return Err(ProtocolError::FrameOverflow.into());
                            } else {
                                Ok(Item::Pong(payload))
                            }
                        }
                    };
                }
            }
        }
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
