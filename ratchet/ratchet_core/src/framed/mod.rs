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

#[cfg(test)]
mod tests;

use crate::errors::{Error, ErrorKind, ProtocolError};
use crate::protocol::{
    apply_mask, CloseCode, CloseReason, ControlCode, DataCode, FrameHeader, HeaderFlags,
    MessageType, OpCode, Role,
};
use crate::WebSocketStream;
use bytes::Buf;
use bytes::{BufMut, BytesMut};
use either::Either;
use nanorand::{WyRand, RNG};
use ratchet_ext::{ExtensionDecoder, FrameHeader as ExtFrameHeader, OpCode as ExtOpCode};
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter};
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

// todo: this could be reworked to save space as it's 64 bytes
#[derive(Debug)]
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

#[derive(Debug)]
pub struct FramedRead {
    read_buffer: BytesMut,
    decoder: FrameDecoder,
}

impl FramedRead {
    pub fn new(read_buffer: BytesMut) -> FramedRead {
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

    pub async fn read<I, E>(
        &mut self,
        io: &mut I,
        flags: &mut CodecFlags,
        read_into: &mut BytesMut,
        is_server: bool,
        rsv_bits: u8,
        max_size: usize,
        extension: &mut E,
    ) -> Result<Item, Error>
    where
        I: AsyncRead + Unpin,
        E: ExtensionDecoder,
    {
        loop {
            let (header, payload) = self.read_frame(io, is_server, rsv_bits, max_size).await?;

            match header.opcode {
                OpCode::DataCode(data_code) => {
                    if read_into.len() + payload.len() > max_size {
                        return Err(ProtocolError::FrameOverflow.into());
                    }

                    read_into.put(payload);

                    match data_code {
                        DataCode::Continuation => {
                            if header.flags.contains(HeaderFlags::FIN) {
                                let item = if flags.contains(CodecFlags::R_CONT) {
                                    extension_decode(
                                        read_into,
                                        extension,
                                        &header.flags,
                                        ExtOpCode::Continuation,
                                    )?;

                                    if flags.contains(CodecFlags::CONT_TYPE) {
                                        Item::Text
                                    } else {
                                        Item::Binary
                                    }
                                } else {
                                    return Err(ProtocolError::ContinuationNotStarted.into());
                                };
                                flags.remove(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                return Ok(item);
                            } else if flags.contains(CodecFlags::R_CONT) {
                                extension_decode(
                                    read_into,
                                    extension,
                                    &header.flags,
                                    ExtOpCode::Continuation,
                                )?;
                                continue;
                            } else {
                                return Err(ProtocolError::ContinuationNotStarted.into());
                            }
                        }
                        DataCode::Text => {
                            if flags.contains(CodecFlags::R_CONT) {
                                return Err(ProtocolError::ContinuationAlreadyStarted.into());
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                extension_decode(
                                    read_into,
                                    extension,
                                    &header.flags,
                                    ExtOpCode::Text,
                                )?;
                                return Ok(Item::Text);
                            } else {
                                flags.insert(CodecFlags::R_CONT | CodecFlags::CONT_TYPE);
                                extension_decode(
                                    read_into,
                                    extension,
                                    &header.flags,
                                    ExtOpCode::Text,
                                )?;
                                continue;
                            }
                        }
                        DataCode::Binary => {
                            if flags.contains(CodecFlags::R_CONT) {
                                return Err(ProtocolError::ContinuationAlreadyStarted.into());
                            } else if header.flags.contains(HeaderFlags::FIN) {
                                extension_decode(
                                    read_into,
                                    extension,
                                    &header.flags,
                                    ExtOpCode::Binary,
                                )?;
                                return Ok(Item::Binary);
                            } else {
                                debug_assert!(!flags.contains(CodecFlags::CONT_TYPE));
                                flags.insert(CodecFlags::R_CONT);
                                extension_decode(
                                    read_into,
                                    extension,
                                    &header.flags,
                                    ExtOpCode::Binary,
                                )?;
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
}

#[derive(Default)]
pub struct FramedWrite {
    write_buffer: BytesMut,
    rand: WyRand,
}

impl Debug for FramedWrite {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedWrite")
            .field("write_buffer", &self.write_buffer)
            .finish()
    }
}

impl FramedWrite {
    pub async fn write<I, A, F>(
        &mut self,
        io: &mut I,
        is_server: bool,
        opcode: OpCode,
        mut header_flags: HeaderFlags,
        mut payload_ref: A,
        extension: F,
    ) -> Result<(), Error>
    where
        I: AsyncWrite + Unpin,
        A: AsMut<[u8]>,
        F: FnMut(&mut BytesMut, &mut ExtFrameHeader) -> Result<(), Error>,
    {
        let FramedWrite { write_buffer, rand } = self;
        let payload = payload_ref.as_mut();

        let mut payload_bytes = BytesMut::with_capacity(payload.len());
        payload_bytes.extend_from_slice(payload);

        if let OpCode::DataCode(data_code) = opcode {
            extension_encode(
                &mut payload_bytes,
                extension,
                &mut header_flags,
                data_code.into(),
            )?;
        }

        // println!("Writing payload {:?}", payload_bytes.as_ref());

        let mask = if is_server {
            None
        } else {
            let mask = rand.generate();
            apply_mask(mask, payload_bytes.as_mut());
            Some(mask)
        };

        FrameHeader::write_into(
            write_buffer,
            opcode,
            header_flags,
            mask,
            payload_bytes.len(),
        );

        io.write_all(write_buffer).await?;
        write_buffer.clear();

        io.write_all(payload_bytes.as_mut())
            .await
            .map_err(Into::into)
    }
}

pub struct FramedIoParts<I> {
    pub io: I,
    pub reader: FramedRead,
    pub writer: FramedWrite,
    pub flags: CodecFlags,
    pub max_size: usize,
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
    pub fn from_parts(parts: FramedIoParts<I>) -> FramedIo<I> {
        let FramedIoParts {
            io,
            reader,
            writer,
            flags,
            max_size,
        } = parts;
        FramedIo {
            io,
            reader,
            writer,
            flags,
            max_size,
        }
    }

    pub fn into_parts(self) -> FramedIoParts<I> {
        let FramedIo {
            io,
            reader,
            writer,
            flags,
            max_size,
        } = self;
        FramedIoParts {
            io,
            reader,
            writer,
            flags,
            max_size,
        }
    }

    pub fn new(io: I, read_buffer: BytesMut, role: Role, max_size: usize, ext_bits: u8) -> Self {
        let flags = match role {
            Role::Client => CodecFlags::from_bits_truncate(ext_bits),
            Role::Server => CodecFlags::from_bits_truncate(CodecFlags::ROLE.bits() | ext_bits),
        };

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

    pub async fn write<A, F>(
        &mut self,
        opcode: OpCode,
        header_flags: HeaderFlags,
        payload_ref: A,
        extension: F,
    ) -> Result<(), Error>
    where
        A: AsMut<[u8]>,
        F: FnMut(&mut BytesMut, &mut ExtFrameHeader) -> Result<(), Error>,
    {
        let FramedIo {
            io, writer, flags, ..
        } = self;
        writer
            .write(
                io,
                flags.contains(CodecFlags::ROLE),
                opcode,
                header_flags,
                payload_ref,
                extension,
            )
            .await
    }

    pub(crate) async fn read_next<E>(
        &mut self,
        read_into: &mut BytesMut,
        extension: &mut E,
    ) -> Result<Item, Error>
    where
        E: ExtensionDecoder,
    {
        let FramedIo {
            io,
            reader,
            flags,
            max_size,
            ..
        } = self;
        read_next(io, reader, flags, *max_size, read_into, extension).await
    }

    pub async fn write_close(&mut self, reason: CloseReason) -> Result<(), Error> {
        let FramedIo {
            io, writer, flags, ..
        } = self;
        write_close(io, writer, reason, flags.contains(CodecFlags::ROLE)).await
    }

    pub async fn write_fragmented<A, F>(
        &mut self,
        buf: A,
        message_type: MessageType,
        fragment_size: usize,
        extension: F,
    ) -> Result<(), Error>
    where
        A: AsMut<[u8]>,
        F: FnMut(&mut BytesMut, &mut ExtFrameHeader) -> Result<(), Error>,
    {
        let FramedIo {
            io, writer, flags, ..
        } = self;
        write_fragmented(
            io,
            writer,
            buf,
            message_type,
            fragment_size,
            flags.contains(CodecFlags::ROLE),
            extension,
        )
        .await
    }
}

pub async fn read_next<I, E>(
    io: &mut I,
    reader: &mut FramedRead,
    flags: &mut CodecFlags,
    max_size: usize,
    read_into: &mut BytesMut,
    extension: &mut E,
) -> Result<Item, Error>
where
    I: AsyncRead + Unpin,
    E: ExtensionDecoder,
{
    let rsv_bits = flags.bits() & 0x70;
    let is_server = flags.contains(CodecFlags::ROLE);

    reader
        .read(
            io, flags, read_into, is_server, rsv_bits, max_size, extension,
        )
        .await
}

pub async fn write_close<I>(
    io: &mut I,
    writer: &mut FramedWrite,
    reason: CloseReason,
    is_server: bool,
) -> Result<(), Error>
where
    I: AsyncWrite + Unpin,
{
    let CloseReason { code, description } = reason;
    let mut payload = u16::from(code).to_be_bytes().to_vec();

    if let Some(description) = description {
        payload.extend_from_slice(description.as_bytes());
    }

    writer
        .write(
            io,
            is_server,
            OpCode::ControlCode(ControlCode::Close),
            HeaderFlags::FIN,
            payload,
            |_, _| Ok(()),
        )
        .await
        .map_err(|e| Error::with_cause(ErrorKind::Close, e))
}

pub async fn write_fragmented<A, I, F>(
    io: &mut I,
    framed: &mut FramedWrite,
    mut buf_ref: A,
    message_type: MessageType,
    fragment_size: usize,
    is_server: bool,
    mut extension: F,
) -> Result<(), Error>
where
    A: AsMut<[u8]>,
    I: AsyncWrite + Unpin,
    F: FnMut(&mut BytesMut, &mut ExtFrameHeader) -> Result<(), Error>,
{
    let buf = buf_ref.as_mut();
    let mut chunks = buf.chunks_mut(fragment_size).peekable();
    match chunks.next() {
        Some(payload) => {
            let payload_type = match message_type {
                MessageType::Text => DataCode::Text,
                MessageType::Binary => DataCode::Binary,
            };

            let flags = if chunks.peek().is_none() {
                HeaderFlags::FIN
            } else {
                HeaderFlags::empty()
            };

            framed
                .write(
                    io,
                    is_server,
                    OpCode::DataCode(payload_type),
                    flags,
                    payload,
                    &mut extension,
                )
                .await?;
        }
        None => return Ok(()),
    }

    while let Some(payload) = chunks.next() {
        let flags = if chunks.peek().is_none() {
            HeaderFlags::FIN
        } else {
            HeaderFlags::empty()
        };

        framed
            .write(
                io,
                is_server,
                OpCode::DataCode(DataCode::Continuation),
                flags,
                payload,
                &mut extension,
            )
            .await?;
    }

    Ok(())
}

#[inline]
fn extension_decode<E>(
    payload: &mut BytesMut,
    extension: &mut E,
    header: &HeaderFlags,
    opcode: ExtOpCode,
) -> Result<(), Error>
where
    E: ExtensionDecoder,
{
    let mut frame_header = ExtFrameHeader {
        fin: header.is_fin(),
        rsv1: header.is_rsv1(),
        rsv2: header.is_rsv2(),
        rsv3: header.is_rsv3(),
        opcode,
    };

    extension
        .decode(payload, &mut frame_header)
        .map_err(|e| Error::with_cause(ErrorKind::Extension, e))
}

#[inline]
fn extension_encode<C>(
    payload: &mut BytesMut,
    mut extension: C,
    header: &mut HeaderFlags,
    opcode: ExtOpCode,
) -> Result<(), Error>
where
    C: FnMut(&mut BytesMut, &mut ExtFrameHeader) -> Result<(), Error>,
{
    let mut frame_header = ExtFrameHeader {
        fin: header.is_fin(),
        rsv1: header.is_rsv1(),
        rsv2: header.is_rsv2(),
        rsv3: header.is_rsv3(),
        opcode,
    };

    extension(payload, &mut frame_header)?;

    header.set(HeaderFlags::RSV_1, frame_header.rsv1);
    header.set(HeaderFlags::RSV_2, frame_header.rsv2);
    header.set(HeaderFlags::RSV_3, frame_header.rsv3);

    Ok(())
}
