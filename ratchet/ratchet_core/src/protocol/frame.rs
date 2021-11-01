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

use crate::errors::ProtocolError;
use crate::protocol::{HeaderFlags, OpCode};
use bytes::{BufMut, BytesMut};
use either::Either;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

const U16_MAX: usize = u16::MAX as usize;

pub struct FramePrinter<'l>(pub &'l FrameHeader);
impl<'l> Display for FramePrinter<'l> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let FrameHeader {
            opcode,
            flags,
            mask,
        } = self.0;
        write!(
            f,
            "opcode: {}, flags: {:?}, mask: {:?}",
            opcode, flags, mask
        )
    }
}

pub struct BorrowedFramePrinter<'l>(pub BorrowedFrameHeader<'l>);
impl<'l> BorrowedFramePrinter<'l> {
    pub fn new(
        opcode: &'l OpCode,
        flags: &'l HeaderFlags,
        mask: &'l Option<u32>,
    ) -> BorrowedFramePrinter<'l> {
        BorrowedFramePrinter(BorrowedFrameHeader {
            opcode,
            flags,
            mask,
        })
    }
}

impl<'l> Display for BorrowedFramePrinter<'l> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let BorrowedFrameHeader {
            opcode,
            flags,
            mask,
        } = self.0;
        write!(
            f,
            "opcode: {}, flags: {:?}, mask: {:?}",
            opcode, flags, mask
        )
    }
}

#[derive(Debug)]
pub struct BorrowedFrameHeader<'l> {
    pub opcode: &'l OpCode,
    pub flags: &'l HeaderFlags,
    pub mask: &'l Option<u32>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FrameHeader {
    pub opcode: OpCode,
    pub flags: HeaderFlags,
    pub mask: Option<u32>,
}

macro_rules! try_parse_int {
    ($source:ident, $offset:ident, $source_length:ident, $into:ty) => {{
        const WIDTH: usize = size_of::<$into>();
        if $source_length < WIDTH + $offset {
            return Ok(Either::Right($offset + WIDTH - $source_length));
        }

        match <[u8; WIDTH]>::try_from(&$source[$offset..$offset + WIDTH]) {
            Ok(len) => {
                let len = <$into>::from_be_bytes(len);
                $offset += WIDTH;
                len
            }
            Err(_) => return Ok(Either::Right($offset + WIDTH - $source_length)),
        }
    }};
}

impl FrameHeader {
    pub fn write_into(
        dst: &mut BytesMut,
        opcode: OpCode,
        header_flags: HeaderFlags,
        mask: Option<u32>,
        payload_len: usize,
    ) {
        let masked = mask.is_some();
        let (second, mut offset) = if masked { (0x80, 6) } else { (0x0, 2) };

        if payload_len >= U16_MAX {
            offset += 8;
        } else if payload_len > 125 {
            offset += 2;
        }

        let additional = if masked { payload_len + offset } else { offset };

        dst.reserve(additional);
        let first = header_flags.bits() | u8::from(opcode);

        if payload_len < 126 {
            dst.extend_from_slice(&[first, second | payload_len as u8]);
        } else if payload_len <= U16_MAX {
            dst.extend_from_slice(&[first, second | 126]);
            dst.put_u16(payload_len as u16);
        } else {
            dst.extend_from_slice(&[first, second | 127]);
            dst.put_u64(payload_len as u64);
        };

        if let Some(mask) = mask {
            dst.put_u32_le(mask);
        }
    }

    pub fn read_from(
        source: &[u8],
        is_server: bool,
        rsv_bits: u8,
        max_size: usize,
    ) -> Result<Either<(FrameHeader, usize, usize), usize>, ProtocolError> {
        let source_length = source.len();
        if source_length < 2 {
            return Ok(Either::Right(2 - source_length));
        }

        let first = source[0];
        let received_flags = HeaderFlags::from_bits_truncate(first);
        let opcode = OpCode::try_from(first & 0xF)?;

        if opcode.is_control() && !received_flags.is_fin() {
            // rfc6455 § 5.4: Control frames themselves MUST NOT be fragmented
            return Err(ProtocolError::FragmentedControl);
        }

        if (received_flags.bits() & !rsv_bits & 0x70) != 0 {
            // Peer set a RSV bit high that hasn't been negotiated
            return Err(ProtocolError::UnknownExtension);
        }

        let second = source[1];
        let masked = second & 0x80 != 0;

        if !masked && is_server {
            // rfc6455 § 6.1: Client must send masked data
            return Err(ProtocolError::UnmaskedFrame);
        } else if masked && !is_server {
            // rfc6455 § 6.2: Server must remove masking
            return Err(ProtocolError::MaskedFrame);
        }

        let payload_length = second & 0x7F;
        let mut offset = 2;

        let length: usize = if payload_length == 126 {
            try_parse_int!(source, offset, source_length, u16) as usize
        } else if payload_length == 127 {
            try_parse_int!(source, offset, source_length, u64) as usize
        } else {
            usize::from(payload_length)
        };

        if length > max_size {
            return Err(ProtocolError::FrameOverflow);
        }

        let mask = if masked {
            Some(try_parse_int!(source, offset, source_length, u32))
        } else {
            None
        };

        Ok(Either::Left((
            (FrameHeader {
                opcode,
                flags: received_flags,
                mask,
            }),
            offset,
            length,
        )))
    }
}
