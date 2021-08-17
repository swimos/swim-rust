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

use crate::codec::CodecFlags;
use crate::errors::{Error, ErrorKind};
use crate::handshake::ProtocolError;
use crate::protocol::HeaderFlags;
use crate::Role;
use bytes::{Buf, BufMut};
use bytes::{Bytes, BytesMut};
use derive_more::Display;
use nanorand::{WyRand, RNG};
use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::mem::size_of;
use thiserror::Error;

const U16_MAX: usize = u16::MAX as usize;

#[derive(Debug, Display, PartialEq)]
pub enum OpCode {
    #[display(fmt = "{}", _0)]
    DataCode(DataCode),
    #[display(fmt = "{}", _0)]
    ControlCode(ControlCode),
}

impl OpCode {
    pub fn is_data(&self) -> bool {
        matches!(self, OpCode::DataCode(_))
    }

    pub fn is_control(&self) -> bool {
        matches!(self, OpCode::ControlCode(_))
    }
}

impl From<OpCode> for u8 {
    fn from(op: OpCode) -> Self {
        match op {
            OpCode::DataCode(code) => code as u8,
            OpCode::ControlCode(code) => code as u8,
        }
    }
}

#[derive(Debug, Display, PartialEq)]
pub enum DataCode {
    #[display(fmt = "Continuation")]
    Continuation = 0,
    #[display(fmt = "Text")]
    Text = 1,
    #[display(fmt = "Binary")]
    Binary = 2,
}

#[derive(Debug, Display, PartialEq)]
pub enum ControlCode {
    #[display(fmt = "Close")]
    Close = 8,
    #[display(fmt = "Ping")]
    Ping = 9,
    #[display(fmt = "Pong")]
    Pong = 10,
}

#[derive(Debug, Error, PartialEq)]
pub enum OpCodeParseErr {
    #[error("Reserved OpCode: `{0}`")]
    Reserved(u8),
    #[error("Invalid OpCode: `{0}`")]
    Invalid(u8),
}

impl TryFrom<u8> for OpCode {
    type Error = OpCodeParseErr;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OpCode::DataCode(DataCode::Continuation)),
            1 => Ok(OpCode::DataCode(DataCode::Text)),
            2 => Ok(OpCode::DataCode(DataCode::Binary)),
            r @ 3..=7 => Err(OpCodeParseErr::Reserved(r)),
            8 => Ok(OpCode::ControlCode(ControlCode::Close)),
            9 => Ok(OpCode::ControlCode(ControlCode::Ping)),
            10 => Ok(OpCode::ControlCode(ControlCode::Pong)),
            r @ 11..=15 => Err(OpCodeParseErr::Reserved(r)),
            e => Err(OpCodeParseErr::Invalid(e)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct CloseReason {
    pub code: CloseCode,
    pub description: Option<String>,
}

impl CloseReason {
    pub fn new(code: CloseCode, description: Option<String>) -> Self {
        CloseReason { code, description }
    }
}

/// # Additional implementation sources:
/// https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent
/// https://mailarchive.ietf.org/arch/msg/hybi/P_1vbD9uyHl63nbIIbFxKMfSwcM/
/// https://tools.ietf.org/id/draft-ietf-hybi-thewebsocketprotocol-09.html
#[derive(Debug, PartialEq)]
pub enum CloseCode {
    Normal,
    GoingAway,
    Protocol,
    Unsupported,
    Status,
    Abnormal,
    Invalid,
    Policy,
    Overflow,
    Extension,
    Unexpected,
    Restarting,
    TryAgain,
    Tls,
    ReservedExtension(u16),
    Library(u16),
    Application(u16),
}

#[derive(Error, Debug)]
#[error("Unknown close code: `{0}`")]
pub struct CloseCodeParseErr(u16);

impl TryFrom<u16> for CloseCode {
    type Error = CloseCodeParseErr;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            n @ 0..=999 => Err(CloseCodeParseErr(n)),
            1000 => Ok(CloseCode::Normal),
            1001 => Ok(CloseCode::GoingAway),
            1002 => Ok(CloseCode::Protocol),
            1003 => Ok(CloseCode::Unexpected),
            1005 => Ok(CloseCode::Status),
            1006 => Ok(CloseCode::Abnormal),
            1007 => Ok(CloseCode::Invalid),
            1008 => Ok(CloseCode::Policy),
            1009 => Ok(CloseCode::Overflow),
            1010 => Ok(CloseCode::Extension),
            1011 => Ok(CloseCode::Unexpected),
            1012 => Ok(CloseCode::Restarting),
            1013 => Ok(CloseCode::TryAgain),
            1015 => Ok(CloseCode::Tls),
            n @ 1016..=1999 => Err(CloseCodeParseErr(n)),
            n @ 2000..=2999 => Ok(CloseCode::ReservedExtension(n)),
            n @ 3000..=3999 => Ok(CloseCode::Library(n)),
            n @ 4000..=4999 => Ok(CloseCode::Application(n)),
            n => Err(CloseCodeParseErr(n)),
        }
    }
}

impl From<CloseCode> for u16 {
    fn from(code: CloseCode) -> u16 {
        match code {
            CloseCode::Normal => 1000,
            CloseCode::GoingAway => 1001,
            CloseCode::Protocol => 1002,
            CloseCode::Unsupported => 1003,
            CloseCode::Status => 1005,
            CloseCode::Abnormal => 1006,
            CloseCode::Invalid => 1007,
            CloseCode::Policy => 1008,
            CloseCode::Overflow => 1009,
            CloseCode::Extension => 1010,
            CloseCode::Unexpected => 1011,
            CloseCode::Restarting => 1012,
            CloseCode::TryAgain => 1013,
            CloseCode::Tls => 1015,
            CloseCode::ReservedExtension(n) => n,
            CloseCode::Library(n) => n,
            CloseCode::Application(n) => n,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<CloseReason>),
}

impl Message {
    pub fn text_from_utf8(bytes: Vec<u8>) -> Result<Message, Error> {
        match String::from_utf8(bytes) {
            Ok(string) => Ok(Message::Text(string)),
            Err(e) => Err(Error::with_cause(ErrorKind::Encoding, e)),
        }
    }
}

impl AsMut<[u8]> for Message {
    fn as_mut(&mut self) -> &mut [u8] {
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub struct FrameHeader {
    pub opcode: OpCode,
    pub flags: HeaderFlags,
    pub mask: Option<u32>,
}

macro_rules! try_parse_int {
    ($source:ident, $offset:ident, $into:ty) => {{
        const WIDTH: usize = size_of::<$into>();
        match <[u8; WIDTH]>::try_from(&$source[$offset..$offset + WIDTH]) {
            Ok(len) => {
                let len = <$into>::from_be_bytes(len);
                $offset += WIDTH;
                len
            }
            Err(_) => return Ok(None),
        }
    }};
}

impl FrameHeader {
    pub fn new(opcode: OpCode, flags: HeaderFlags, mask: Option<u32>) -> Self {
        FrameHeader {
            opcode,
            flags,
            mask,
        }
    }

    pub fn read_from(
        source: &[u8],
        codec_flags: &CodecFlags,
        max_size: usize,
    ) -> Result<Option<(FrameHeader, usize, usize)>, Error> {
        let source_length = source.len();
        if source_length < 2 {
            return Ok(None);
        }

        let server = codec_flags.contains(CodecFlags::ROLE);

        let first = source[0];
        let received_flags = HeaderFlags::from_bits_truncate(first);
        let opcode = OpCode::try_from(first & 0xF)?;

        if opcode.is_control() && !received_flags.is_fin() {
            // rfc6455 § 5.4: Control frames themselves MUST NOT be fragmented
            return Err(ProtocolError::FragmentedControl.into());
        }

        if (received_flags.bits() & !codec_flags.bits() & 0x70) != 0 {
            // Peer set a RSV bit high that hasn't been negotiated
            return Err(ProtocolError::UnknownExtension.into());
        }

        let second = source[1];
        let masked = second & 0x80 != 0;

        if !masked && server {
            // rfc6455 § 6.1: Client must send masked data
            return Err(ProtocolError::UnmaskedFrame.into());
        } else if masked && !server {
            // rfc6455 § 6.2: Server must remove masking
            return Err(ProtocolError::MaskedFrame.into());
        }

        let payload_length = second & 0x7F;
        let mut offset = 2;

        let length: usize = if payload_length == 126 {
            try_parse_int!(source, offset, u16) as usize
        } else if payload_length == 127 {
            try_parse_int!(source, offset, u64) as usize
        } else {
            usize::from(payload_length)
        };

        if length > max_size {
            return Err(ProtocolError::FrameOverflow.into());
        }

        let mask = if masked {
            Some(try_parse_int!(source, offset, u32))
        } else {
            None
        };

        Ok(Some((
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

// todo this needs tidying up / removing
pub enum Payload<'p> {
    Owned(Vec<u8>),
    Unique(&'p mut [u8]),
}

impl<'p> Into<Payload<'p>> for Vec<u8> {
    fn into(self) -> Payload<'p> {
        Payload::Owned(self)
    }
}

impl<'p> Into<Payload<'p>> for &'p mut [u8] {
    fn into(self) -> Payload<'p> {
        Payload::Unique(self)
    }
}

impl<'p> Borrow<[u8]> for Payload<'p> {
    fn borrow(&self) -> &[u8] {
        match self {
            Payload::Owned(payload) => payload,
            Payload::Unique(payload) => payload,
        }
    }
}

impl<'p> BorrowMut<[u8]> for Payload<'p> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        match self {
            Payload::Owned(payload) => payload,
            Payload::Unique(payload) => payload,
        }
    }
}

pub struct Frame<'p> {
    pub header: FrameHeader,
    pub payload: Payload<'p>,
}

impl<'p> Frame<'p> {
    pub fn new<P>(header: FrameHeader, payload: P) -> Frame<'p>
    where
        P: Into<Payload<'p>>,
    {
        Frame {
            header,
            payload: payload.into(),
        }
    }

    pub fn write_into(self, dst: &mut BytesMut) {
        let Frame {
            header,
            mut payload,
        } = self;
        let FrameHeader {
            opcode,
            flags,
            mask,
        } = header;

        let mut payload: &mut [u8] = payload.borrow_mut();
        let payload_len = payload.len();
        let mut masked = mask.is_some();

        let (second, mut offset) = if masked { (0x80, 6) } else { (0x0, 2) };

        if payload_len >= U16_MAX {
            offset += 8;
        } else if payload_len > 125 {
            offset += 2;
        }

        let additional = if masked {
            payload.len() + offset
        } else {
            offset
        };

        dst.reserve(additional);
        let first = flags.bits | u8::from(opcode);

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
            apply_mask(mask, &mut payload);
            dst.put_u32_le(mask as u32);
        }

        dst.extend_from_slice(payload);
    }

    pub fn read_from(
        from: &mut BytesMut,
        codec_flags: &CodecFlags,
        max_size: usize,
    ) -> Result<Option<Frame<'p>>, Error> {
        let (header, header_len, payload_len) =
            match FrameHeader::read_from(from.as_ref(), codec_flags, max_size)? {
                Some(r) => r,
                None => return Ok(None),
            };

        if from.len() < header_len + payload_len {
            return Ok(None);
        }

        from.advance(header_len);

        if payload_len == 0 {
            return Ok(Some(Frame::new(header, Vec::new())));
        }

        let mut payload = from.split_to(payload_len);

        if let Some(mask) = header.mask {
            apply_mask(mask, &mut payload);
        }

        Ok(Some(Frame::new(header, payload.to_vec())))
    }
}

// todo speed up with an XOR lookup table
pub fn apply_mask(mask: u32, bytes: &mut [u8]) {
    let mask: [u8; 4] = mask.to_be_bytes();

    for i in 0..bytes.len() {
        bytes[i] ^= mask[i & 0x3]
    }
}
