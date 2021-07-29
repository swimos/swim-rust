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

use crate::errors::Error;
use crate::protocol::frame::{ControlCode, DataCode, Frame, FrameHeader, Message, OpCode};
use crate::protocol::HeaderFlags;
use crate::Role;
use bytes::BytesMut;
use nanorand::{WyRand, RNG};
use tokio_util::codec::{Decoder, Encoder};

bitflags::bitflags! {
    pub struct CodecFlags: u8 {
        const R_CONT    = 0b0000_0001;
        const W_CONT    = 0b0000_0010;
        const CONT      = Self::R_CONT.bits | Self::W_CONT.bits;

        // If high then `server`, else `client
        const ROLE     = 0b0000_0100;

        // Below are the reserved bits used by the negotiated extensions
        const RSV1      = 0b0100_0000;
        const RSV2      = 0b0010_0000;
        const RSV3      = 0b0001_0000;
        const RESERVED  = Self::RSV1.bits | Self::RSV2.bits | Self::RSV3.bits;
    }
}

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

pub struct Codec {
    flags: CodecFlags,
    max_size: usize,
    rand: WyRand,
}

impl Codec {
    pub fn new(role: Role, max_size: usize) -> Self {
        let role_flag = match role {
            Role::Client => CodecFlags::empty(),
            Role::Server => CodecFlags::ROLE,
        };
        let flags = CodecFlags::from(role_flag);

        Codec {
            flags,
            max_size,
            rand: WyRand::new(),
        }
    }

    pub fn is_cont(&self) -> bool {
        self.flags.contains(CodecFlags::CONT)
    }

    pub fn is_read_cont(&self) -> bool {
        self.flags.contains(CodecFlags::R_CONT)
    }

    pub fn is_write_cont(&self) -> bool {
        self.flags.contains(CodecFlags::W_CONT)
    }

    pub fn is_client(&self) -> bool {
        !self.flags.contains(CodecFlags::ROLE)
    }

    pub fn is_server(&self) -> bool {
        self.flags.contains(CodecFlags::ROLE)
    }
}

impl Encoder<Message> for Codec {
    type Error = Error;

    fn encode(&mut self, message: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (opcode, bytes) = match message {
            Message::Text(text) => (OpCode::DataCode(DataCode::Text), text.into_bytes()),
            Message::Binary(data) => (OpCode::DataCode(DataCode::Binary), data),
            Message::Ping(data) => (OpCode::ControlCode(ControlCode::Ping), data),
            Message::Pong(data) => (OpCode::ControlCode(ControlCode::Pong), data),
        };

        // todo run bytes through extensions

        let (flags, mask) = if self.is_client() {
            (HeaderFlags::FIN, Some(self.rand.generate()))
        } else {
            (HeaderFlags::FIN, None)
        };

        let header = FrameHeader::new(opcode, flags, mask, bytes.len());
        Frame::new(header, bytes).write_into(dst);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // todo max_size from config
        match FrameHeader::read_from(src, &self.flags, usize::MAX)? {
            Some(_header) => {
                // allocate enough space in `src` to handle the payload
                unimplemented!()
            }
            None => Ok(None),
        }
    }
}
