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
use crate::protocol::frame::{write_into, ControlCode, DataCode, Message, OpCode};
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

        const ROLE      = 0b0000_0100;
        const SERVER    = Self::ROLE.bits;
        const CLIENT    = !Self::ROLE.bits;
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
            Role::Client => CodecFlags::CLIENT,
            Role::Server => CodecFlags::SERVER,
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
        self.flags.contains(CodecFlags::CLIENT)
    }

    pub fn is_server(&self) -> bool {
        self.flags.contains(CodecFlags::SERVER)
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

        let mask: u32 = self.rand.generate();
        // todo run bytes through extensions

        let flags = if self.is_client() {
            HeaderFlags::FIN | HeaderFlags::MASKED
        } else {
            HeaderFlags::FIN
        };

        write_into(dst, flags, opcode, bytes, mask);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}

#[test]
fn t() {
    let mut codec = Codec::new(Role::Client, 0);
    let mut buf = BytesMut::new();
    codec
        .encode(Message::Text("Test".to_string()), &mut buf)
        .unwrap();
    println!("{:?}", buf.as_ref());
}
