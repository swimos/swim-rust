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

pub mod frame;
#[cfg(test)]
mod tests;

use crate::protocol::frame::OpCode;
use bytes::{Buf, BufMut, BytesMut};
use nanorand::{WyRand, RNG};
use std::io::Write;

bitflags::bitflags! {
    pub struct HeaderFlags: u8 {
        const FIN       = 0b1000_0000;

        const RSV_1     = 0b0100_0000;
        const RSV_2     = 0b0010_0000;
        const RSV_3     = 0b0001_0000;

        const RESERVED  = Self::RSV_1.bits | Self::RSV_2.bits | Self::RSV_3.bits;

        const MASKED    = 0b0000_1000;
    }
}

impl HeaderFlags {
    pub fn is_fin(&self) -> bool {
        self.contains(HeaderFlags::FIN)
    }

    pub fn is_rsv1(&self) -> bool {
        self.contains(HeaderFlags::RSV_1)
    }

    pub fn is_rsv2(&self) -> bool {
        self.contains(HeaderFlags::RSV_2)
    }

    pub fn is_rsv3(&self) -> bool {
        self.contains(HeaderFlags::RSV_3)
    }

    pub fn reserved(&self) -> bool {
        self.contains(HeaderFlags::RESERVED)
    }

    pub fn is_masked(&self) -> bool {
        self.contains(HeaderFlags::MASKED)
    }
}

pub struct Header {
    flags: HeaderFlags,
    opcode: OpCode,
    payload_length: usize,
}
//
// impl Header {
//     pub fn write_into<A>(
//         dst: &mut BytesMut,
//         flags: HeaderFlags,
//         opcode: OpCode,
//         payload_length: usize,
//     ) -> u32
//     where
//         A: AsRef<[u8]>,
//     {
//         let mut first = 0u8;
//
//         if flags.is_fin() {
//             first |= 0x80
//         }
//         if flags.is_rsv1() {
//             first |= 0x40
//         }
//         if flags.is_rsv2() {
//             first |= 0x20
//         }
//         if flags.is_rsv3() {
//             first |= 0x10
//         }
//         first |= Into::<u8>::into(opcode);
//
//         let (second, payload_length, mask) = if flags.is_masked() {
//             (0x80, payload_length + 4, 4)
//         } else {
//             (0, payload_length, 0)
//         };
//
//         if payload_len < 126 {
//             dst.reserve(payload_length + 2 + mask);
//             dst.extend_from_slice(&[one, second | payload_length as u8]);
//         } else if payload_len <= u16::MAX {
//             dst.reserve(payload_length + 4 + mask);
//             dst.extend_from_slice(&[one, second | 126]);
//             dst.put_u16(payload_length as u16);
//         } else {
//             dst.reserve(payload_length + 10 + mask);
//             dst.extend_from_slice(&[one, second | 127]);
//             dst.put_u64(payload_length as u64);
//         };
//     }
// }
//
// fn mask_into(dst: &mut BytesMut) {
//     let mask: u32 = WyRand::new().generate();
//     dst.put_u32_le(mask);
// }
