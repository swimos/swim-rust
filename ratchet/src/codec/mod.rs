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

use crate::errors::{Error, ErrorKind};
use crate::handshake::ProtocolError;
use crate::protocol::frame::{
    CloseCode, CloseReason, ControlCode, DataCode, Frame, FrameHeader, Message, OpCode, Payload,
};
use crate::protocol::HeaderFlags;
use crate::Role;
use bytes::BytesMut;
use nanorand::{WyRand, RNG};
use std::borrow::BorrowMut;
use std::convert::TryFrom;
use tokio_util::codec::{Decoder, Encoder};

const CONTROL_FRAME_LEN: &str = "Control frame length greater than 125";

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

#[derive(Copy, Clone)]
pub enum DataType {
    Text,
    Binary,
}

#[derive(Clone)]
pub struct FragmentBuffer {
    buffer: BytesMut,
    op_code: Option<DataType>,
    max_size: usize,
}

impl FragmentBuffer {
    pub fn new(max_size: usize) -> Self {
        FragmentBuffer {
            buffer: BytesMut::default(),
            op_code: None,
            max_size,
        }
    }
}

impl FragmentBuffer {
    fn start_continuation(&mut self, first_code: DataType, payload: Vec<u8>) -> Result<(), Error> {
        let FragmentBuffer { op_code, .. } = self;
        match op_code {
            Some(_) => Err(ProtocolError::ContinuationAlreadyStarted.into()),
            None => {
                *op_code = Some(first_code);
                self.on_frame(payload)
            }
        }
    }

    fn on_frame(&mut self, payload: Vec<u8>) -> Result<(), Error> {
        let FragmentBuffer {
            buffer,
            op_code,
            max_size,
        } = self;

        match op_code {
            None => Err(ProtocolError::ContinuationNotStarted.into()),
            Some(_) => {
                if buffer.len() + payload.len() >= *max_size {
                    Err(ProtocolError::FrameOverflow.into())
                } else {
                    buffer.extend_from_slice(payload.as_slice());
                    Ok(())
                }
            }
        }
    }

    fn finish_continuation(&mut self) -> Result<(DataType, Vec<u8>), Error> {
        let FragmentBuffer {
            buffer,
            op_code,
            max_size,
        } = self;

        match op_code.take() {
            None => Err(ProtocolError::ContinuationNotStarted.into()),
            Some(data_type) => {
                let payload = buffer.split().freeze();
                buffer.truncate(*max_size / 2);
                Ok((data_type, payload.to_vec()))
            }
        }
    }
}

#[derive(Clone)]
pub struct Codec {
    flags: CodecFlags,
    max_size: usize,
    rand: WyRand,
    fragment_buffer: FragmentBuffer,
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
            fragment_buffer: FragmentBuffer::new(max_size),
        }
    }

    pub fn is_client(&self) -> bool {
        !self.flags.contains(CodecFlags::ROLE)
    }
}

impl Encoder<Message> for Codec {
    type Error = Error;

    fn encode(&mut self, message: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // let (opcode, bytes) = match message {
        //     Message::Text(text) => (OpCode::DataCode(DataCode::Text), text.into_bytes()),
        //     Message::Binary(data) => (OpCode::DataCode(DataCode::Binary), data),
        //     Message::Ping(data) => (OpCode::ControlCode(ControlCode::Ping), data),
        //     Message::Pong(data) => (OpCode::ControlCode(ControlCode::Pong), data),
        //     Message::Close(reason) => {
        //         let payload = match reason {
        //             Some(reason) => {
        //                 let CloseReason { code, description } = reason;
        //                 let mut payload = u16::from(code).to_be_bytes().to_vec();
        //                 if let Some(description) = description {
        //                     payload.extend_from_slice(description.as_bytes());
        //                 }
        //                 payload
        //             }
        //             None => Vec::new(),
        //         };
        //         (OpCode::ControlCode(ControlCode::Close), payload)
        //     }
        // };
        //
        // // todo run bytes through extensions
        //
        // let (flags, mask) = if self.is_client() {
        //     (HeaderFlags::FIN, Some(self.rand.generate()))
        // } else {
        //     (HeaderFlags::FIN, None)
        // };
        //
        // let header = FrameHeader::new(opcode, flags, mask);
        // Frame::new(header, bytes).write_into(dst);
        // Ok(())
        unimplemented!()
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Codec {
            flags,
            max_size,
            fragment_buffer,
            ..
        } = self;

        // match Frame::read_from(src, flags, *max_size)? {
        //     Some(frame) => {
        //         // println!("Read frame: {:?}", frame.header.flags);
        //
        //         let Frame { header, payload } = frame;
        //         let FrameHeader { opcode, flags, .. } = header;
        //
        //         match opcode {
        //             OpCode::DataCode(data_code) => {
        //                 on_data_frame(fragment_buffer, data_code, payload, flags)
        //             }
        //             OpCode::ControlCode(control_code) => on_control_frame(control_code, payload),
        //         }
        //     }
        //     None => {
        //         return Ok(None);
        //     }
        // }
        unimplemented!()
    }
}

fn on_data_frame(
    buffer: &mut FragmentBuffer,
    data_code: DataCode,
    mut payload: Payload,
    flags: HeaderFlags,
) -> Result<Option<Message>, Error> {
    // let payload: &mut [u8] = payload.borrow_mut();
    // let payload = payload.to_vec();
    //
    // match data_code {
    //     DataCode::Continuation => {
    //         buffer.on_frame(payload)?;
    //         if flags.is_fin() {
    //             // let (data_type, payload) = buffer.finish_continuation()?;
    //             // let message = match data_type {
    //             //     DataType::Text => Message::Text(payload)?,
    //             //     DataType::Binary => Message::Binary(payload),
    //             // };
    //             // Ok(Some(message))
    //             unimplemented!()
    //         } else {
    //             Ok(None)
    //         }
    //     }
    //     DataCode::Text => {
    //         if flags.is_fin() {
    //             unimplemented!()
    //         } else {
    //             buffer
    //                 .start_continuation(DataType::Text, payload)
    //                 .map(|_| None)
    //         }
    //     }
    //     DataCode::Binary => {
    //         if flags.is_fin() {
    //             Ok(Some(Message::Binary(payload)))
    //         } else {
    //             buffer
    //                 .start_continuation(DataType::Binary, payload)
    //                 .map(|_| None)
    //         }
    //     }
    // }
    unimplemented!()
}

fn on_control_frame(
    control_code: ControlCode,
    mut payload: Payload,
) -> Result<Option<Message>, Error> {
    // let payload: &mut [u8] = payload.borrow_mut();
    // let payload_len = payload.len();
    // if payload_len > 125 {
    //     return Err(Error::with_cause(ErrorKind::Protocol, CONTROL_FRAME_LEN));
    // }
    //
    // match control_code {
    //     ControlCode::Close => {
    //         if payload_len < 2 {
    //             Ok(Some(Message::Close(None)))
    //         } else {
    //             let close_reason = std::str::from_utf8(&payload[2..])?.to_string();
    //             let description = if close_reason.is_empty() {
    //                 None
    //             } else {
    //                 Some(close_reason)
    //             };
    //
    //             let code_no = u16::from_be_bytes([payload[0], payload[1]]);
    //             let close_code = CloseCode::try_from(code_no)?;
    //             let reason = CloseReason::new(close_code, description);
    //
    //             Ok(Some(Message::Close(Some(reason))))
    //         }
    //     }
    //     ControlCode::Ping => {
    //         let payload = payload.to_vec();
    //         Ok(Some(Message::Ping(payload)))
    //     }
    //     ControlCode::Pong => {
    //         let payload = payload.to_vec();
    //         Ok(Some(Message::Pong(payload)))
    //     }
    // }
    unimplemented!()
}
