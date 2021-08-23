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
use crate::protocol::frame::{
    CloseCode, CloseReason, ControlCode, DataCode, Frame, FrameHeader, OpCode, Payload,
};
use crate::protocol::HeaderFlags;
use crate::Role;
use bytes::Bytes;
use bytes::BytesMut;
use either::Either;
use nanorand::{WyRand, RNG};
use std::borrow::BorrowMut;
use std::convert::TryFrom;
use tokio_util::codec::{Decoder, Encoder};

const CONTROL_FRAME_LEN: &str = "Control frame length greater than 125";

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
    pub fn start_continuation(
        &mut self,
        first_code: DataType,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let FragmentBuffer { op_code, .. } = self;
        match op_code {
            Some(_) => Err(ProtocolError::ContinuationAlreadyStarted.into()),
            None => {
                *op_code = Some(first_code);
                self.on_frame(payload)
            }
        }
    }

    pub fn on_frame(&mut self, payload: Vec<u8>) -> Result<(), Error> {
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

    pub fn is_client(&self) -> bool {
        !self.flags.contains(CodecFlags::ROLE)
    }

    pub(crate) fn encode(&mut self, message: Item, dst: &mut BytesMut) -> Result<(), Error> {
        let (fin, opcode, bytes) = match message {
            Item::Text(text) => (true, OpCode::DataCode(DataCode::Text), Bytes::from(text)),
            Item::Binary(data) => (true, OpCode::DataCode(DataCode::Binary), data),
            Item::Ping(data) => (true, OpCode::ControlCode(ControlCode::Ping), data),
            Item::Pong(data) => (true, OpCode::ControlCode(ControlCode::Pong), data),
            Item::Close(reason) => {
                let payload = match reason {
                    Some(reason) => {
                        let CloseReason { code, description } = reason;
                        let mut payload = u16::from(code).to_be_bytes().to_vec();
                        if let Some(description) = description {
                            payload.extend_from_slice(description.as_bytes());
                        }
                        payload
                    }
                    None => Vec::new(),
                };
                (
                    true,
                    OpCode::ControlCode(ControlCode::Close),
                    Bytes::from(payload),
                )
            }
            Item::Continuation(Continuation::Continue(bytes)) => {
                (false, OpCode::DataCode(DataCode::Continuation), bytes)
            }
            Item::Continuation(Continuation::FirstText(bytes)) => {
                (false, OpCode::DataCode(DataCode::Text), bytes)
            }
            Item::Continuation(Continuation::FirstBinary(bytes)) => {
                (false, OpCode::DataCode(DataCode::Binary), bytes)
            }
            Item::Continuation(Continuation::Last(bytes)) => {
                (true, OpCode::DataCode(DataCode::Continuation), bytes)
            }
        };

        // todo run bytes through extensions

        let flags = if fin {
            HeaderFlags::FIN
        } else {
            HeaderFlags::empty()
        };
        let mask = if self.is_client() {
            Some(self.rand.generate())
        } else {
            None
        };

        let header = FrameHeader::new(opcode, flags, mask);
        Frame::new(header, bytes).write_into(dst);
        Ok(())
    }

    pub(crate) fn decode(&mut self, src: &mut BytesMut) -> Result<Either<Item, usize>, Error> {
        let Codec {
            flags, max_size, ..
        } = self;

        match Frame::read_from(src, flags, *max_size)? {
            Either::Left(frame) => {
                // println!("Read frame: {:?}", frame);

                let Frame { header, payload } = frame;
                let FrameHeader { opcode, flags, .. } = header;

                match opcode {
                    OpCode::DataCode(data_code) => {
                        on_data_frame(data_code, payload, flags).map(Either::Left)
                    }
                    OpCode::ControlCode(control_code) => {
                        on_control_frame(control_code, payload).map(Either::Left)
                    }
                }
            }
            Either::Right(count) => Ok(Either::Right(count)),
        }
    }

    pub fn decode_frame(&mut self, frame: Frame) -> Result<Item, Error> {
        let Frame { header, payload } = frame;
        let FrameHeader { opcode, flags, .. } = header;

        match opcode {
            OpCode::DataCode(data_code) => on_data_frame(data_code, payload, flags),
            OpCode::ControlCode(control_code) => on_control_frame(control_code, payload),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Item {
    Text(Bytes),
    Continuation(Continuation),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<CloseReason>),
}

#[cold]
#[doc(hidden)]
#[inline(never)]
fn expect_type_failed(fn_name: &str, on: &str) -> ! {
    panic!("Called `{}` on a `{}` message", fn_name, on)
}

#[derive(Debug, PartialEq)]
pub enum Continuation {
    FirstText(Bytes),
    FirstBinary(Bytes),
    Continue(Bytes),
    Last(Bytes),
}

fn on_data_frame(
    data_code: DataCode,
    mut payload: Bytes,
    flags: HeaderFlags,
) -> Result<Item, Error> {
    match data_code {
        DataCode::Continuation => {
            if flags.is_fin() {
                Ok(Item::Continuation(Continuation::Last(payload)))
            } else {
                Ok(Item::Continuation(Continuation::Continue(payload)))
            }
        }
        DataCode::Text => {
            if flags.is_fin() {
                Ok(Item::Text(payload))
            } else {
                Ok(Item::Continuation(Continuation::FirstText(payload)))
            }
        }
        DataCode::Binary => {
            if flags.is_fin() {
                Ok(Item::Binary(payload))
            } else {
                Ok(Item::Continuation(Continuation::FirstBinary(payload)))
            }
        }
    }
}

fn on_control_frame(control_code: ControlCode, mut payload: Bytes) -> Result<Item, Error> {
    let payload_len = payload.len();
    if payload_len > 125 {
        return Err(Error::with_cause(ErrorKind::Protocol, CONTROL_FRAME_LEN));
    }

    match control_code {
        ControlCode::Close => {
            if payload_len < 2 {
                Ok(Item::Close(None))
            } else {
                let close_reason = std::str::from_utf8(&payload[2..])?.to_string();
                let description = if close_reason.is_empty() {
                    None
                } else {
                    Some(close_reason)
                };

                let code_no = u16::from_be_bytes([payload[0], payload[1]]);
                let close_code = CloseCode::try_from(code_no)?;
                let reason = CloseReason::new(close_code, description);

                Ok(Item::Close(Some(reason)))
            }
        }
        ControlCode::Ping => Ok(Item::Ping(payload)),
        ControlCode::Pong => Ok(Item::Pong(payload)),
    }
}
