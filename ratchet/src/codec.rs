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

#[derive(Clone)]
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

impl FrameBuffer for FragmentBuffer {
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

    fn finish_continuation(&mut self) -> Result<Message, Error> {
        let FragmentBuffer {
            buffer,
            op_code,
            max_size,
        } = self;

        match op_code {
            None => Err(ProtocolError::ContinuationNotStarted.into()),
            Some(op_code) => {
                let payload = buffer.split().freeze();
                buffer.truncate(*max_size / 2);

                match op_code {
                    DataType::Text => Message::text_from_utf8(payload.to_vec()),
                    DataType::Binary => Ok(Message::Binary(payload.to_vec())),
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Codec<B> {
    flags: CodecFlags,
    max_size: usize,
    rand: WyRand,
    fragment_buffer: B,
}

impl<B> Codec<B>
where
    B: FrameBuffer,
{
    pub fn new(role: Role, max_size: usize, fragment_buffer: B) -> Self {
        let role_flag = match role {
            Role::Client => CodecFlags::empty(),
            Role::Server => CodecFlags::ROLE,
        };
        let flags = CodecFlags::from(role_flag);

        Codec {
            flags,
            max_size,
            rand: WyRand::new(),
            fragment_buffer,
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

impl<B> Encoder<Message> for Codec<B>
where
    B: FrameBuffer,
{
    type Error = Error;

    fn encode(&mut self, message: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (opcode, bytes) = match message {
            Message::Text(text) => (OpCode::DataCode(DataCode::Text), text.into_bytes()),
            Message::Binary(data) => (OpCode::DataCode(DataCode::Binary), data),
            Message::Ping(data) => (OpCode::ControlCode(ControlCode::Ping), data),
            Message::Pong(data) => (OpCode::ControlCode(ControlCode::Pong), data),
            Message::Close(_reason) => {
                unimplemented!()
            }
        };

        // todo run bytes through extensions

        let (flags, mask) = if self.is_client() {
            (HeaderFlags::FIN, Some(self.rand.generate()))
        } else {
            (HeaderFlags::FIN, None)
        };

        let header = FrameHeader::new(opcode, flags, mask);
        Frame::new(header, bytes).write_into(dst);

        Ok(())
    }
}

impl<B> Decoder for Codec<B>
where
    B: FrameBuffer,
{
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Codec {
            flags,
            max_size,
            fragment_buffer,
            ..
        } = self;

        match Frame::read_from(src, flags, *max_size)? {
            Some(frame) => {
                let Frame { header, payload } = frame;
                let FrameHeader { opcode, flags, .. } = header;

                match opcode {
                    OpCode::DataCode(data_code) => {
                        println!("{:?}", data_code);
                        on_data_frame(fragment_buffer, data_code, payload, flags)
                    }
                    OpCode::ControlCode(control_code) => on_control_frame(control_code, payload),
                }
            }
            None => return Ok(None),
        }
    }
}

pub trait FrameBuffer {
    fn start_continuation(&mut self, op_code: DataType, payload: Vec<u8>) -> Result<(), Error>;

    fn on_frame(&mut self, payload: Vec<u8>) -> Result<(), Error>;

    fn finish_continuation(&mut self) -> Result<Message, Error>;
}

fn on_data_frame<B>(
    buffer: &mut B,
    data_code: DataCode,
    mut payload: Payload,
    flags: HeaderFlags,
) -> Result<Option<Message>, Error>
where
    B: FrameBuffer,
{
    let payload: &mut [u8] = payload.borrow_mut();
    let payload = payload.to_vec();

    match data_code {
        DataCode::Continuation => {
            buffer.on_frame(payload)?;
            if flags.is_fin() {
                buffer.finish_continuation().map(Some)
            } else {
                Ok(None)
            }
        }
        DataCode::Text => {
            if flags.is_fin() {
                Message::text_from_utf8(payload).map(Some)
            } else {
                buffer
                    .start_continuation(DataType::Text, payload)
                    .map(|_| None)
            }
        }
        DataCode::Binary => {
            if flags.is_fin() {
                Ok(Some(Message::Binary(payload)))
            } else {
                buffer
                    .start_continuation(DataType::Binary, payload)
                    .map(|_| None)
            }
        }
    }
}

fn on_control_frame(
    control_code: ControlCode,
    mut payload: Payload,
) -> Result<Option<Message>, Error> {
    let payload: &mut [u8] = payload.borrow_mut();
    let payload_len = payload.len();
    if payload_len > 125 {
        return Err(Error::with_cause(ErrorKind::Protocol, CONTROL_FRAME_LEN));
    }

    match control_code {
        ControlCode::Close => {
            if payload_len < 2 {
                Ok(Some(Message::Close(None)))
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

                Ok(Some(Message::Close(Some(reason))))
            }
        }
        ControlCode::Ping => {
            let payload = payload.to_vec();
            Ok(Some(Message::Ping(payload)))
        }
        ControlCode::Pong => {
            let payload = payload.to_vec();
            Ok(Some(Message::Pong(payload)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::expect_err;
    use std::error::Error as StdError;
    use std::fmt::Debug;
    use std::iter::FromIterator;

    #[test]
    fn frame_text() {
        let mut bytes = BytesMut::from_iter(&[
            129, 143, 0, 0, 0, 0, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111,
            116,
        ]);

        let mut codec = Codec::new(Role::Server, usize::MAX, FragmentBuffer::new(usize::MAX));
        let result = codec.decode(&mut bytes);
        assert_eq!(
            result.unwrap(),
            Some(Message::Text("Bonsoir, Elliot".to_string()))
        )
    }

    #[test]
    fn continuation_text() {
        let mut buffer = BytesMut::new();
        let mut codec = Codec::new(Role::Server, usize::MAX, FragmentBuffer::new(usize::MAX));

        let input = "a bunch of characters that form a string";
        let mut iter = input.as_bytes().chunks(5).peekable();
        let first_frame = Frame::new(
            FrameHeader::new(
                OpCode::DataCode(DataCode::Text),
                HeaderFlags::empty(),
                Some(0),
            ),
            iter.next().unwrap().to_vec(),
        );

        first_frame.write_into(&mut buffer);

        while let Some(data) = iter.next() {
            let fin = iter.peek().is_none();
            let flags =
                HeaderFlags::from_bits_truncate(HeaderFlags::FIN.bits() & ((fin as u8) << 7));
            let frame = Frame::new(
                FrameHeader::new(OpCode::DataCode(DataCode::Continuation), flags, Some(0)),
                data.to_vec(),
            );

            frame.write_into(&mut buffer);
        }

        loop {
            match codec.decode(&mut buffer) {
                Ok(Some(message)) => {
                    assert_eq!(message, Message::Text(input.to_string()));
                    break;
                }
                Ok(None) => continue,
                Err(e) => panic!("{:?}", e),
            }
        }
    }

    #[test]
    fn double_cont() {
        let mut buffer = FragmentBuffer::new(usize::MAX);
        assert!(buffer.start_continuation(DataType::Binary, vec![1]).is_ok());

        expect_err(
            buffer.start_continuation(DataType::Binary, vec![1]),
            ProtocolError::ContinuationAlreadyStarted,
        )
    }

    #[test]
    fn no_cont() {
        let mut buffer = FragmentBuffer::new(usize::MAX);
        expect_err(
            buffer.on_frame(vec![]),
            ProtocolError::ContinuationNotStarted,
        );
    }

    #[test]
    fn overflow_buffer() {
        let mut buffer = FragmentBuffer::new(5);

        assert!(buffer
            .start_continuation(DataType::Binary, vec![1, 2, 3])
            .is_ok());
        assert!(buffer.on_frame(vec![4]).is_ok());
        expect_err(buffer.on_frame(vec![6, 7]), ProtocolError::FrameOverflow);
    }

    #[test]
    fn invalid_utf8() {
        let mut buffer = FragmentBuffer::new(5);

        assert!(buffer
            .start_continuation(DataType::Text, vec![0, 159, 146, 150])
            .is_ok());
        let error = buffer.finish_continuation().err().unwrap();
        assert!(error.is_encoding());
    }

    fn ok_eq<O>(result: Result<O, Error>, eq: O)
    where
        O: PartialEq + Debug,
    {
        match result {
            Ok(actual) => assert_eq!(actual, eq),
            Err(e) => panic!("Expected: `{:?}`, got: `{:?}`", eq, e),
        }
    }

    #[test]
    fn ping() {
        let mut buffer = BytesMut::from_iter(&[137, 4, 1, 2, 3, 4]);
        let mut codec = Codec::new(Role::Client, usize::MAX, FragmentBuffer::new(usize::MAX));

        ok_eq(
            codec.decode(&mut buffer),
            Some(Message::Ping(vec![1, 2, 3, 4])),
        );
    }

    #[test]
    fn pong() {
        let mut buffer = BytesMut::from_iter(&[138, 4, 1, 2, 3, 4]);
        let mut codec = Codec::new(Role::Client, usize::MAX, FragmentBuffer::new(usize::MAX));

        ok_eq(
            codec.decode(&mut buffer),
            Some(Message::Pong(vec![1, 2, 3, 4])),
        );
    }

    #[test]
    fn close() {
        let mut codec = Codec::new(Role::Client, usize::MAX, FragmentBuffer::new(usize::MAX));

        ok_eq(
            codec.decode(&mut BytesMut::from_iter(&[136, 0])),
            Some(Message::Close(None)),
        );
        ok_eq(
            codec.decode(&mut BytesMut::from_iter(&[136, 2, 3, 232])),
            Some(Message::Close(Some(CloseReason::new(
                CloseCode::Normal,
                None,
            )))),
        );
        ok_eq(
            codec.decode(&mut BytesMut::from_iter(&[
                136, 17, 3, 240, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111,
                116,
            ])),
            Some(Message::Close(Some(CloseReason::new(
                CloseCode::Policy,
                Some("Bonsoir, Elliot".to_string()),
            )))),
        );

        let mut frame = vec![136, 126, 1, 0];
        frame.extend_from_slice(&[0; 256]);
        let decode_result = codec.decode(&mut BytesMut::from_iter(frame));
        let error = decode_result.unwrap_err();
        assert_eq!(error.source().unwrap().to_string(), CONTROL_FRAME_LEN);
    }
}
