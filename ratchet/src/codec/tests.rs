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

use crate::codec::{Codec, DataType, FragmentBuffer, FrameBuffer, CONTROL_FRAME_LEN};
use crate::errors::Error;
use crate::fixture::expect_err;
use crate::handshake::ProtocolError;
use crate::protocol::frame::{
    CloseCode, CloseReason, DataCode, Frame, FrameHeader, Message, OpCode,
};
use crate::protocol::HeaderFlags;
use crate::Role;
use bytes::BytesMut;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::iter::FromIterator;
use tokio_util::codec::Decoder;

#[test]
fn frame_text() {
    let mut bytes = BytesMut::from_iter(&[
        129, 143, 0, 0, 0, 0, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111, 116,
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
        let flags = HeaderFlags::from_bits_truncate(HeaderFlags::FIN.bits() & ((fin as u8) << 7));
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
            136, 17, 3, 240, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111, 116,
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
