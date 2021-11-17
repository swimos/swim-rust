// Copyright 2015-2021 Swim Inc.
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

use crate::errors::{Error, ProtocolError};
use crate::ext::NoExt;
use crate::framed::{CodecFlags, FramedIo, Item};
use crate::protocol::{CloseCode, CloseCodeParseErr, CloseReason, DataCode, OpCode};
use crate::protocol::{HeaderFlags, Role};
use crate::test_fixture::{expect_err, EmptyIo, MirroredIo};
use bytes::BytesMut;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::iter::FromIterator;

#[tokio::test]
async fn frame_text() {
    let bytes = BytesMut::from_iter(&[
        129, 143, 0, 0, 0, 0, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111, 116,
    ]);

    let mut out = BytesMut::new();

    let mut framed = FramedIo::new(EmptyIo, bytes, Role::Server, usize::MAX, 0);
    let item = framed.read_next(&mut out, &mut NoExt).await.unwrap();

    assert_eq!(item, Item::Text);
    assert_eq!(
        std::str::from_utf8(out.as_ref()).unwrap(),
        "Bonsoir, Elliot"
    );
}

#[tokio::test]
async fn continuation() {
    let mut input = "a bunch of characters that form a string".to_string();
    let mut iter = unsafe { input.as_bytes_mut() }.chunks_mut(5).peekable();
    let mut framed = FramedIo::new(
        MirroredIo::default(),
        BytesMut::default(),
        Role::Server,
        usize::MAX,
        0,
    );

    framed
        .write(
            OpCode::DataCode(DataCode::Text),
            HeaderFlags::empty(),
            iter.next().unwrap().to_vec(),
            |_, _| Ok(()),
        )
        .await
        .unwrap();

    while let Some(data) = iter.next() {
        let fin = iter.peek().is_none();
        let flags = HeaderFlags::from_bits_truncate((fin as u8) << 7);

        framed
            .write(
                OpCode::DataCode(DataCode::Continuation),
                flags,
                data,
                |_, _| Ok(()),
            )
            .await
            .unwrap();
    }

    framed.flags.set(CodecFlags::ROLE, false);

    let mut rx_buf = BytesMut::default();
    let message_type = framed.read_next(&mut rx_buf, &mut NoExt).await.unwrap();
    match message_type {
        Item::Text => {
            let received = std::str::from_utf8(rx_buf.as_ref()).unwrap();
            assert_eq!(input, received);
        }
        i => panic!("Unexpected message type: {:?}", i),
    }
}

#[tokio::test]
async fn double_cont() {
    let mut framed = FramedIo::new(
        MirroredIo::default(),
        BytesMut::default(),
        Role::Server,
        usize::MAX,
        0,
    );

    framed
        .write(
            OpCode::DataCode(DataCode::Text),
            HeaderFlags::empty(),
            unsafe { "hello".to_string().as_bytes_mut() },
            |_, _| Ok(()),
        )
        .await
        .unwrap();

    framed
        .write(
            OpCode::DataCode(DataCode::Text),
            HeaderFlags::empty(),
            unsafe { "hello again".to_string().as_bytes_mut() },
            |_, _| Ok(()),
        )
        .await
        .unwrap();

    framed.flags.set(CodecFlags::ROLE, false);

    let mut rx_buf = BytesMut::default();
    expect_err(
        framed.read_next(&mut rx_buf, &mut NoExt).await,
        ProtocolError::ContinuationAlreadyStarted,
    );
}

#[tokio::test]
async fn no_cont() {
    let mut framed = FramedIo::new(
        MirroredIo::default(),
        BytesMut::default(),
        Role::Server,
        usize::MAX,
        0,
    );

    framed
        .write(
            OpCode::DataCode(DataCode::Continuation),
            HeaderFlags::empty(),
            unsafe { "hello".to_string().as_bytes_mut() },
            |_, _| Ok(()),
        )
        .await
        .unwrap();

    framed.flags.set(CodecFlags::ROLE, false);

    let mut rx_buf = BytesMut::default();
    expect_err(
        framed.read_next(&mut rx_buf, &mut NoExt).await,
        ProtocolError::ContinuationNotStarted,
    );
}

#[tokio::test]
async fn overflow_buffer() {
    let mut framed = FramedIo::new(
        MirroredIo::default(),
        BytesMut::default(),
        Role::Server,
        7,
        0,
    );

    framed
        .write(
            OpCode::DataCode(DataCode::Text),
            HeaderFlags::empty(),
            unsafe { "Houston, we have a problem.".to_string().as_bytes_mut() },
            |_, _| Ok(()),
        )
        .await
        .unwrap();

    framed.flags.set(CodecFlags::ROLE, false);

    let mut rx_buf = BytesMut::default();
    expect_err(
        framed.read_next(&mut rx_buf, &mut NoExt).await,
        ProtocolError::FrameOverflow,
    );
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

#[tokio::test]
async fn ping() {
    let buffer = BytesMut::from_iter(&[137, 4, 1, 2, 3, 4]);
    let mut framed = FramedIo::new(EmptyIo, buffer, Role::Client, usize::MAX, 0);

    ok_eq(
        framed.read_next(&mut BytesMut::default(), &mut NoExt).await,
        Item::Ping(BytesMut::from_iter(vec![1, 2, 3, 4])),
    );
}

#[tokio::test]
async fn pong() {
    let buffer = BytesMut::from_iter(&[138, 4, 1, 2, 3, 4]);
    let mut framed = FramedIo::new(EmptyIo, buffer, Role::Client, usize::MAX, 0);

    ok_eq(
        framed.read_next(&mut BytesMut::default(), &mut NoExt).await,
        Item::Pong(BytesMut::from_iter(vec![1, 2, 3, 4])),
    );
}

#[tokio::test]
async fn close() {
    async fn test(frame: Vec<u8>, eq: Option<CloseReason>) {
        let buffer = BytesMut::from_iter(frame);
        let mut framed = FramedIo::new(EmptyIo, buffer, Role::Client, usize::MAX, 0);

        ok_eq(
            framed.read_next(&mut BytesMut::default(), &mut NoExt).await,
            Item::Close(eq),
        );
    }

    test(vec![136, 0], None).await;
    test(
        vec![136, 2, 3, 232],
        Some(CloseReason::new(CloseCode::Normal, None)),
    )
    .await;
    test(
        vec![
            136, 17, 3, 240, 66, 111, 110, 115, 111, 105, 114, 44, 32, 69, 108, 108, 105, 111, 116,
        ],
        Some(CloseReason::new(
            CloseCode::Policy,
            Some("Bonsoir, Elliot".to_string()),
        )),
    )
    .await;

    let mut frame = vec![136, 126, 1, 0];
    frame.extend_from_slice(&[0; 256]);

    let buffer = BytesMut::from_iter(frame);
    let mut framed = FramedIo::new(EmptyIo, buffer, Role::Client, usize::MAX, 0);

    let decode_result = framed.read_next(&mut BytesMut::default(), &mut NoExt).await;
    let error = decode_result.unwrap_err();
    assert_eq!(
        error.source().unwrap().to_string(),
        CloseCodeParseErr(0).to_string()
    );
}
