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

use crate::compat::{
    AgentMessage, AgentMessageDecodeError, AgentMessageDecoder, RawAgentMessage,
    RawAgentMessageEncoder, COMMAND, HEADER_INIT_LEN, LINK, OP_MASK, OP_SHIFT, SYNC, UNLINK,
};
use bytes::{Buf, BytesMut};
use futures::future::join;
use futures::{SinkExt, StreamExt};
use std::fmt::Debug;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_recon::printer::print_recon_compact;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::byte_channel;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

#[test]
fn encode_link_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";
    let path = RelativePath::new(node, lane);
    let frame = RawAgentMessage::link(id, path);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, LINK);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_sync_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";
    let path = RelativePath::new(node, lane);
    let frame = RawAgentMessage::sync(id, path);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, SYNC);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_unlink_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";
    let path = RelativePath::new(node, lane);
    let frame = RawAgentMessage::unlink(id, path);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, UNLINK);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_command_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";
    let path = RelativePath::new(node, lane);
    let body = "@Example { first: 1, second: 2 }";
    let frame = RawAgentMessage::command(id, path, body.as_bytes());

    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(
        buffer.len(),
        HEADER_INIT_LEN + node.len() + lane.len() + body.len()
    );

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, COMMAND);

    let body_len = (body_descriptor & !OP_MASK) as usize;
    assert_eq!(body_len, body.len());

    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(&buffer.as_ref()[0..lane.len()], lane.as_bytes());
    buffer.advance(lane.len());

    assert_eq!(buffer.as_ref(), body.as_bytes());
}

#[derive(Form, PartialEq, Eq, Debug, Clone, Copy)]
struct Example {
    first: i32,
    second: i32,
}

fn check_result<T: Eq + Debug>(
    result: Result<Option<AgentMessage<T>>, AgentMessageDecodeError>,
    expected: AgentMessage<T>,
) {
    match result {
        Ok(Some(msg)) => assert_eq!(msg, expected),
        Ok(_) => panic!("Incomplete."),
        Err(e) => panic!("Failed: {}", e),
    }
}

fn round_trip<T>(
    frame: AgentMessage<&[u8]>,
) -> Result<Option<AgentMessage<T>>, AgentMessageDecodeError>
where
    T: RecognizerReadable,
{
    let mut decoder = AgentMessageDecoder::<T, _>::new(T::make_recognizer());
    let mut encoder = RawAgentMessageEncoder;

    let mut buffer = BytesMut::new();
    assert!(encoder.encode(frame, &mut buffer).is_ok());

    let result = decoder.decode(&mut buffer);

    if result.is_ok() {
        assert!(buffer.is_empty());
    }

    result
}

#[test]
fn decode_link_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";

    let frame = RawAgentMessage::link(id, RelativePath::new(node, lane));
    let result = round_trip::<Example>(frame);

    check_result(
        result,
        AgentMessage::link(id, RelativePath::new(node, lane)),
    );
}

#[test]
fn decode_sync_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";

    let frame = RawAgentMessage::sync(id, RelativePath::new(node, lane));
    let result = round_trip::<Example>(frame);

    check_result(
        result,
        AgentMessage::sync(id, RelativePath::new(node, lane)),
    );
}

#[test]
fn decode_unlink_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";

    let frame = RawAgentMessage::unlink(id, RelativePath::new(node, lane));
    let result = round_trip::<Example>(frame);

    check_result(
        result,
        AgentMessage::unlink(id, RelativePath::new(node, lane)),
    );
}

#[test]
fn decode_command_frame() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";

    let record = Example {
        first: 1,
        second: 2,
    };
    let as_text = print_recon_compact(&record).to_string();

    let frame = RawAgentMessage::command(id, RelativePath::new(node, lane), as_text.as_bytes());

    let result = round_trip::<Example>(frame);

    check_result(
        result,
        AgentMessage::command(id, RelativePath::new(node, lane), record),
    );
}

const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(16);

#[tokio::test]
async fn multiple_frames() {
    let id = Uuid::new_v4();
    let node = "my_node";
    let lane = "lane";

    let (tx, rx) = byte_channel::byte_channel(CHANNEL_SIZE);

    let mut framed_write = FramedWrite::new(tx, RawAgentMessageEncoder);
    let decoder = AgentMessageDecoder::<Example, _>::new(Example::make_recognizer());

    let mut framed_read = FramedRead::new(rx, decoder);

    let record1 = Example {
        first: 1,
        second: 2,
    };

    let record2 = Example {
        first: 3,
        second: 4,
    };

    let str1 = print_recon_compact(&record1).to_string();
    let str2 = print_recon_compact(&record2).to_string();

    let frames = vec![
        RawAgentMessage::sync(id, RelativePath::new(node, lane)),
        RawAgentMessage::command(id, RelativePath::new(node, lane), str1.as_bytes()),
        RawAgentMessage::command(id, RelativePath::new(node, lane), str2.as_bytes()),
        RawAgentMessage::unlink(id, RelativePath::new(node, lane)),
    ];

    let send_task = async move {
        for frame in frames {
            if let Err(e) = framed_write.send(frame).await {
                if e.kind() != ErrorKind::BrokenPipe {
                    panic!("{}", e);
                } else {
                    break;
                }
            }
        }
    };

    let recv_task = async move {
        let mut received = vec![];
        while let Some(result) = framed_read.next().await {
            match result {
                Ok(message) => received.push(message),
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(received)
    };

    let (_, result) = join(send_task, recv_task).await;

    let expected = vec![
        AgentMessage::sync(id, RelativePath::new(node, lane)),
        AgentMessage::command(id, RelativePath::new(node, lane), record1),
        AgentMessage::command(id, RelativePath::new(node, lane), record2),
        AgentMessage::unlink(id, RelativePath::new(node, lane)),
    ];

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected);
}
