// Copyright 2015-2023 Swim Inc.
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

use crate::protocol::{
    AgentMessageDecoder, BytesResponseMessage, MessageDecodeError, RawRequestMessage,
    RawRequestMessageEncoder, RawResponseMessageDecoder, RequestMessage, ResponseMessage,
    ResponseMessageEncoder, COMMAND, EVENT, HEADER_INIT_LEN, LINK, LINKED, OP_MASK, OP_SHIFT, SYNC,
    SYNCED, UNLINK, UNLINKED,
};
use bytes::{Buf, Bytes, BytesMut};
use futures::future::join;
use futures::{SinkExt, StreamExt};
use std::fmt::Debug;
use std::fmt::Write;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use swimos_api::address::RelativeAddress;
use swimos_form::read::RecognizerReadable;
use swimos_form::write::StructuralWritable;
use swimos_form::Form;
use swimos_model::Text;
use swimos_recon::print_recon_compact;
use swimos_utilities::byte_channel;
use swimos_utilities::encoding::BytesStr;
use swimos_utilities::non_zero_usize;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use uuid::Uuid;

fn make_addr() -> Uuid {
    let id: u32 = rand::random();
    let mut uuid_as_int = id as u128;
    uuid_as_int |= 1_u128 << 120;
    Uuid::from_u128(uuid_as_int)
}

#[test]
fn encode_link_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = RawRequestMessage::link(id, path);
    let mut encoder = RawRequestMessageEncoder;
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
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = RawRequestMessage::sync(id, path);
    let mut encoder = RawRequestMessageEncoder;
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
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = RawRequestMessage::unlink(id, path);
    let mut encoder = RawRequestMessageEncoder;
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
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let body = "@Example { first: 1, second: 2 }";
    let frame = RawRequestMessage::command(id, path, body.as_bytes());

    let mut encoder = RawRequestMessageEncoder;
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

fn check_result<P: Eq + Debug, T: Eq + Debug>(
    result: Result<Option<RequestMessage<P, T>>, MessageDecodeError>,
    expected: RequestMessage<P, T>,
) {
    match result {
        Ok(Some(msg)) => assert_eq!(msg, expected),
        Ok(_) => panic!("Incomplete."),
        Err(e) => panic!("Failed: {}", e),
    }
}

fn check_result_rawresponse(
    result: Result<Option<BytesResponseMessage>, std::io::Error>,
    expected: BytesResponseMessage,
) {
    match result {
        Ok(Some(msg)) => assert_eq!(msg, expected),
        Ok(_) => panic!("Incomplete."),
        Err(e) => panic!("Failed: {}", e),
    }
}

fn round_trip<P, T>(
    frame: RequestMessage<P, &[u8]>,
) -> Result<Option<RequestMessage<Text, T>>, MessageDecodeError>
where
    P: AsRef<str>,
    T: RecognizerReadable + Debug,
{
    let mut decoder = AgentMessageDecoder::<T, _>::new(T::make_recognizer());
    let mut encoder = RawRequestMessageEncoder;

    let mut buffer = BytesMut::new();
    assert!(encoder.encode(frame, &mut buffer).is_ok());

    let result = decoder.decode(&mut buffer);

    if result.is_ok() {
        assert!(buffer.is_empty());
    }

    result
}

fn round_trip_rawresponse<P, T>(
    frame: ResponseMessage<P, T, Bytes>,
) -> Result<Option<BytesResponseMessage>, std::io::Error>
where
    P: AsRef<str>,
    T: StructuralWritable,
{
    let mut decoder = RawResponseMessageDecoder;
    let mut encoder = ResponseMessageEncoder;

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
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame = RawRequestMessage::link(id, RelativeAddress::new(node, lane));
    let result = round_trip::<&str, Example>(frame);

    check_result(
        result,
        RequestMessage::link(id, RelativeAddress::new(Text::new(node), Text::new(lane))),
    );
}

#[test]
fn decode_sync_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame = RawRequestMessage::sync(id, RelativeAddress::new(node, lane));
    let result = round_trip::<_, Example>(frame);

    check_result(
        result,
        RequestMessage::sync(id, RelativeAddress::new(Text::new(node), Text::new(lane))),
    );
}

#[test]
fn decode_unlink_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame = RawRequestMessage::unlink(id, RelativeAddress::new(node, lane));
    let result = round_trip::<_, Example>(frame);

    check_result(
        result,
        RequestMessage::unlink(id, RelativeAddress::new(Text::new(node), Text::new(lane))),
    );
}

#[test]
fn decode_command_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let record = Example {
        first: 1,
        second: 2,
    };
    let as_text = print_recon_compact(&record).to_string();

    let frame =
        RawRequestMessage::command(id, RelativeAddress::new(node, lane), as_text.as_bytes());

    let result = round_trip::<_, Example>(frame);

    check_result(
        result,
        RequestMessage::command(
            id,
            RelativeAddress::new(Text::new(node), Text::new(lane)),
            record,
        ),
    );
}

const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(16);

#[tokio::test]
async fn multiple_frames() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let (tx, rx) = byte_channel::byte_channel(CHANNEL_SIZE);

    let mut framed_write = FramedWrite::new(tx, RawRequestMessageEncoder);
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
        RawRequestMessage::sync(id, RelativeAddress::new(node, lane)),
        RawRequestMessage::command(id, RelativeAddress::new(node, lane), str1.as_bytes()),
        RawRequestMessage::command(id, RelativeAddress::new(node, lane), str2.as_bytes()),
        RawRequestMessage::unlink(id, RelativeAddress::new(node, lane)),
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
        RequestMessage::sync(id, RelativeAddress::text(node, lane)),
        RequestMessage::command(id, RelativeAddress::text(node, lane), record1),
        RequestMessage::command(id, RelativeAddress::text(node, lane), record2),
        RequestMessage::unlink(id, RelativeAddress::text(node, lane)),
    ];

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn encode_linked_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = ResponseMessage::<_, Example, Bytes>::linked(id, path);
    let mut encoder = ResponseMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, LINKED);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_synced_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = ResponseMessage::<_, Example, Bytes>::synced(id, path);
    let mut encoder = ResponseMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, SYNCED);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_unlinked_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let frame = ResponseMessage::<_, Example, Bytes>::unlinked(id, path, None);
    let mut encoder = ResponseMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + node.len() + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, UNLINKED);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_unlinked_frame_with_body() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);

    let body = "Gone";

    let frame = ResponseMessage::<_, Example, _>::unlinked(id, path, Some(body.as_bytes()));
    let mut encoder = ResponseMessageEncoder;
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
    assert_eq!(tag, UNLINKED);

    let body_len = (body_descriptor & !OP_MASK) as usize;
    assert_eq!(body_len, body.len());

    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(&buffer.as_ref()[0..lane.len()], lane.as_bytes());
    buffer.advance(lane.len());

    assert_eq!(buffer.as_ref(), body.as_bytes());
}

#[test]
fn encode_event_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let path = RelativeAddress::new(node, lane);
    let body = Example {
        first: 0,
        second: 0,
    };
    let expected_body = format!("{}", print_recon_compact(&body));

    let frame = ResponseMessage::<_, Example, Bytes>::event(id, path, body);

    let mut encoder = ResponseMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(
        buffer.len(),
        HEADER_INIT_LEN + node.len() + lane.len() + expected_body.len()
    );

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), node.len() as u32);
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, EVENT);

    let body_len = (body_descriptor & !OP_MASK) as usize;
    assert_eq!(body_len, expected_body.len());

    assert_eq!(&buffer.as_ref()[0..node.len()], node.as_bytes());
    buffer.advance(node.len());
    assert_eq!(&buffer.as_ref()[0..lane.len()], lane.as_bytes());
    buffer.advance(lane.len());

    assert_eq!(buffer.as_ref(), expected_body.as_bytes());
}

fn bytes_path(node: &str, lane: &str) -> RelativeAddress<BytesStr> {
    RelativeAddress::new(BytesStr::from(node), BytesStr::from(lane))
}

#[test]
fn decode_linked_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame = ResponseMessage::<_, Example, Bytes>::linked(id, bytes_path(node, lane));
    let result = round_trip_rawresponse::<_, Example>(frame);

    check_result_rawresponse(
        result,
        BytesResponseMessage::linked(id, bytes_path(node, lane)),
    );
}

#[test]
fn decode_synced_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame = ResponseMessage::<_, Example, Bytes>::synced(id, RelativeAddress::new(node, lane));
    let result = round_trip_rawresponse::<_, Example>(frame);

    check_result_rawresponse(
        result,
        BytesResponseMessage::synced(id, bytes_path(node, lane)),
    );
}

#[test]
fn decode_unlinked_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let frame =
        ResponseMessage::<_, Example, Bytes>::unlinked(id, RelativeAddress::new(node, lane), None);
    let result = round_trip_rawresponse::<_, Example>(frame);

    check_result_rawresponse(
        result,
        BytesResponseMessage::unlinked(id, bytes_path(node, lane), None),
    );
}

#[test]
fn decode_event_frame() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";

    let message = Example {
        first: 1,
        second: 2,
    };
    let mut expected_body = BytesMut::new();
    expected_body.reserve(1024);
    write!(expected_body, "{}", print_recon_compact(&message)).expect("Serialization failed.");

    let frame = ResponseMessage::<_, Example, Bytes>::event(
        id,
        RelativeAddress::new(node, lane),
        Example {
            first: 1,
            second: 2,
        },
    );
    let result = round_trip_rawresponse::<_, Example>(frame);

    check_result_rawresponse(
        result,
        BytesResponseMessage::event(id, bytes_path(node, lane), expected_body.freeze()),
    );
}

#[derive(Form, PartialEq, Eq, Debug)]

enum Example2 {
    First(Text),
    Second,
}

#[test]
fn decode_command_frame_twice() {
    let id = make_addr();
    let node = "my_node";
    let lane = "lane";
    let body = "body";

    let first = Example2::First(Text::new(body));
    let second = Example2::Second;
    let first_as_text = print_recon_compact(&first).to_string();
    let second_as_text = print_recon_compact(&second).to_string();

    let first_frame = RawRequestMessage::command(
        id,
        RelativeAddress::new(node, lane),
        first_as_text.as_bytes(),
    );
    let second_frame = RawRequestMessage::command(
        id,
        RelativeAddress::new(node, lane),
        second_as_text.as_bytes(),
    );

    let mut decoder = AgentMessageDecoder::<Example2, _>::new(Example2::make_recognizer());
    let mut encoder = RawRequestMessageEncoder;

    let mut buffer = BytesMut::new();
    assert!(encoder.encode(first_frame, &mut buffer).is_ok());
    assert!(encoder.encode(second_frame, &mut buffer).is_ok());

    let first_result = decoder.decode(&mut buffer);
    let second_result = decoder.decode(&mut buffer);

    check_result(
        first_result,
        RequestMessage::command(id, RelativeAddress::text(node, lane), first),
    );

    check_result(
        second_result,
        RequestMessage::command(id, RelativeAddress::text(node, lane), second),
    );
}
