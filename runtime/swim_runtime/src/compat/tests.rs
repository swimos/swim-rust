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
    RawAgentMessage, RawAgentMessageEncoder, COMMAND, HEADER_INIT_LEN, LINK, OP_MASK, OP_SHIFT,
    SYNC, UNLINK,
};
use bytes::{Buf, BytesMut};
use tokio_util::codec::Encoder;
use uuid::Uuid;

#[test]
fn encode_link_frame() {
    let id = Uuid::new_v4();
    let lane = "lane";
    let frame = RawAgentMessage::link(id, lane);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, LINK);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_sync_frame() {
    let id = Uuid::new_v4();
    let lane = "lane";
    let frame = RawAgentMessage::sync(id, lane);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, SYNC);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_unlink_frame() {
    let id = Uuid::new_v4();
    let lane = "lane";
    let frame = RawAgentMessage::unlink(id, lane);
    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + lane.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, UNLINK);

    let body_len = body_descriptor & !OP_MASK;
    assert_eq!(body_len, 0);
    assert_eq!(buffer.as_ref(), lane.as_bytes());
}

#[test]
fn encode_command_frame() {
    let id = Uuid::new_v4();
    let lane = "lane";
    let body = "@Example { first: 1, second: 2 }";
    let frame = RawAgentMessage::command(id, lane, body.as_bytes());

    let mut encoder = RawAgentMessageEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(frame, &mut buffer).is_ok());

    assert_eq!(buffer.len(), HEADER_INIT_LEN + lane.len() + body.len());

    assert_eq!(buffer.get_u128(), id.as_u128());
    assert_eq!(buffer.get_u32(), lane.len() as u32);
    let body_descriptor = buffer.get_u64();

    let tag = body_descriptor >> OP_SHIFT;
    assert_eq!(tag, COMMAND);

    let body_len = (body_descriptor & !OP_MASK) as usize;
    assert_eq!(body_len, body.len());

    let remainder = buffer.as_ref();

    let (lane_name, body_content) = remainder.split_at(lane.len());

    assert_eq!(lane_name, lane.as_bytes());
    assert_eq!(body_content, body.as_bytes());
}
