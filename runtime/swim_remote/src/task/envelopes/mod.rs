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

use bytes::{BufMut, BytesMut};
use swim_messages::protocol::{
    BytesRequestMessage, BytesResponseMessage, Notification, Operation, Path, RequestMessage,
    ResponseMessage,
};
use swim_model::{escape_if_needed, identifier::is_identifier};
use tokio_util::codec::Encoder;

use crate::error::LaneNotFound;

#[derive(Debug, Default)]
pub struct ReconEncoder;

const LINK_HEADER: &[u8] = b"@link(";
const SYNC_HEADER: &[u8] = b"@sync(";
const UNLINK_HEADER: &[u8] = b"@unlink(";
const CMD_HEADER: &[u8] = b"@command(";

const LINKED_HEADER: &[u8] = b"@linked(";
const SYNCED_HEADER: &[u8] = b"@synced(";
const UNLINKED_HEADER: &[u8] = b"@unlinked(";
const EVENT_HEADER: &[u8] = b"@event(";

const NODE_TAG: &[u8] = b"node:";
const LANE_TAG: &[u8] = b"lane:";

const NODE_NOT_FOUND_TAG: &str = "@nodeNotFound";
const LANE_NOT_FOUND_TAG: &str = "@laneNotFound";

const FIXED_LEN: usize = 12;

impl Encoder<BytesRequestMessage> for ReconEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: BytesRequestMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let RequestMessage {
            path: Path { node, lane },
            envelope,
            ..
        } = item;
        match envelope {
            Operation::Link => write_header(LINK_HEADER, node.as_str(), lane.as_str(), dst),
            Operation::Sync => write_header(SYNC_HEADER, node.as_str(), lane.as_str(), dst),
            Operation::Unlink => write_header(UNLINK_HEADER, node.as_str(), lane.as_str(), dst),
            Operation::Command(body) => {
                write_header(CMD_HEADER, node.as_str(), lane.as_str(), dst);
                dst.reserve(body.len() + 1);
                dst.put_u8(b' ');
                dst.put(body);
            }
        }
        Ok(())
    }
}

impl Encoder<BytesResponseMessage> for ReconEncoder {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: BytesResponseMessage,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let ResponseMessage {
            path: Path { node, lane },
            envelope,
            ..
        } = item;
        match envelope {
            Notification::Linked => write_header(LINKED_HEADER, node.as_str(), lane.as_str(), dst),
            Notification::Synced => write_header(SYNCED_HEADER, node.as_str(), lane.as_str(), dst),
            Notification::Unlinked(body) => {
                write_header(UNLINKED_HEADER, node.as_str(), lane.as_str(), dst);
                if let Some(body) = body {
                    dst.reserve(body.len() + 1);
                    dst.put_u8(b' ');
                    dst.put(body);
                }
            }
            Notification::Event(body) => {
                write_header(EVENT_HEADER, node.as_str(), lane.as_str(), dst);
                dst.reserve(body.len() + 1);
                dst.put_u8(b' ');
                dst.put(body);
            }
        }
        Ok(())
    }
}

impl Encoder<LaneNotFound> for ReconEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: LaneNotFound, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (node, lane, body) = match item {
            LaneNotFound::NoSuchAgent { node, lane } => (node, lane, NODE_NOT_FOUND_TAG),
            LaneNotFound::NoSuchLane { node, lane } => (node, lane, LANE_NOT_FOUND_TAG),
        };
        write_header(UNLINKED_HEADER, node.as_str(), lane.as_str(), dst);
        dst.reserve(body.len() + 1);
        dst.put_u8(b' ');
        dst.put_slice(body.as_bytes());
        Ok(())
    }
}

fn compute_len(header: &[u8], node: &str, lane: &str, node_ident: bool, lane_ident: bool) -> usize {
    header.len() + len_lit(node, node_ident) + len_lit(lane, lane_ident) + FIXED_LEN
}

fn len_lit(lit: &str, ident: bool) -> usize {
    if ident {
        lit.len()
    } else {
        lit.len() + 2
    }
}

fn write_lit(lit: &str, ident: bool, dst: &mut BytesMut) {
    if ident {
        dst.put_slice(lit.as_bytes());
    } else {
        dst.put_u8(b'\"');
        dst.put_slice(lit.as_bytes());
        dst.put_u8(b'\"');
    }
}

fn write_header(header: &[u8], node: &str, lane: &str, dst: &mut BytesMut) {
    let node_ident = is_identifier(node);
    let lane_ident = is_identifier(lane);

    let node_str = escape_if_needed(node);
    let lane_str = escape_if_needed(lane);

    let header_len = compute_len(
        header,
        node_str.as_ref(),
        lane_str.as_ref(),
        node_ident,
        lane_ident,
    );

    dst.reserve(header_len);
    dst.put_slice(header);
    dst.put_slice(NODE_TAG);
    write_lit(node_str.as_ref(), node_ident, dst);
    dst.put_u8(b',');
    dst.put_slice(LANE_TAG);
    write_lit(lane_str.as_ref(), lane_ident, dst);
    dst.put_u8(b')');
}
