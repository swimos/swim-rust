// Copyright 2015-2024 Swim Inc.
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

use bytes::BytesMut;
use swimos_api::address::Address;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    ad_hoc::{RawAdHocCommandDecoder, RawCommandMessageEncoder},
    CommandMessageTarget,
};

use super::CommandMessage;

fn round_trip(message: CommandMessage<&str, &[u8]>) -> CommandMessage<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = RawCommandMessageEncoder::default();
    let mut decoder = RawAdHocCommandDecoder::<String>::default();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    match decoder.decode_eof(&mut buffer) {
        Ok(Some(msg)) => {
            assert!(buffer.is_empty());
            msg
        }
        Err(e) => panic!("{}", e),
        _ => panic!("Incomplete."),
    }
}

fn round_trip2(
    message1: CommandMessage<&str, &[u8]>,
    message2: CommandMessage<&str, &[u8]>,
) -> (
    CommandMessage<String, BytesMut>,
    CommandMessage<String, BytesMut>,
) {
    let mut buffer = BytesMut::new();
    let mut encoder = RawCommandMessageEncoder::default();
    let mut decoder = RawAdHocCommandDecoder::<String>::default();

    assert!(encoder.encode(message1, &mut buffer).is_ok());
    assert!(encoder.encode(message2, &mut buffer).is_ok());

    let first = match decoder.decode(&mut buffer) {
        Ok(Some(msg)) => msg,
        Err(e) => panic!("{}", e),
        _ => panic!("Incomplete."),
    };

    let second = match decoder.decode_eof(&mut buffer) {
        Ok(Some(msg)) => {
            assert!(buffer.is_empty());
            msg
        }
        Err(e) => panic!("{}", e),
        _ => panic!("Incomplete."),
    };

    (first, second)
}

fn header_len(msg: &CommandMessage<&str, &[u8]>) -> usize {
    match &msg.target {
        CommandMessageTarget::Addressed(Address { host, node, lane }) => {
            let n = node.len() + lane.len();
            if let Some(h) = host {
                n + super::MAX_REQUIRED + h.len()
            } else {
                n + super::MIN_REQUIRED
            }
        }
        CommandMessageTarget::Registered(_) => todo!(),
    }
}

fn round_trip_partial(message: CommandMessage<&str, &[u8]>) -> CommandMessage<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = RawCommandMessageEncoder::default();
    let mut decoder = RawAdHocCommandDecoder::<String>::default();

    let header_len = header_len(&message);
    assert!(encoder.encode(message, &mut buffer).is_ok());

    let mut part1 = buffer.split_to(header_len);

    match decoder.decode_eof(&mut part1) {
        Ok(Some(_)) => panic!("Terminated early."),
        Err(e) => panic!("{}", e),
        _ => {}
    }

    match decoder.decode_eof(&mut buffer) {
        Ok(Some(msg)) => {
            assert!(buffer.is_empty());
            msg
        }
        Err(e) => panic!("{}", e),
        _ => panic!("Incomplete."),
    }
}

fn extract_address(target: CommandMessageTarget<String>) -> Address<String> {
    match target {
        CommandMessageTarget::Addressed(addr) => addr,
        CommandMessageTarget::Registered(_) => panic!("Not addressed."),
    }
}

#[test]
fn round_trip_with_host_with_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip(msg);

    let Address { host, node, lane } = extract_address(target);

    assert_eq!(host, Some("ws://localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_with_host_no_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip(msg);

    let Address { host, node, lane } = extract_address(target);

    assert_eq!(host, Some("ws://localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_with_host_partial_with_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    let Address { host, node, lane } = extract_address(target);

    assert_eq!(host, Some("ws://localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_with_host_partial_no_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    let Address { host, node, lane } = extract_address(target);

    assert_eq!(host, Some("ws://localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_no_host_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip(msg);

    let Address { host, node, lane } = extract_address(target);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_no_host_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip(msg);

    let Address { host, node, lane } = extract_address(target);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_no_host_partial_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    let Address { host, node, lane } = extract_address(target);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_no_host_partial_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);
    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    let Address { host, node, lane } = extract_address(target);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_two_messages() {
    let addr1 = Address::new(Some("ws://localhost:8080"), "/first_node", "lane1");
    let msg1 = CommandMessage::<_, &[u8]>::ad_hoc(addr1, &[1, 2, 3], true);

    let addr2 = Address::new(None, "/second_node", "lane2");
    let msg2 = CommandMessage::<_, &[u8]>::ad_hoc(addr2, &[4, 5, 6, 7, 8], false);
    let (first, second) = round_trip2(msg1, msg2);

    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = first;

    let Address { host, node, lane } = extract_address(target);

    assert_eq!(host, Some("ws://localhost:8080".to_string()));
    assert_eq!(node, "/first_node");
    assert_eq!(lane, "lane1");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);

    let CommandMessage {
        target,
        command,
        overwrite_permitted,
    } = second;

    let Address { host, node, lane } = extract_address(target);

    assert!(host.is_none());
    assert_eq!(node, "/second_node");
    assert_eq!(lane, "lane2");
    assert_eq!(command.as_ref(), &[4, 5, 6, 7, 8]);
    assert!(!overwrite_permitted);
}
