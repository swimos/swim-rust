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

use bytes::BytesMut;
use swim_model::address::Address;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{agent::ad_hoc::AdHocCommandDecoder, WithLengthBytesCodec};

use super::{AdHocCommand, AdHocCommandEncoder};

fn round_trip(message: AdHocCommand<&str, &[u8]>) -> AdHocCommand<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = AdHocCommandEncoder::new(WithLengthBytesCodec);
    let mut decoder = AdHocCommandDecoder::<String, _>::new(WithLengthBytesCodec);

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
    message1: AdHocCommand<&str, &[u8]>,
    message2: AdHocCommand<&str, &[u8]>,
) -> (
    AdHocCommand<String, BytesMut>,
    AdHocCommand<String, BytesMut>,
) {
    let mut buffer = BytesMut::new();
    let mut encoder = AdHocCommandEncoder::new(WithLengthBytesCodec);
    let mut decoder = AdHocCommandDecoder::<String, _>::new(WithLengthBytesCodec);

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

fn header_len(msg: &AdHocCommand<&str, &[u8]>) -> usize {
    let Address { host, node, lane } = &msg.address;
    let n = node.len() + lane.len();
    if let Some(h) = host {
        n + super::MAX_REQUIRED + h.len()
    } else {
        n + super::MIN_REQUIRED
    }
}

fn round_trip_partial(message: AdHocCommand<&str, &[u8]>) -> AdHocCommand<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = AdHocCommandEncoder::new(WithLengthBytesCodec);
    let mut decoder = AdHocCommandDecoder::<String, _>::new(WithLengthBytesCodec);

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

#[test]
fn round_trip_with_host_with_ow() {
    let addr = Address::new(Some("localhost:8080"), "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], true);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip(msg);

    assert_eq!(host, Some("localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_with_host_no_ow() {
    let addr = Address::new(Some("localhost:8080"), "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], false);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip(msg);

    assert_eq!(host, Some("localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_with_host_partial_with_ow() {
    let addr = Address::new(Some("localhost:8080"), "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], true);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    assert_eq!(host, Some("localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_with_host_partial_no_ow() {
    let addr = Address::new(Some("localhost:8080"), "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], false);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    assert_eq!(host, Some("localhost:8080".to_string()));
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_no_host_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], true);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip(msg);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_no_host_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], false);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip(msg);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_no_host_partial_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], true);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);
}

#[test]
fn round_trip_no_host_partial_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = AdHocCommand::<_, &[u8]>::new(addr, &[1, 2, 3], false);
    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = round_trip_partial(msg);

    assert!(host.is_none());
    assert_eq!(node, "/node");
    assert_eq!(lane, "lane");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(!overwrite_permitted);
}

#[test]
fn round_trip_two_messages() {
    let addr1 = Address::new(Some("localhost:8080"), "/first_node", "lane1");
    let msg1 = AdHocCommand::<_, &[u8]>::new(addr1, &[1, 2, 3], true);

    let addr2 = Address::new(None, "/second_node", "lane2");
    let msg2 = AdHocCommand::<_, &[u8]>::new(addr2, &[4, 5, 6, 7, 8], false);
    let (first, second) = round_trip2(msg1, msg2);

    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = first;
    assert_eq!(host, Some("localhost:8080".to_string()));
    assert_eq!(node, "/first_node");
    assert_eq!(lane, "lane1");
    assert_eq!(command.as_ref(), &[1, 2, 3]);
    assert!(overwrite_permitted);

    let AdHocCommand {
        address: Address { host, node, lane },
        command,
        overwrite_permitted,
    } = second;
    assert!(host.is_none());
    assert_eq!(node, "/second_node");
    assert_eq!(lane, "lane2");
    assert_eq!(command.as_ref(), &[4, 5, 6, 7, 8]);
    assert!(!overwrite_permitted);
}
