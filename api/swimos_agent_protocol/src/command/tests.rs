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

use crate::command::{RawCommandMessageDecoder, RawCommandMessageEncoder};

use super::{CommandMessage, ID_LEN};

fn round_trip(message: CommandMessage<&str, &[u8]>) -> CommandMessage<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = RawCommandMessageEncoder::default();
    let mut decoder = RawCommandMessageDecoder::<String>::default();

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
    let mut decoder = RawCommandMessageDecoder::<String>::default();

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
    match msg {
        CommandMessage::Register {
            address: Address { host, node, lane },
            ..
        } => {
            let n = super::FLAGS_LEN + node.len() + lane.len() + ID_LEN;
            if let Some(h) = host {
                n + super::MAX_REQUIRED + h.len()
            } else {
                n + super::MIN_REQUIRED
            }
        }
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            ..
        } => {
            let n = super::FLAGS_LEN + node.len() + lane.len();
            if let Some(h) = host {
                n + super::MAX_REQUIRED + h.len()
            } else {
                n + super::MIN_REQUIRED
            }
        }
        CommandMessage::Registered { .. } => super::FLAGS_LEN + super::ID_LEN,
    }
}

fn round_trip_partial(message: CommandMessage<&str, &[u8]>) -> CommandMessage<String, BytesMut> {
    let mut buffer = BytesMut::new();
    let mut encoder = RawCommandMessageEncoder::default();
    let mut decoder = RawCommandMessageDecoder::<String>::default();

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
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);

    match round_trip(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_with_host_no_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);

    match round_trip(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_with_host_partial_with_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);

    match round_trip_partial(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_with_host_partial_no_ow() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);

    match round_trip_partial(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_no_host_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);

    match round_trip(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_no_host_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);
    match round_trip(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_no_host_partial_with_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], true);

    match round_trip_partial(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_no_host_partial_no_ow() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::ad_hoc(addr, &[1, 2, 3], false);

    match round_trip_partial(msg) {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_two_messages() {
    let addr1 = Address::new(Some("ws://localhost:8080"), "/first_node", "lane1");
    let msg1 = CommandMessage::<_, &[u8]>::ad_hoc(addr1, &[1, 2, 3], true);

    let addr2 = Address::new(None, "/second_node", "lane2");
    let msg2 = CommandMessage::<_, &[u8]>::ad_hoc(addr2, &[4, 5, 6, 7, 8], false);
    let (first, second) = round_trip2(msg1, msg2);

    match first {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/first_node");
            assert_eq!(lane, "lane1");
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }

    match second {
        CommandMessage::Addressed {
            target: Address { host, node, lane },
            command,
            overwrite_permitted,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/second_node");
            assert_eq!(lane, "lane2");
            assert_eq!(command.as_ref(), &[4, 5, 6, 7, 8]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_registered_no_ow() {
    let msg = CommandMessage::<_, &[u8]>::registered(653, &[1, 2, 3], false);

    match round_trip(msg) {
        CommandMessage::Registered {
            target,
            command,
            overwrite_permitted,
        } => {
            assert_eq!(target, 653);
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_registered_with_ow() {
    let msg = CommandMessage::<_, &[u8]>::registered(187, &[1, 2, 3], true);

    match round_trip(msg) {
        CommandMessage::Registered {
            target,
            command,
            overwrite_permitted,
        } => {
            assert_eq!(target, 187);
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_registered_partial_no_ow() {
    let msg = CommandMessage::<_, &[u8]>::registered(653, &[1, 2, 3], false);

    match round_trip_partial(msg) {
        CommandMessage::Registered {
            target,
            command,
            overwrite_permitted,
        } => {
            assert_eq!(target, 653);
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(!overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_registered_partial_with_ow() {
    let msg = CommandMessage::<_, &[u8]>::registered(187, &[1, 2, 3], true);

    match round_trip_partial(msg) {
        CommandMessage::Registered {
            target,
            command,
            overwrite_permitted,
        } => {
            assert_eq!(target, 187);
            assert_eq!(command.as_ref(), &[1, 2, 3]);
            assert!(overwrite_permitted);
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_register() {
    let addr = Address::new(Some("ws://localhost:8080"), "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::register(addr, 7392);

    match round_trip(msg) {
        CommandMessage::Register {
            address: Address { host, node, lane },
            id,
        } => {
            assert_eq!(host, Some("ws://localhost:8080".to_string()));
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(id, 7392)
        }
        _ => panic!("Incorrect kind."),
    }
}

#[test]
fn round_trip_register_no_host() {
    let addr = Address::new(None, "/node", "lane");
    let msg = CommandMessage::<_, &[u8]>::register(addr, 9992);

    match round_trip(msg) {
        CommandMessage::Register {
            address: Address { host, node, lane },
            id,
        } => {
            assert!(host.is_none());
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(id, 9992)
        }
        _ => panic!("Incorrect kind."),
    }
}
