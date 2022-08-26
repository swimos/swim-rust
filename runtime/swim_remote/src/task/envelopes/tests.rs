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

use bytes::{Bytes, BytesMut};
use swim_messages::{
    bytes_str::BytesStr,
    protocol::{
        path_from_static_strs, BytesRequestMessage, BytesResponseMessage, Path, RequestMessage,
        ResponseMessage,
    },
};
use swim_model::Text;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::NoSuchAgent;

use super::ReconEncoder;

const ID: Uuid = Uuid::from_u128(7474834);
const NODE: &str = "/node";
const LANE: &str = "lane";

fn path() -> Path<BytesStr> {
    path_from_static_strs(NODE, LANE)
}

#[test]
fn encode_link() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage = RequestMessage::link(ID, path());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@link(node:\"/node\",lane:lane)");
}

#[test]
fn encode_sync() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage = RequestMessage::sync(ID, path());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@sync(node:\"/node\",lane:lane)");
}

#[test]
fn encode_unlink() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage = RequestMessage::unlink(ID, path());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@unlink(node:\"/node\",lane:lane)");
}

#[test]
fn encode_command_empty() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage = RequestMessage::command(ID, path(), Bytes::new());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@command(node:\"/node\",lane:lane)");
}

#[test]
fn encode_command() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage =
        RequestMessage::command(ID, path(), Bytes::from_static(b"body"));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@command(node:\"/node\",lane:lane) body");
}

#[test]
fn encode_command_with_attr() {
    let mut encoder = ReconEncoder;
    let message: BytesRequestMessage =
        RequestMessage::command(ID, path(), Bytes::from_static(b"@body"));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@command(node:\"/node\",lane:lane)@body");
}

#[test]
fn encode_linked() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage = ResponseMessage::linked(ID, path());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@linked(node:\"/node\",lane:lane)");
}

#[test]
fn encode_synced() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage = ResponseMessage::synced(ID, path());

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@synced(node:\"/node\",lane:lane)");
}

#[test]
fn encode_unlinked_no_body() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage = ResponseMessage::unlinked(ID, path(), None);

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@unlinked(node:\"/node\",lane:lane)");
}

#[test]
fn encode_unlinked_empty() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage = ResponseMessage::unlinked(ID, path(), Some(Bytes::new()));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@unlinked(node:\"/node\",lane:lane)");
}

#[test]
fn encode_unlinked() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage =
        ResponseMessage::unlinked(ID, path(), Some(Bytes::from_static(b"gone")));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@unlinked(node:\"/node\",lane:lane) gone");
}

#[test]
fn encode_event() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage =
        ResponseMessage::event(ID, path(), Bytes::from_static(b"body"));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@event(node:\"/node\",lane:lane) body");
}

#[test]
fn encode_event_with_attr() {
    let mut encoder = ReconEncoder;
    let message: BytesResponseMessage =
        ResponseMessage::event(ID, path(), Bytes::from_static(b"@body"));

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(envelope_str, "@event(node:\"/node\",lane:lane)@body");
}

#[test]
fn encode_not_found() {
    let mut encoder = ReconEncoder;
    let message = NoSuchAgent {
        node: Text::new(NODE),
        lane: Text::new(LANE),
    };

    let mut buffer = BytesMut::new();

    assert!(encoder.encode(message, &mut buffer).is_ok());

    let envelope_str = std::str::from_utf8(buffer.as_ref()).expect("Invalid UTF8!");

    assert_eq!(
        envelope_str,
        "@unlinked(node:\"/node\",lane:lane)@nodeNotFound"
    );
}
