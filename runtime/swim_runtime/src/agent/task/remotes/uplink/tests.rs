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

use std::num::NonZeroUsize;

use bytes::{Bytes, BytesMut};
use swim_api::protocol::map::{MapOperation, RawMapOperation};
use swim_model::Text;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader},
    trigger::promise,
};
use uuid::Uuid;

use crate::{
    agent::{
        task::{
            remotes::{LaneRegistry, UplinkResponse},
            write_fut::WriteTask,
        },
        DisconnectionReason,
    },
    routing::RoutingAddr,
};

use super::{RemoteSender, SpecialAction, Uplinks, WriteAction};

const NODE_URI: &str = "/node";
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const REMOTE_ID: Uuid = Uuid::from_u128(748383);

fn make_uplinks() -> (Uplinks, ByteReader, promise::Receiver<DisconnectionReason>) {
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    let (completion_tx, completion_rx) = promise::promise();
    (
        Uplinks::new(
            Text::new(NODE_URI),
            RoutingAddr::plane(0),
            REMOTE_ID,
            tx,
            completion_tx,
        ),
        rx,
        completion_rx,
    )
}

const LANE_NAME: &str = "lane";
const OTHER_LANE_NAME: &str = "other";

fn lane_names() -> LaneRegistry {
    let mut names = LaneRegistry::default();
    assert_eq!(names.add_endpoint(Text::new(LANE_NAME)), 0);
    assert_eq!(names.add_endpoint(Text::new(OTHER_LANE_NAME)), 1);
    names
}

#[test]
fn push_linked() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask { sender, action, .. } = uplinks
        .push_special(SpecialAction::Linked(0), &lane_names)
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(
        action,
        WriteAction::Special(SpecialAction::Linked(0))
    ));
}

#[test]
fn push_unlinked() {
    let lane_names = lane_names();

    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask { sender, action, .. } = uplinks
        .push_special(SpecialAction::unlinked(0, Text::new("Gone")), &lane_names)
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(
        matches!(action, WriteAction::Special(SpecialAction::Unlinked { lane_id: 0, message }) if message == "Gone")
    );
}

#[test]
fn push_lane_not_found() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask { sender, action, .. } = uplinks
        .push_special(
            SpecialAction::lane_not_found(Text::new("boom")),
            &lane_names,
        )
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, "boom");
    assert!(
        matches!(action, WriteAction::Special(SpecialAction::LaneNotFound { lane_name }) if lane_name == "boom")
    );
}

#[test]
fn resinstate_writer() {
    let lane_names = lane_names();

    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask { sender, buffer, .. } = uplinks
        .push_special(SpecialAction::Linked(0), &lane_names)
        .expect("Expected immediate write.");

    let result = uplinks.replace_and_pop(sender, buffer, &lane_names);

    assert!(result.is_none());
}

#[test]
fn queue_special() {
    let lane_names = lane_names();

    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .push_special(SpecialAction::Linked(0), &lane_names)
        .expect("Expected immediate write.");

    let second_result =
        uplinks.push_special(SpecialAction::unlinked(1, Text::new("Gone")), &lane_names);
    assert!(second_result.is_none());

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(
        action,
        WriteAction::Special(SpecialAction::Linked(0))
    ));

    let WriteTask { sender, action, .. } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, OTHER_LANE_NAME);
    assert!(
        matches!(action, WriteAction::Special(SpecialAction::Unlinked { lane_id: 1, message }) if message == "Gone")
    );
}

const BODY1: &[u8] = b"@body(1)";
const BODY2: &[u8] = b"@body(2)";

#[test]
fn push_value_event() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let content = Bytes::from_static(BODY1);

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .push(0, UplinkResponse::Value(content), &lane_names)
        .expect("Action was invalid.")
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(action, WriteAction::Event));
    assert_eq!(buffer.as_ref(), BODY1);
}

#[test]
fn push_value_synced() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let content = Bytes::from_static(BODY1);

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .push(0, UplinkResponse::SyncedValue(content), &lane_names)
        .expect("Action was invalid.")
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(action, WriteAction::EventAndSynced));
    assert_eq!(buffer.as_ref(), BODY1);
}

#[test]
fn push_map_synced() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask { sender, action, .. } = uplinks
        .push(0, UplinkResponse::SyncedMap, &lane_names)
        .expect("Action was invalid.")
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(action, WriteAction::MapSynced(None)));
}

const KEY: i32 = 6;
const KEY_STR: &[u8] = b"6";

#[test]
fn push_good_map_event() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .push(
            0,
            UplinkResponse::Map(MapOperation::Remove {
                key: Bytes::from_static(KEY_STR),
            }),
            &lane_names,
        )
        .expect("Action was invalid.")
        .expect("Expected immediate write.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(action, WriteAction::Event));
    let expected = format!("@remove(key:{})", KEY);
    assert_eq!(buffer.as_ref(), expected.as_bytes());
}

const BAD_UTF8: &[u8] = &[0xf0, 0x28, 0x8c, 0x28, 0x00, 0x00, 0x00];

#[test]
fn push_bad_map_event() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, ..) = make_uplinks();

    let result = uplinks.push(
        0,
        UplinkResponse::Map(MapOperation::Remove {
            key: Bytes::from_static(BAD_UTF8),
        }),
        &lane_names,
    );

    assert!(result.is_err());
}

fn make_uplinks_writing() -> (
    Uplinks,
    ByteReader,
    promise::Receiver<DisconnectionReason>,
    RemoteSender,
    BytesMut,
) {
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    let (completion_tx, completion_rx) = promise::promise();
    let mut uplinks = Uplinks::new(
        Text::new(NODE_URI),
        RoutingAddr::plane(0),
        REMOTE_ID,
        tx,
        completion_tx,
    );
    let (writer, buffer) = uplinks.writer.take().unwrap();
    (uplinks, rx, completion_rx, writer, buffer)
}

#[test]
fn push_multiple_value_events_same_lane() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, _, sender, buffer) = make_uplinks_writing();

    let events = [
        (0, UplinkResponse::Value(Bytes::from_static(BODY1))),
        (0, UplinkResponse::Value(Bytes::from_static(BODY2))),
    ];

    for (id, event) in events {
        let result = uplinks
            .push(id, event, &lane_names)
            .expect("Action was invalid.");
        assert!(result.is_none());
    }

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    // Due to backpressure relief we expect to only see the second value.
    assert_eq!(&sender.lane, LANE_NAME);
    assert!(matches!(action, WriteAction::Event));
    assert_eq!(buffer.as_ref(), BODY2);

    assert!(uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .is_none());
}

#[test]
fn push_multiple_value_events_multiple_lanes() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, _, mut sender, mut buffer) = make_uplinks_writing();

    let events = [
        (0, UplinkResponse::Value(Bytes::from_static(BODY1))),
        (1, UplinkResponse::Value(Bytes::from_static(BODY2))),
    ];

    for (id, event) in events {
        let result = uplinks
            .push(id, event, &lane_names)
            .expect("Action was invalid.");
        assert!(result.is_none());
    }

    // Both events should be produced, in the order the were pushed.
    let expected = [(LANE_NAME, BODY1), (OTHER_LANE_NAME, BODY2)];

    for (name, body) in expected {
        let WriteTask {
            sender: send,
            buffer: buf,
            action,
        } = uplinks
            .replace_and_pop(sender, buffer, &lane_names)
            .expect("Expected queued result.");

        assert_eq!(&send.lane, name);
        assert!(matches!(action, WriteAction::Event));
        assert_eq!(buf.as_ref(), body);

        sender = send;
        buffer = buf;
    }

    assert!(uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .is_none());
}

#[test]
fn special_before_events() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, _, sender, buffer) = make_uplinks_writing();

    let result = uplinks
        .push(
            0,
            UplinkResponse::Value(Bytes::from_static(BODY1)),
            &lane_names,
        )
        .expect("Action was invalid.");
    assert!(result.is_none());

    let result = uplinks.push_special(SpecialAction::Linked(1), &lane_names);

    assert!(result.is_none());

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, OTHER_LANE_NAME);
    assert!(matches!(
        action,
        WriteAction::Special(SpecialAction::Linked(1))
    ));

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert_eq!(buffer.as_ref(), BODY1);
    assert!(matches!(action, WriteAction::Event));

    assert!(uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .is_none());
}

#[test]
fn unlink_clears_pending() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, _, sender, buffer) = make_uplinks_writing();

    let result = uplinks
        .push(
            0,
            UplinkResponse::Value(Bytes::from_static(BODY1)),
            &lane_names,
        )
        .expect("Action was invalid.");
    assert!(result.is_none());

    let result = uplinks
        .push(
            1,
            UplinkResponse::Value(Bytes::from_static(BODY2)),
            &lane_names,
        )
        .expect("Action was invalid.");
    assert!(result.is_none());

    let result = uplinks.push_special(SpecialAction::unlinked(0, Text::new("gone")), &lane_names);

    assert!(result.is_none());

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, LANE_NAME);
    assert!(
        matches!(action, WriteAction::Special(SpecialAction::Unlinked { lane_id: 0, message }) if message == "gone")
    );

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, OTHER_LANE_NAME);
    assert_eq!(buffer.as_ref(), BODY2);
    assert!(matches!(action, WriteAction::Event));

    assert!(uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .is_none());
}

const KEY1_STR: &[u8] = b"78";
const KEY2_STR: &[u8] = b"567";
const VAL1: &[u8] = b"value1";
const VAL2: &[u8] = b"value2";

#[test]
fn map_lane_sync_consumes_buffer() {
    let lane_names = lane_names();
    let (mut uplinks, _reader, _, sender, buffer) = make_uplinks_writing();

    let ops = [
        MapOperation::Clear,
        MapOperation::Update {
            key: Bytes::from_static(KEY1_STR),
            value: Bytes::from_static(VAL1),
        },
        MapOperation::Update {
            key: Bytes::from_static(KEY2_STR),
            value: Bytes::from_static(VAL2),
        },
    ];

    for op in ops {
        let result = uplinks
            .push(0, UplinkResponse::Map(op), &lane_names)
            .expect("Action was invalid.");
        assert!(result.is_none());
    }

    let result = uplinks
        .push(0, UplinkResponse::SyncedMap, &lane_names)
        .expect("Action was invalid.");
    assert!(result.is_none());

    let WriteTask {
        sender,
        buffer,
        action,
    } = uplinks
        .replace_and_pop(sender, buffer, &lane_names)
        .expect("Expected queued result.");

    assert_eq!(&sender.lane, LANE_NAME);
    match action {
        WriteAction::MapSynced(Some(mut queue)) => {
            let first = queue.pop();
            let second = queue.pop();
            let third = queue.pop();
            assert!(matches!(first, Some(RawMapOperation::Clear)));
            assert!(
                matches!(second, Some(RawMapOperation::Update { key, value }) if key.as_ref() == KEY1_STR && value.as_ref() == VAL1)
            );
            assert!(
                matches!(third, Some(RawMapOperation::Update { key, value }) if key.as_ref() == KEY2_STR && value.as_ref() == VAL2)
            );
            assert!(queue.pop().is_none());
        }
        ow => panic!("Unexpected action {:?}.", ow),
    }

    let result = uplinks.replace_and_pop(sender, buffer, &lane_names);
    assert!(result.is_none());
}
