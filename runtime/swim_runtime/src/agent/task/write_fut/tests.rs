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

use bytes::{BufMut, BytesMut};
use futures::StreamExt;
use swim_api::protocol::map::RawMapOperationMut;
use swim_messages::{protocol::{RawResponseMessageDecoder, ResponseMessage, Notification, Path}, bytes_str::BytesStr};
use swim_model::Text;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader},
};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::{
    agent::task::remotes::RemoteSender,
    pressure::MapBackpressure,
    routing::RoutingAddr,
};

use super::{SpecialAction, WriteAction, WriteTask};

type Reader = FramedRead<ByteReader, RawResponseMessageDecoder>;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const ADDR: Uuid = *RoutingAddr::plane(5).uuid();
const REMOTE_ID: Uuid = Uuid::from_u128(2847743);
const NODE: &str = "/node";
const LANE: &str = "lane";

fn make_task(action: WriteAction, content: Option<&[u8]>) -> (WriteTask, Reader) {
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    let mut writer = RemoteSender::new(tx, ADDR, REMOTE_ID, Text::new(NODE));
    writer.update_lane(LANE);
    let mut buffer = BytesMut::new();
    if let Some(data) = content {
        buffer.put(data);
    }
    let task = WriteTask::new(writer, buffer, action);
    let reader = FramedRead::new(rx, Default::default());
    (task, reader)
}

const BODY: &str = "body";
const BODY_BYTES: &[u8] = BODY.as_bytes();

fn make_path() -> Path<BytesStr> {
    Path::new(BytesStr::from(NODE), BytesStr::from(LANE))
}

#[tokio::test]
async fn write_event() {
    let (task, mut reader) = make_task(WriteAction::Event, Some(BODY_BYTES));

    assert!(task.into_future().await.2.is_ok());

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Event(body),
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
            assert_eq!(body.as_ref(), BODY_BYTES);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[tokio::test]
async fn write_event_with_synced() {
    let (task, mut reader) = make_task(WriteAction::EventAndSynced, Some(BODY_BYTES));

    assert!(task.into_future().await.2.is_ok());

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Event(body),
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
            assert_eq!(body.as_ref(), BODY_BYTES);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Synced,
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

const KEY1: &str = "first";
const KEY1_BYTES: &[u8] = KEY1.as_bytes();
const KEY2: &str = "second";
const KEY2_BYTES: &[u8] = KEY2.as_bytes();

#[tokio::test]
async fn write_map_queue_with_synced() {
    let mut map_backpressure = MapBackpressure::default();
    assert!(map_backpressure.push(RawMapOperationMut::Clear).is_ok());
    assert!(map_backpressure
        .push(RawMapOperationMut::Update {
            key: BytesMut::from(KEY1_BYTES),
            value: BytesMut::from(BODY_BYTES)
        })
        .is_ok());
    assert!(map_backpressure
        .push(RawMapOperationMut::Remove {
            key: BytesMut::from(KEY2_BYTES)
        })
        .is_ok());

    let (task, mut reader) = make_task(
        WriteAction::MapSynced(Some(Box::new(map_backpressure))),
        None,
    );

    assert!(task.into_future().await.2.is_ok());

    let expected_bodies = [
        "@clear".to_string(),
        format!("@update(key:{}) {}", KEY1, BODY),
        format!("@remove(key:{})", KEY2),
    ];

    for expected in expected_bodies {
        let result = reader.next().await;
        match result {
            Some(Ok(ResponseMessage {
                origin,
                path,
                envelope: Notification::Event(body),
            })) => {
                assert_eq!(origin, ADDR);
                assert_eq!(path, make_path());
                let body_str = std::str::from_utf8(body.as_ref()).unwrap();
                assert_eq!(body_str, expected);
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
    }

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Synced,
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[tokio::test]
async fn write_linked() {
    let (task, mut reader) = make_task(
        WriteAction::Special(SpecialAction::Linked(0)),
        Some(BODY_BYTES),
    );

    assert!(task.into_future().await.2.is_ok());

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Linked,
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

const GONE: &str = "Gone";

#[tokio::test]
async fn write_unlinked() {
    let (task, mut reader) = make_task(
        WriteAction::Special(SpecialAction::unlinked(0, Text::new(GONE))),
        Some(BODY_BYTES),
    );

    assert!(task.into_future().await.2.is_ok());

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Unlinked(Some(message)),
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
            let message_str = std::str::from_utf8(message.as_ref()).unwrap();
            assert_eq!(message_str, GONE);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[tokio::test]
async fn write_lane_not_found() {
    let (task, mut reader) = make_task(
        WriteAction::Special(SpecialAction::lane_not_found(Text::new(LANE))),
        Some(BODY_BYTES),
    );

    assert!(task.into_future().await.2.is_ok());

    let result = reader.next().await;
    match result {
        Some(Ok(ResponseMessage {
            origin,
            path,
            envelope: Notification::Unlinked(Some(message)),
        })) => {
            assert_eq!(origin, ADDR);
            assert_eq!(path, make_path());
            assert_eq!(message.as_ref(), super::LANE_NOT_FOUND_BODY);
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}
