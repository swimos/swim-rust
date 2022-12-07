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
use futures::StreamExt;
use swim_messages::protocol::{Notification, RawResponseMessageDecoder, ResponseMessage};
use swim_model::Text;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader},
};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::routing::RoutingAddr;

use super::RemoteSender;

const ID: Uuid = *RoutingAddr::plane(1).uuid();
const REMOTE_ID: Uuid = Uuid::from_u128(8573923);
const NODE: &str = "node_uri";

type Reader = FramedRead<ByteReader, RawResponseMessageDecoder>;

fn make_sender() -> (RemoteSender, Reader) {
    let (tx, rx) = byte_channel(non_zero_usize!(4096));
    let reader = FramedRead::new(rx, RawResponseMessageDecoder);
    (
        RemoteSender::new(tx, ID, REMOTE_ID, Text::new(NODE)),
        reader,
    )
}

#[test]
fn get_remote_id() {
    let (sender, _receiver) = make_sender();
    assert_eq!(sender.remote_id(), REMOTE_ID);
}

#[tokio::test]
async fn send_record() {
    let (mut sender, mut receiver) = make_sender();

    sender.update_lane("my_lane");
    let write_result = sender.send_notification(Notification::Linked).await;
    assert!(write_result.is_ok());

    let read_result = receiver.next().await;
    match read_result {
        Some(Ok(resp)) => {
            let ResponseMessage {
                origin,
                path,
                envelope,
            } = resp;
            assert_eq!(origin, ID);
            assert_eq!(&path.node, NODE);
            assert_eq!(&path.lane, "my_lane");
            assert!(matches!(envelope, Notification::Linked));
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[tokio::test]
async fn send_records_same_lane() {
    let (mut sender, mut receiver) = make_sender();

    sender.update_lane("my_lane");

    let mut data = BytesMut::new();
    data.put(b"body".as_ref());

    let records = vec![Notification::Linked, Notification::Event(&data)];

    for record in records {
        let write_result = sender.send_notification(record).await;
        assert!(write_result.is_ok());
    }

    let read_result1 = receiver.next().await;
    let read_result2 = receiver.next().await;
    match read_result1 {
        Some(Ok(resp)) => {
            let ResponseMessage {
                origin,
                path,
                envelope,
            } = resp;
            assert_eq!(origin, ID);
            assert_eq!(&path.node, NODE);
            assert_eq!(&path.lane, "my_lane");
            assert!(matches!(envelope, Notification::Linked));
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    match read_result2 {
        Some(Ok(resp)) => {
            let ResponseMessage {
                origin,
                path,
                envelope,
            } = resp;
            assert_eq!(origin, ID);
            assert_eq!(&path.node, NODE);
            assert_eq!(&path.lane, "my_lane");
            match envelope {
                Notification::Event(content) => {
                    assert_eq!(content.as_ref(), b"body");
                }
                ow => panic!("Unexpected envelope: {:?}", ow),
            }
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}

#[tokio::test]
async fn send_records_switch_lane() {
    let (mut sender, mut receiver) = make_sender();

    let mut data = BytesMut::new();
    data.put(b"body".as_ref());

    let records = vec![
        ("my_lane", Notification::Linked),
        ("other", Notification::Event(&data)),
    ];

    for (lane, record) in records {
        sender.update_lane(lane);
        let write_result = sender.send_notification(record).await;
        assert!(write_result.is_ok());
    }

    let read_result1 = receiver.next().await;
    let read_result2 = receiver.next().await;
    match read_result1 {
        Some(Ok(resp)) => {
            let ResponseMessage {
                origin,
                path,
                envelope,
            } = resp;
            assert_eq!(origin, ID);
            assert_eq!(&path.node, NODE);
            assert_eq!(&path.lane, "my_lane");
            assert!(matches!(envelope, Notification::Linked));
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }

    match read_result2 {
        Some(Ok(resp)) => {
            let ResponseMessage {
                origin,
                path,
                envelope,
            } = resp;
            assert_eq!(origin, ID);
            assert_eq!(&path.node, NODE);
            assert_eq!(&path.lane, "other");
            match envelope {
                Notification::Event(content) => {
                    assert_eq!(content.as_ref(), b"body");
                }
                ow => panic!("Unexpected envelope: {:?}", ow),
            }
        }
        ow => panic!("Unexpected result: {:?}", ow),
    }
}
