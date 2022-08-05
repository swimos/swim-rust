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

use std::{num::NonZeroUsize, time::Duration};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use swim_model::{path::RelativePath, Text};
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader},
    trigger::promise,
};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::{
    agent::{
        task::write_fut::{SpecialAction, WriteTask},
        DisconnectionReason,
    },
    compat::{RawResponseMessage, RawResponseMessageDecoder},
    routing::RoutingAddr,
};

use super::{RemoteSender, RemoteTracker, UplinkResponse};

const RID1: Uuid = Uuid::from_u128(757373);
const RID2: Uuid = Uuid::from_u128(4639830);
const LANE: &str = "lane";

const ADDR: RoutingAddr = RoutingAddr::plane(1);
const NODE: &str = "/node";
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

#[test]
fn insert_remote() {
    let (tx, _rx) = byte_channel(BUFFER_SIZE);
    let mut remotes = RemoteTracker::new(ADDR, Text::new(NODE));
    let (comp_tx, _comp_rx) = promise::promise();

    assert!(remotes.is_empty());
    assert!(!remotes.has_remote(RID1));

    remotes.insert(RID1, tx, comp_tx);

    assert!(!remotes.is_empty());
    assert!(remotes.has_remote(RID1));
}

struct TestData {
    remotes: RemoteTracker,
    rx1: ByteReader,
    rx2: ByteReader,
    comp_rx1: promise::Receiver<DisconnectionReason>,
    comp_rx2: promise::Receiver<DisconnectionReason>,
    lane_id: u64,
}

fn setup() -> TestData {
    let (tx1, rx1) = byte_channel(BUFFER_SIZE);
    let (tx2, rx2) = byte_channel(BUFFER_SIZE);
    let (comp_tx1, comp_rx1) = promise::promise();
    let (comp_tx2, comp_rx2) = promise::promise();
    let mut remotes = RemoteTracker::new(ADDR, Text::new(NODE));
    let lane_id = remotes.lane_registry().add_endpoint(Text::new(LANE));

    remotes.insert(RID1, tx1, comp_tx1);
    remotes.insert(RID2, tx2, comp_tx2);

    TestData {
        remotes,
        rx1,
        rx2,
        comp_rx1,
        comp_rx2,
        lane_id,
    }
}

async fn expect_message(
    task: WriteTask,
    rx: &mut ByteReader,
    expected: RawResponseMessage,
) -> (RemoteSender, BytesMut) {
    let mut read = FramedRead::new(rx, RawResponseMessageDecoder::default());
    let (writer, buffer, result) = task.into_future().await;
    assert!(result.is_ok());
    match read.next().await {
        Some(Ok(frame)) => assert_eq!(frame, expected),
        Some(Err(e)) => panic!("Read failed: {:?}", e),
        _ => panic!("Channel dropped."),
    }
    (writer, buffer)
}

#[tokio::test]
async fn dispatch_special() {
    let TestData {
        mut remotes,
        mut rx1,
        rx2: _rx2,
        lane_id,
        ..
    } = setup();
    if let Some(write) = remotes.push_special(SpecialAction::Linked(lane_id), &RID1) {
        let expected = RawResponseMessage::linked(ADDR, RelativePath::new(NODE, LANE));
        expect_message(write, &mut rx1, expected).await;
    } else {
        panic!("Expected a write task.");
    }
}

const BODY: &[u8] = b"body";

#[tokio::test]
async fn dispatch_normal() {
    let TestData {
        mut remotes,
        mut rx1,
        rx2: _rx2,
        lane_id,
        ..
    } = setup();
    if let Ok(Some(write)) = remotes.push_write(
        lane_id,
        UplinkResponse::Value(Bytes::from_static(BODY)),
        &RID1,
    ) {
        let expected = RawResponseMessage::event(
            ADDR,
            RelativePath::new(NODE, LANE),
            Bytes::from_static(BODY),
        );
        expect_message(write, &mut rx1, expected).await;
    } else {
        panic!("Expected a write task.");
    }
}

fn setup_with_pending(queue_write: bool) -> (TestData, WriteTask) {
    let TestData {
        mut remotes,
        rx1,
        rx2,
        lane_id,
        comp_rx1,
        comp_rx2,
    } = setup();
    let write = remotes
        .push_special(SpecialAction::Linked(lane_id), &RID1)
        .unwrap();
    if queue_write {
        assert!(matches!(
            remotes.push_write(
                lane_id,
                UplinkResponse::Value(Bytes::from_static(BODY)),
                &RID1
            ),
            Ok(None)
        ));
    }
    (
        TestData {
            remotes,
            rx1,
            rx2,
            lane_id,
            comp_rx1,
            comp_rx2,
        },
        write,
    )
}

#[tokio::test]
async fn replace_sender() {
    let (
        TestData {
            mut remotes,
            mut rx1,
            rx2: _rx2,
            ..
        },
        write,
    ) = setup_with_pending(false);

    let expected = RawResponseMessage::linked(ADDR, RelativePath::new(NODE, LANE));
    let (writer, buffer) = expect_message(write, &mut rx1, expected).await;

    assert!(remotes.replace_and_pop(writer, buffer).is_none());
}

#[tokio::test]
async fn replace_sender_queued() {
    let (
        TestData {
            mut remotes,
            mut rx1,
            rx2: _rx2,
            ..
        },
        write,
    ) = setup_with_pending(true);

    let expected = RawResponseMessage::linked(ADDR, RelativePath::new(NODE, LANE));
    let (writer, buffer) = expect_message(write, &mut rx1, expected).await;

    if let Some(write) = remotes.replace_and_pop(writer, buffer) {
        let expected = RawResponseMessage::event(
            ADDR,
            RelativePath::new(NODE, LANE),
            Bytes::from_static(BODY),
        );
        expect_message(write, &mut rx1, expected).await;
    } else {
        panic!("Expected a write task.");
    }
}

#[tokio::test]
async fn remove_remote() {
    let TestData {
        mut remotes,
        rx1: _rx1,
        rx2: _rx2,
        comp_rx2,
        ..
    } = setup();
    remotes.remove_remote(RID2, DisconnectionReason::RemoteTimedOut);
    assert!(!remotes.has_remote(RID2));
    let result = tokio::time::timeout(Duration::from_secs(1), comp_rx2)
        .await
        .expect("Timed out.")
        .expect("Reason promised dropped.");

    assert_eq!(*result, DisconnectionReason::RemoteTimedOut);
}
