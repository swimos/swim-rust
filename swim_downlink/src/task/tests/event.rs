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

use swim_api::error::{DownlinkTaskError, FrameIoError, InvalidFrame};
use swim_api::{downlink::DownlinkConfig, protocol::downlink::DownlinkNotification};

use swim_utilities::algebra::non_zero_usize;
use tokio::sync::mpsc;

use super::run_downlink_task;
use crate::model::lifecycle::{BasicEventDownlinkLifecycle, EventDownlinkLifecycle};
use crate::{DownlinkTask, EventDownlinkModel};

#[derive(Debug, PartialEq, Eq)]
enum TestMessage<T> {
    Linked,
    Event(T),
    Unlinked,
}

fn make_lifecycle<T>(tx: mpsc::UnboundedSender<TestMessage<T>>) -> impl EventDownlinkLifecycle<T>
where
    T: Clone + Send + Sync + 'static,
{
    BasicEventDownlinkLifecycle::<T>::default()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(TestMessage::Linked).is_ok());
        })
        .on_event_blocking(|tx, v| {
            assert!(tx.send(TestMessage::Event(v.clone())).is_ok());
        })
        .on_unlinked_blocking(|tx| {
            assert!(tx.send(TestMessage::Unlinked).is_ok());
        })
}

async fn expect_event<T: Eq + std::fmt::Debug>(
    event_rx: &mut mpsc::UnboundedReceiver<TestMessage<T>>,
    expected: TestMessage<T>,
) {
    assert_eq!(event_rx.recv().await, Some(expected))
}

const DEEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

#[tokio::test]
async fn link_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn message_before_linked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 9 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn message_after_linked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 9 })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Event(9)).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn terminate_after_unlinked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 9 })
                .await;
            writer
                .send_value::<i32>(DownlinkNotification::Unlinked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Event(9)).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            (writer, reader, event_rx)
        },
    )
    .await;
    match result {
        Ok((_writer, _reader, mut events)) => {
            assert!(events.recv().await.is_none());
        }
        Err(e) => {
            panic!("Task failed: {}", e)
        }
    }
}

#[tokio::test]
async fn terminate_after_corrupt_frame() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            writer.send_corrupted_frame().await;
            (writer, reader, event_rx)
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(DownlinkTaskError::BadFrame(FrameIoError::BadFrame(
            InvalidFrame::InvalidMessageBody(_)
        )))
    ));
}

#[tokio::test]
async fn relink_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32>>();
    let lifecycle = make_lifecycle(event_tx);
    let model = EventDownlinkModel::<i32, _>::new(lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
        buffer_size: DEEFAULT_BUFFER_SIZE,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 9 })
                .await;
            writer
                .send_value::<i32>(DownlinkNotification::Unlinked)
                .await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 10 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 11 })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Event(9)).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Event(11)).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}
