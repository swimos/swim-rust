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

use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use swimos_agent_protocol::MapMessage;
use swimos_client_api::{Downlink, DownlinkConfig};

use swimos_agent_protocol::encoding::downlink::DownlinkNotificationEncoder;
use swimos_agent_protocol::encoding::map::MapMessageEncoder;
use swimos_agent_protocol::DownlinkNotification;
use swimos_api::error::{DownlinkTaskError, FrameIoError, InvalidFrame};
use swimos_form::write::StructuralWritable;
use swimos_form::Form;
use swimos_utilities::io::byte_channel::ByteWriter;
use swimos_utilities::non_zero_usize;
use tokio::sync::mpsc;
use tokio_util::codec::{Encoder, FramedWrite};

use super::{run_downlink_task, TestReader};
use crate::lifecycle::BasicMapDownlinkLifecycle;
use crate::model::lifecycle::MapDownlinkLifecycle;
use crate::model::MapDownlinkModel;
use crate::{DownlinkTask, MapDownlinkHandle};

async fn run_map_downlink_task<D, F, Fut>(
    task: D,
    config: DownlinkConfig,
    test_block: F,
) -> Result<Fut::Output, DownlinkTaskError>
where
    D: Downlink,
    F: FnOnce(TestMapWriter, TestReader) -> Fut,
    Fut: Future,
{
    run_downlink_task(task, config, test_block, TestMapWriter::new).await
}

struct TestMapWriter(FramedWrite<ByteWriter, DownlinkNotificationEncoder>);

impl TestMapWriter {
    fn new(tx: ByteWriter) -> Self {
        TestMapWriter(FramedWrite::new(tx, DownlinkNotificationEncoder))
    }

    async fn send_message<K, V>(&mut self, notification: DownlinkNotification<MapMessage<K, V>>)
    where
        K: StructuralWritable,
        V: StructuralWritable,
    {
        let TestMapWriter(writer) = self;
        let raw = match notification {
            DownlinkNotification::Linked => DownlinkNotification::Linked,
            DownlinkNotification::Synced => DownlinkNotification::Synced,
            DownlinkNotification::Unlinked => DownlinkNotification::Unlinked,
            DownlinkNotification::Event { body } => {
                let mut encoder = MapMessageEncoder::default();
                let mut buf = BytesMut::new();
                encoder
                    .encode(body, &mut buf)
                    .expect("Failed to encode map message");

                DownlinkNotification::Event { body: buf }
            }
        };
        assert!(writer.send(raw).await.is_ok());
    }

    async fn send_corrupted_frame(&mut self) {
        let TestMapWriter(writer) = self;
        let mut buf = BytesMut::default();
        buf.put_u64(std::mem::size_of::<u8>() as u64);
        buf.put_u8(5); // unknown tag

        let bad = DownlinkNotification::Event { body: buf };

        assert!(writer.send(bad).await.is_ok());
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TestMessage<K, V> {
    Linked,
    Synced(BTreeMap<K, V>),
    Event(MapMessage<K, V>),
    Unlinked,
}

fn make_lifecycle<K, V>(
    tx: mpsc::UnboundedSender<TestMessage<K, V>>,
) -> impl MapDownlinkLifecycle<K, V>
where
    K: Ord + Clone + Form + Send + Sync + Eq + Hash + 'static,
    V: Clone + Form + Send + Sync + 'static,
{
    BasicMapDownlinkLifecycle::<K, V>::default()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(TestMessage::Linked).is_ok());
        })
        .on_synced_blocking(|tx, map| {
            assert!(tx.send(TestMessage::Synced(map.clone())).is_ok());
        })
        .on_update_blocking(|tx, key, _, _, new_value| {
            let value: V = new_value.clone();
            assert!(tx
                .send(TestMessage::Event(MapMessage::Update { key, value }))
                .is_ok());
        })
        .on_removed_blocking(|tx, key, _, _| {
            assert!(tx
                .send(TestMessage::Event(MapMessage::Remove { key }))
                .is_ok());
        })
        .on_clear_blocking(|tx, _| {
            assert!(tx.send(TestMessage::Event(MapMessage::Clear)).is_ok());
        })
        .on_unlink_blocking(|tx| {
            assert!(tx.send(TestMessage::Unlinked).is_ok());
        })
}

async fn expect_event<K, V>(
    event_rx: &mut mpsc::UnboundedReceiver<TestMessage<K, V>>,
    expected: TestMessage<K, V>,
) where
    K: Eq + std::fmt::Debug,
    V: Eq + std::fmt::Debug,
{
    assert_eq!(event_rx.recv().await, Some(expected))
}

const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

#[tokio::test]
async fn link_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let _handle = MapDownlinkHandle::new(set_tx);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn invalid_sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let _handle = MapDownlinkHandle::new(set_tx);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;

            let expected = BTreeMap::from([(1, 1)]);
            expect_event(&mut event_rx, TestMessage::Synced(expected.clone())).await;

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 2, value: 2 },
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update { key: 1, value: 1 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update { key: 2, value: 2 }),
            )
            .await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let _handle = MapDownlinkHandle::new(set_tx);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 2, value: 2 },
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(1, 1)]))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update { key: 2, value: 2 }),
            )
            .await;

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn terminate_after_unlinked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Unlinked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(1, 1)]))).await;
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
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer
                .send_message::<i32, String>(DownlinkNotification::Linked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            writer.send_corrupted_frame().await;
            (writer, reader, event_rx)
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(DownlinkTaskError::BadFrame(FrameIoError::BadFrame(
            InvalidFrame::InvalidHeader { .. }
        )))
    ));
}

#[tokio::test]
async fn unlink_discards_value() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Unlinked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(1, 1)]))).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn relink_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Unlinked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 2, value: 2 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(1, 1)]))).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(2, 2)]))).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn send_on_downlink() {
    let (event_tx, _event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |writer, mut reader| async move {
            let _writer = writer;
            assert!(set_tx
                .send(MapMessage::Update { key: 1, value: 1 })
                .await
                .is_ok());
            assert_eq!(
                reader.recv::<MapMessage<i32, i32>>().await,
                Ok(Some(MapMessage::Update { key: 1, value: 1 }))
            );
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn clear_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::from([(1, 1)]))).await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn empty_sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(BTreeMap::new())).await;
            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn rx_take_elem_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;

            for i in 0..5 {
                writer
                    .send_message::<i32, i32>(DownlinkNotification::Event {
                        body: MapMessage::Update { key: i, value: i },
                    })
                    .await;
            }

            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;

            let state = (0..5).map(|i| (i, i)).collect::<BTreeMap<i32, i32>>();
            expect_event(&mut event_rx, TestMessage::Synced(state)).await;

            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Take(2),
                })
                .await;

            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 2 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 3 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 4 }),
            )
            .await;

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn handle_take_elem_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let handle = MapDownlinkHandle::new(set_tx);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, mut reader| async move {
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;

            for i in 0..5 {
                writer
                    .send_message::<i32, i32>(DownlinkNotification::Event {
                        body: MapMessage::Update { key: i, value: i },
                    })
                    .await;
            }

            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;

            let state = (0..5).map(|i| (i, i)).collect::<BTreeMap<i32, i32>>();
            expect_event(&mut event_rx, TestMessage::Synced(state)).await;

            assert!(handle.take(2).await.is_ok());
            assert_eq!(
                reader.recv::<MapMessage<i32, i32>>().await,
                Ok(Some(MapMessage::Take(2)))
            );

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn rx_drop_elem_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;

            for i in 0..5 {
                writer
                    .send_message::<i32, i32>(DownlinkNotification::Event {
                        body: MapMessage::Update { key: i, value: i },
                    })
                    .await;
            }

            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;

            let state = (0..5).map(|i| (i, i)).collect::<BTreeMap<i32, i32>>();
            expect_event(&mut event_rx, TestMessage::Synced(state)).await;

            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Drop(2),
                })
                .await;

            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 0 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 1 }),
            )
            .await;

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn handle_drop_elem_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let handle = MapDownlinkHandle::new(set_tx);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, mut reader| async move {
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;

            for i in 0..5 {
                writer
                    .send_message::<i32, i32>(DownlinkNotification::Event {
                        body: MapMessage::Update { key: i, value: i },
                    })
                    .await;
            }

            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;

            let state = (0..5).map(|i| (i, i)).collect::<BTreeMap<i32, i32>>();
            expect_event(&mut event_rx, TestMessage::Synced(state)).await;

            assert!(handle.drop(2).await.is_ok());

            assert_eq!(
                reader.recv::<MapMessage<i32, i32>>().await,
                Ok(Some(MapMessage::Drop(2)))
            );

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn remove_elem_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
        buffer_size: DEFAULT_BUFFER_SIZE,
    };

    let result = run_map_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Linked)
                .await;

            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 1, value: 1 },
                })
                .await;
            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Update { key: 2, value: 2 },
                })
                .await;

            writer
                .send_message::<i32, i32>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Synced(BTreeMap::from([(1, 1), (2, 2)])),
            )
            .await;

            writer
                .send_message::<i32, i32>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 1 },
                })
                .await;

            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 1 }),
            )
            .await;

            event_rx
        },
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}
