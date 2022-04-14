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

use im::OrdMap;
use swim_api::{
    downlink::DownlinkConfig,
    error::{DownlinkTaskError, FrameIoError, InvalidFrame},
    protocol::downlink::DownlinkNotification,
};

use swim_api::protocol::map::{MapMessage, MapMessageEncoder, MapOperationEncoder};
use tokio::sync::mpsc;

use super::run_downlink_task;
use crate::model::lifecycle::{for_map_downlink, MapDownlinkLifecycle};
use crate::{DownlinkTask, MapDownlinkModel};

#[derive(Debug, PartialEq, Eq)]
enum TestMessage<K: Ord, V: Ord> {
    Linked,
    Synced(OrdMap<K, V>),
    Event(MapMessage<K, V>),
    Update(K, Option<V>, V),
    Remove(K, Option<V>),
    Clear(OrdMap<K, V>),
    Unlinked,
}

fn make_lifecycle<K, V>(
    tx: mpsc::UnboundedSender<TestMessage<K, V>>,
) -> impl MapDownlinkLifecycle<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Ord + Clone + Send + Sync + 'static,
{
    for_map_downlink::<K, V>()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(TestMessage::Linked).is_ok());
        })
        .on_synced_blocking(|tx, map| {
            assert!(tx.send(TestMessage::Synced(map.clone())).is_ok());
        })
        .on_event_blocking(|tx, event| {
            assert!(tx.send(TestMessage::Event(event.clone())).is_ok());
        })
        .on_updated_blocking(|tx, key, before, after| {
            assert!(tx
                .send(TestMessage::Update(
                    key.clone(),
                    before.cloned(),
                    after.clone(),
                ))
                .is_ok());
        })
        .on_removed_blocking(|tx, key, before| {
            assert!(tx
                .send(TestMessage::Remove(key.clone(), before.cloned()))
                .is_ok());
        })
        .on_cleared_blocking(|tx, map| {
            assert!(tx.send(TestMessage::Clear(map.clone())).is_ok());
        })
        .on_unlinked_blocking(|tx| {
            assert!(tx.send(TestMessage::Unlinked).is_ok());
        })
}

async fn expect_event<K, V>(
    event_rx: &mut mpsc::UnboundedReceiver<TestMessage<K, V>>,
    expected: TestMessage<K, V>,
) where
    K: Ord + Eq + std::fmt::Debug,
    V: Ord + Eq + std::fmt::Debug,
{
    assert_eq!(event_rx.recv().await, Some(expected))
}

#[tokio::test]
async fn link_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn invalid_sync_downlink() {
    let (event_tx, _event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::SyncedBeforeLinked)));
}

#[tokio::test]
async fn sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(map)).await;
            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_update_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "biscuits".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "biscuits".to_string(),
                    },
                })
                .await;

            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, Some("milk".to_string()), "biscuits".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(map)).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_update_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let mut first_map = OrdMap::new();
    first_map.insert(10, "milk".to_string());

    let mut second_map = OrdMap::new();
    second_map.insert(10, "biscuits".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(first_map)).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, Some("milk".to_string()), "biscuits".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(second_map)).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_remove_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 15 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 25 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 15 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 25 }),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Remove(25, None)).await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(map)).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_remove_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 15 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 25 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 15 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Remove { key: 25 },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(map.clone())).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 15 }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Remove { key: 25 }),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Remove(25, None)).await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(map)).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_clear_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());
    map.insert(15, "biscuits".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;

            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(map)).await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_clear_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());
    map.insert(15, "tea".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(OrdMap::new())).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "tea".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "tea".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(map)).await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_take_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 20,
                    value: "tea".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(20, None, "tea".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Take(2))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(10, Some("milk".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Take(3))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(20, Some("tea".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_take_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Take(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(OrdMap::new())).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 20,
                    value: "tea".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(20, None, "tea".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Take(2))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(10, Some("milk".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Take(3))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(20, Some("tea".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_drop_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 20,
                    value: "tea".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(20, None, "tea".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Drop(2))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(20, Some("tea".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Drop(3))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(10, Some("milk".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn report_drop_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 20,
                        value: "tea".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(2),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Drop(3),
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Clear,
                })
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(OrdMap::new())).await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(10, None, "milk".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 15,
                    value: "biscuits".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(15, None, "biscuits".to_string()),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Event(MapMessage::Update {
                    key: 20,
                    value: "tea".to_string(),
                }),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Update(20, None, "tea".to_string()),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Drop(2))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(20, Some("tea".to_string())),
            )
            .await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(15, Some("biscuits".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Drop(3))).await;
            expect_event(
                &mut event_rx,
                TestMessage::Remove(10, Some("milk".to_string())),
            )
            .await;
            expect_event(&mut event_rx, TestMessage::Event(MapMessage::Clear)).await;
            expect_event(&mut event_rx, TestMessage::Clear(OrdMap::new())).await;

            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn terminate_after_unlinked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Unlinked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(map)).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            (writer, reader, event_rx)
        },
        MapMessageEncoder::new(MapOperationEncoder),
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
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            writer.send_corrupted_frame().await;
            (writer, reader, event_rx)
        },
        MapMessageEncoder::new(MapOperationEncoder),
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
async fn unlink_discards_value() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
    };

    let mut map = OrdMap::new();
    map.insert(10, "milk".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Unlinked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(map)).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::SyncedBeforeLinked)));
}

#[tokio::test]
async fn relink_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (_command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
    };

    let mut first_map = OrdMap::new();
    first_map.insert(10, "milk".to_string());

    let mut second_map = OrdMap::new();
    second_map.insert(15, "biscuits".to_string());

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 10,
                        value: "milk".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Unlinked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Linked)
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Event {
                    body: MapMessage::Update {
                        key: 15,
                        value: "biscuits".to_string(),
                    },
                })
                .await;
            writer
                .send_value::<i32, String>(DownlinkNotification::Synced)
                .await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(first_map)).await;
            expect_event(&mut event_rx, TestMessage::Unlinked).await;
            expect_event(&mut event_rx, TestMessage::Linked).await;
            expect_event(&mut event_rx, TestMessage::Synced(second_map)).await;
            event_rx
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
    assert!(result.unwrap().recv().await.is_none());
}

#[tokio::test]
async fn send_on_downlink() {
    let (event_tx, _event_rx) = mpsc::unbounded_channel::<TestMessage<i32, String>>();
    let (command_tx, command_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = MapDownlinkModel::new(command_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |writer, mut reader| async move {
            let _writer = writer;
            assert!(command_tx
                .send(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                })
                .await
                .is_ok());
            assert_eq!(
                reader.recv::<MapMessage<i32, String>>().await,
                Ok(Some(MapMessage::Update {
                    key: 10,
                    value: "milk".to_string(),
                }))
            );
        },
        MapMessageEncoder::new(MapOperationEncoder),
    )
    .await;
    assert!(result.is_ok());
}
