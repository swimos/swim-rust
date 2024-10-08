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

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::future::join;
use swimos_api::persistence::{NodePersistence, PlanePersistence, RangeConsumer};
use swimos_utilities::future::NotifyOnBlocked;
use tokio::sync::Notify;

use super::{InMemoryNodePersistence, InMemoryPlanePersistence};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn create_node_stores() {
    let plane = InMemoryPlanePersistence::default();

    tokio::time::timeout(TIMEOUT, async {
        let store1 = plane
            .node_store("example1")
            .await
            .expect("Failed to open first node store.");
        let store2 = plane
            .node_store("example2")
            .await
            .expect("Failed to open second node store.");
        (store1, store2)
    })
    .await
    .expect("Test timed out.");
}

#[tokio::test]
async fn wait_for_release() {
    let plane = InMemoryPlanePersistence::default();

    let store1 = tokio::time::timeout(TIMEOUT, plane.node_store("example1"))
        .await
        .expect("Test timed out.")
        .expect("Failed to open first node store.");

    let open_second = async {
        plane
            .node_store("example1")
            .await
            .expect("Failed to open second node store.")
    };

    let notify = Arc::new(Notify::new());
    let when_blocked = NotifyOnBlocked::new(open_second, notify.clone());

    let free_up_first = async move {
        notify.notified().await;
        drop(store1);
    };

    tokio::time::timeout(TIMEOUT, join(when_blocked, free_up_first))
        .await
        .expect("Test timed out.");
}

async fn make_store(plane: &InMemoryPlanePersistence) -> InMemoryNodePersistence {
    tokio::time::timeout(TIMEOUT, plane.node_store("example1"))
        .await
        .expect("Test timed out.")
        .expect("Failed to open first node store.")
}

const SHOULD_NOT_FAIL: &str = "In memory store should be infallible.";

#[tokio::test]
async fn store_and_retrieve_value() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("value").expect(SHOULD_NOT_FAIL);

    let data = [1, 2, 3];
    store.put_value(store_id, &data).expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    let read = store
        .get_value(store_id, &mut buffer)
        .expect(SHOULD_NOT_FAIL);
    assert_eq!(read, Some(data.len()));
    assert_eq!(buffer.as_ref(), data);
}

#[tokio::test]
async fn overwrite_value() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("value").expect(SHOULD_NOT_FAIL);

    let data1 = [1, 2, 3];
    let data2 = [4, 5];
    store.put_value(store_id, &data1).expect(SHOULD_NOT_FAIL);
    store.put_value(store_id, &data2).expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    let read = store
        .get_value(store_id, &mut buffer)
        .expect(SHOULD_NOT_FAIL);
    assert_eq!(read, Some(data2.len()));
    assert_eq!(buffer.as_ref(), data2);
}

#[tokio::test]
async fn remove_value() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("value").expect(SHOULD_NOT_FAIL);

    let data = [1, 2, 3];
    store.put_value(store_id, &data).expect(SHOULD_NOT_FAIL);
    store.delete_value(store_id).expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    let read = store
        .get_value(store_id, &mut buffer)
        .expect(SHOULD_NOT_FAIL);
    assert!(read.is_none());
}

#[tokio::test]
async fn get_undefined_value() {
    let plane = InMemoryPlanePersistence::default();

    let store = make_store(&plane).await;

    let store_id = store.id_for("value").expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    let read = store
        .get_value(store_id, &mut buffer)
        .expect(SHOULD_NOT_FAIL);
    assert!(read.is_none());
}

const KEY1: &[u8] = &[1];
const KEY2: &[u8] = &[2, 0];
const KEY3: &[u8] = &[3, 0, 0];

const VALUE1: &[u8] = &[1, 2, 3];
const VALUE2: &[u8] = &[4, 5];
const VALUE3: &[u8] = &[6, 7, 8, 9];

#[tokio::test]
async fn store_and_retrieve_map() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    store
        .update_map(store_id, KEY1, VALUE1)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY2, VALUE2)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY3, VALUE3)
        .expect(SHOULD_NOT_FAIL);

    let mut consumer = store.read_map(store_id).expect(SHOULD_NOT_FAIL);

    let mut expected = HashMap::new();
    expected.insert(KEY1.to_vec(), VALUE1.to_vec());
    expected.insert(KEY2.to_vec(), VALUE2.to_vec());
    expected.insert(KEY3.to_vec(), VALUE3.to_vec());
    let mut actual = HashMap::new();

    while let Some((k, v)) = consumer.consume_next().expect(SHOULD_NOT_FAIL) {
        actual.insert(k.to_vec(), v.to_vec());
    }

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn delete_from_map() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    store
        .update_map(store_id, KEY1, VALUE1)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY2, VALUE2)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY3, VALUE3)
        .expect(SHOULD_NOT_FAIL);
    store.remove_map(store_id, KEY2).expect(SHOULD_NOT_FAIL);

    let mut consumer = store.read_map(store_id).expect(SHOULD_NOT_FAIL);

    let mut expected = HashMap::new();
    expected.insert(KEY1.to_vec(), VALUE1.to_vec());
    expected.insert(KEY3.to_vec(), VALUE3.to_vec());
    let mut actual = HashMap::new();

    while let Some((k, v)) = consumer.consume_next().expect(SHOULD_NOT_FAIL) {
        actual.insert(k.to_vec(), v.to_vec());
    }

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn clear_map() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    store
        .update_map(store_id, KEY1, VALUE1)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY2, VALUE2)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY3, VALUE3)
        .expect(SHOULD_NOT_FAIL);
    store.clear_map(store_id).expect(SHOULD_NOT_FAIL);

    let mut consumer = store.read_map(store_id).expect(SHOULD_NOT_FAIL);

    let mut actual = HashMap::new();

    while let Some((k, v)) = consumer.consume_next().expect(SHOULD_NOT_FAIL) {
        actual.insert(k.to_vec(), v.to_vec());
    }

    assert!(actual.is_empty());
}

#[tokio::test]
async fn replace_map_value() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let store_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    store
        .update_map(store_id, KEY3, VALUE3)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(store_id, KEY3, VALUE1)
        .expect(SHOULD_NOT_FAIL);

    let mut consumer = store.read_map(store_id).expect(SHOULD_NOT_FAIL);

    let mut expected = HashMap::new();
    expected.insert(KEY3.to_vec(), VALUE1.to_vec());
    let mut actual = HashMap::new();

    while let Some((k, v)) = consumer.consume_next().expect(SHOULD_NOT_FAIL) {
        actual.insert(k.to_vec(), v.to_vec());
    }

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn persist_across_restart() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let value_id = store.id_for("value").expect(SHOULD_NOT_FAIL);
    let map_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    let value = [34];
    store.put_value(value_id, &value).expect(SHOULD_NOT_FAIL);
    store
        .update_map(map_id, KEY1, VALUE1)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(map_id, KEY2, VALUE2)
        .expect(SHOULD_NOT_FAIL);
    store
        .update_map(map_id, KEY3, VALUE3)
        .expect(SHOULD_NOT_FAIL);

    drop(store);

    let restored = make_store(&plane).await;

    let value_id_restored = restored.id_for("value").expect(SHOULD_NOT_FAIL);
    let map_id_restored = restored.id_for("map").expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    let read = restored
        .get_value(value_id_restored, &mut buffer)
        .expect(SHOULD_NOT_FAIL);
    assert_eq!(read, Some(value.len()));
    assert_eq!(buffer.as_ref(), value);

    let mut consumer = restored.read_map(map_id_restored).expect(SHOULD_NOT_FAIL);

    let mut expected = HashMap::new();
    expected.insert(KEY1.to_vec(), VALUE1.to_vec());
    expected.insert(KEY2.to_vec(), VALUE2.to_vec());
    expected.insert(KEY3.to_vec(), VALUE3.to_vec());
    let mut actual = HashMap::new();

    while let Some((k, v)) = consumer.consume_next().expect(SHOULD_NOT_FAIL) {
        actual.insert(k.to_vec(), v.to_vec());
    }

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn kind_collision() {
    let plane = InMemoryPlanePersistence::default();

    let mut store = make_store(&plane).await;

    let value_id = store.id_for("value").expect(SHOULD_NOT_FAIL);
    let map_id = store.id_for("map").expect(SHOULD_NOT_FAIL);

    let value = [34];
    store.put_value(value_id, &value).expect(SHOULD_NOT_FAIL);
    store
        .update_map(map_id, KEY1, VALUE1)
        .expect(SHOULD_NOT_FAIL);

    let mut buffer = BytesMut::new();
    assert!(store.get_value(map_id, &mut buffer).is_err());
    assert!(buffer.is_empty());
    assert!(store.delete_value(map_id).is_err());
    assert!(store.put_value(map_id, VALUE1).is_err());

    assert!(store.update_map(value_id, KEY1, VALUE1).is_err());
    assert!(store.remove_map(value_id, KEY1).is_err());
    assert!(store.clear_map(value_id).is_err());
    assert!(store.read_map(value_id).is_err());
}
