// Copyright 2015-2020 SWIM.AI inc.
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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use tokio::sync::mpsc;

use super::*;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(30);
const SHORT_TIMEOUT: Duration = Duration::from_secs(5);

type Modification = UntypedMapModification<Arc<Value>>;

fn insert(key: i32, value: i32) -> Modification {
    UntypedMapModification::Insert(Value::Int32Value(key), Arc::new(Value::Int32Value(value)))
}

fn remove(key: i32) -> Modification {
    UntypedMapModification::Remove(Value::Int32Value(key))
}

async fn validate_receive(
    mut rx: mpsc::Receiver<Modification>,
    end_state: BTreeMap<i32, i32>,
) -> Result<(), BTreeMap<Value, Value>> {
    let mut expected = BTreeMap::new();
    for (k, v) in end_state.iter() {
        expected.insert(Value::Int32Value(*k), Value::Int32Value(*v));
    }
    let mut map = BTreeMap::new();
    while let Ok(Some(modification)) = timeout(SHORT_TIMEOUT, rx.recv()).await {
        match modification {
            UntypedMapModification::Insert(k, v) => {
                map.insert(k, (*v).clone());
            }
            UntypedMapModification::Remove(k) => {
                map.remove(&k);
            }
            UntypedMapModification::Take(n) => {
                map = map.into_iter().take(n).collect();
            }
            UntypedMapModification::Skip(n) => {
                map = map.into_iter().skip(n).collect();
            }
            UntypedMapModification::Clear => {
                map.clear();
            }
        }
        if map == expected {
            return Ok(());
        }
    }
    return Err(map);
}

fn buffer_size() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn max_active_keys() -> NonZeroUsize {
    NonZeroUsize::new(5).unwrap()
}

fn yield_after() -> NonZeroUsize {
    NonZeroUsize::new(256).unwrap()
}

#[tokio::test(threaded_scheduler)]
async fn single_pass_through() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let receiver = tokio::task::spawn(async move { rx.recv().await });

    let result = watcher.send_item(insert(1, 5)).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(insert(1, 5))));
}

#[tokio::test(threaded_scheduler)]
async fn multiple_one_key() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let modifications = vec![insert(1, 5), remove(1), insert(1, 8)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 8);

    let receiver = tokio::task::spawn(validate_receive(rx, expected));

    for m in modifications.into_iter() {
        let result = watcher.send_item(m).await;
        assert_that!(result, ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert_that!(&output, ok());

    assert_that!(output.unwrap(), ok());
}

#[tokio::test(threaded_scheduler)]
async fn multiple_keys() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let modifications = vec![insert(1, 5), insert(2, 8)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 5);
    expected.insert(2, 8);

    let receiver = tokio::task::spawn(validate_receive(rx, expected));

    for m in modifications.into_iter() {
        let result = watcher.send_item(m).await;
        assert_that!(result, ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert_that!(&output, ok());

    assert_that!(output.unwrap(), ok());
}

#[tokio::test(threaded_scheduler)]
async fn multiple_keys_multiple_values() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let modifications = vec![insert(1, 5), insert(2, 8), insert(1, 22), remove(2)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 22);

    let receiver = tokio::task::spawn(validate_receive(rx, expected));

    for m in modifications.into_iter() {
        let result = watcher.send_item(m).await;
        assert_that!(result, ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert_that!(&output, ok());

    assert_that!(output.unwrap(), ok());
}

#[tokio::test(threaded_scheduler)]
async fn single_clear() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let receiver = tokio::task::spawn(async move { rx.recv().await });

    let result = watcher.send_item(UntypedMapModification::Clear).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(UntypedMapModification::Clear)));
}

#[tokio::test(threaded_scheduler)]
async fn single_take() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let receiver = tokio::task::spawn(async move { rx.recv().await });

    let result = watcher.send_item(UntypedMapModification::Take(4)).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(UntypedMapModification::Take(4))));
}

#[tokio::test(threaded_scheduler)]
async fn single_skip() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let receiver = tokio::task::spawn(async move { rx.recv().await });

    let result = watcher.send_item(UntypedMapModification::Skip(4)).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(UntypedMapModification::Skip(4))));
}

#[tokio::test(threaded_scheduler)]
async fn special_action_ordering() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let modifications = vec![
        insert(1, 5),
        insert(2, 8),
        insert(3, 21),
        UntypedMapModification::Skip(2),
        insert(1, 42),
    ];

    let mut expected = BTreeMap::new();
    expected.insert(1, 42);
    expected.insert(3, 21);

    let receiver = tokio::task::spawn(validate_receive(rx, expected));

    for m in modifications.into_iter() {
        let result = watcher.send_item(m).await;
        assert_that!(result, ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert_that!(&output, ok());

    assert_that!(output.unwrap(), ok());
}

#[tokio::test(threaded_scheduler)]
async fn overflow_active_keys() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(
        tx.map_err_into(),
        buffer_size(),
        buffer_size(),
        max_active_keys(),
        yield_after(),
    )
    .await;

    let mut modifications = (1..5).into_iter().map(|i| insert(i, i)).collect::<Vec<_>>();

    modifications.push(insert(1, -1));

    let mut expected = BTreeMap::new();
    expected.insert(1, -1);
    for i in 2..5 {
        expected.insert(i, i);
    }

    let receiver = tokio::task::spawn(validate_receive(rx, expected));

    for m in modifications.into_iter() {
        let result = watcher.send_item(m).await;
        assert_that!(result, ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert_that!(&output, ok());

    assert_that!(output.unwrap(), ok());
}
