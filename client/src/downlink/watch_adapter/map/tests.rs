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
use tokio::time::timeout;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);
const SHORT_TIMEOUT: Duration = Duration::from_secs(5);

async fn validate_receive(mut rx: mpsc::Receiver<MapModification<Arc<Value>>>,
    end_state: BTreeMap<Value, Value>) -> Result<(), BTreeMap<Value, Value>> {

    let mut map = BTreeMap::new();
    while let Ok(Some(modification)) =
    timeout(SHORT_TIMEOUT, rx.recv()).await {
        match modification {
            MapModification::Insert(k, v) => {
                map.insert(k, (*v).clone());
            }
            MapModification::Remove(k) => {
                map.remove(&k);
            }
            MapModification::Take(n) => {
                map = map.into_iter().take(n).collect();
            }
            MapModification::Skip(n) => {
                map = map.into_iter().skip(n).collect();
            }
            MapModification::Clear => {
                map.clear();
            }
        }
        if map == end_state {
            return Ok(());
        }
    }
    return Err(map);
}

#[tokio::test(threaded_scheduler)]
async fn single_pass_through() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let key = Value::Int32Value(1);
    let value = Arc::new(Value::Int32Value(5));

    let receiver = tokio::task::spawn(async move {
        rx.recv().await
    });

    let result = watcher.send_item(MapModification::Insert(key.clone(), value.clone())).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(MapModification::Insert(key, value))));

}

#[tokio::test(threaded_scheduler)]
async fn multiple_one_key() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let key = Value::Int32Value(1);
    let value1 = Arc::new(Value::Int32Value(5));
    let value2 = Arc::new(Value::Int32Value(8));

    let modifications = vec![
        MapModification::Insert(key.clone(), value1.clone()),
        MapModification::Remove(key.clone()),
        MapModification::Insert(key.clone(), value2.clone()),
    ];

    let mut expected = BTreeMap::new();
    expected.insert(key, (*value2).clone());

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

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let key1 = Value::Int32Value(1);
    let key2 = Value::Int32Value(2);
    let value1 = Arc::new(Value::Int32Value(5));
    let value2 = Arc::new(Value::Int32Value(8));

    let modifications = vec![
        MapModification::Insert(key1.clone(), value1.clone()),
        MapModification::Insert(key2.clone(), value2.clone()),
    ];

    let mut expected = BTreeMap::new();
    expected.insert(key1, (*value1).clone());
    expected.insert(key2, (*value2).clone());

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

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let key1 = Value::Int32Value(1);
    let key2 = Value::Int32Value(2);
    let value1 = Arc::new(Value::Int32Value(5));
    let value2 = Arc::new(Value::Int32Value(8));
    let value3 = Arc::new(Value::Int32Value(22));

    let modifications = vec![
        MapModification::Insert(key1.clone(), value1.clone()),
        MapModification::Insert(key2.clone(), value2.clone()),
        MapModification::Insert(key1.clone(), value3.clone()),
        MapModification::Remove(key2.clone())
    ];

    let mut expected = BTreeMap::new();
    expected.insert(key1, (*value3).clone());

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

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let receiver = tokio::task::spawn(async move {
        rx.recv().await
    });

    let result = watcher.send_item(MapModification::Clear).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(MapModification::Clear)));

}

#[tokio::test(threaded_scheduler)]
async fn single_take() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let receiver = tokio::task::spawn(async move {
        rx.recv().await
    });

    let result = watcher.send_item(MapModification::Take(4)).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(MapModification::Take(4))));

}

#[tokio::test(threaded_scheduler)]
async fn single_skip() {
    let (tx, mut rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let receiver = tokio::task::spawn(async move {
        rx.recv().await
    });

    let result = watcher.send_item(MapModification::Skip(4)).await;
    assert_that!(result, ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert_that!(&output, ok());

    assert_that!(output.unwrap(), eq(Some(MapModification::Skip(4))));

}

#[tokio::test(threaded_scheduler)]
async fn special_action_ordering() {
    let (tx, rx) = mpsc::channel(5);

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 5).await;

    let key1 = Value::Int32Value(1);
    let key2 = Value::Int32Value(2);
    let key3 = Value::Int32Value(3);
    let value1 = Arc::new(Value::Int32Value(5));
    let value2 = Arc::new(Value::Int32Value(8));
    let value3 = Arc::new(Value::Int32Value(21));
    let value4 = Arc::new(Value::Int32Value(42));

    let modifications = vec![
        MapModification::Insert(key1.clone(), value1.clone()),
        MapModification::Insert(key2.clone(), value2.clone()),
        MapModification::Insert(key3.clone(), value3.clone()),
        MapModification::Skip(2),
        MapModification::Insert(key1.clone(), value4.clone()),
    ];

    let mut expected = BTreeMap::new();
    expected.insert(key1, (*value4).clone());
    expected.insert(key3, (*value3).clone());

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

    let mut watcher = KeyedWatch::new(tx.map_err_into(), 5, 5, 2).await;

    let mut modifications = (1..5).into_iter()
        .map(|i| MapModification::Insert(Value::Int32Value(i), Arc::new(Value::Int32Value(i))))
        .collect::<Vec<_>>();

    modifications.push(MapModification::Insert(Value::Int32Value(1), Arc::new(Value::Int32Value(-1))));

    let mut expected = BTreeMap::new();
    expected.insert(Value::Int32Value(1), Value::Int32Value(-1));
    for i in 2..5 {
        expected.insert(Value::Int32Value(i), Value::Int32Value(i));
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