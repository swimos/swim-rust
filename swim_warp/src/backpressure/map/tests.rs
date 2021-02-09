// Copyright 2015-2021 SWIM.AI inc.
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

use tokio::sync::mpsc;

use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use swim_common::model::Value;
use swim_common::sink::item::for_mpsc_sender;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(30);
const SHORT_TIMEOUT: Duration = Duration::from_secs(5);

type Modification = MapUpdate<Value, Value>;

fn update(key: i32, value: i32) -> Modification {
    MapUpdate::Update(Value::Int32Value(key), Arc::new(Value::Int32Value(value)))
}

fn remove(key: i32) -> Modification {
    MapUpdate::Remove(Value::Int32Value(key))
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
            MapUpdate::Update(k, v) => {
                map.insert(k, (*v).clone());
            }
            MapUpdate::Remove(k) => {
                map.remove(&k);
            }
            MapUpdate::Take(n) => {
                map = map.into_iter().take(n).collect();
            }
            MapUpdate::Drop(n) => {
                map = map.into_iter().skip(n).collect();
            }
            MapUpdate::Clear => {
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

#[tokio::test(flavor = "multi_thread")]
async fn single_pass_through() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, mut rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let receiver = tokio::task::spawn(async move { rx_out.recv().await });

    let result = tx_in.send(update(1, 5)).await;
    assert!(result.is_ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert!(output.is_ok());

    assert_eq!(output.unwrap(), Some(update(1, 5)));

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_one_key() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let modifications = vec![update(1, 5), remove(1), update(1, 8)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 8);

    let receiver = tokio::task::spawn(validate_receive(rx_out, expected));

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert!(output.is_ok());
    assert!(output.unwrap().is_ok());

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_keys() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let modifications = vec![update(1, 5), update(2, 8)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 5);
    expected.insert(2, 8);

    let receiver = tokio::task::spawn(validate_receive(rx_out, expected));

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert!(output.is_ok());

    assert!(output.unwrap().is_ok());

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_keys_multiple_values() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let modifications = vec![update(1, 5), update(2, 8), update(1, 22), remove(2)];

    let mut expected = BTreeMap::new();
    expected.insert(1, 22);

    let receiver = tokio::task::spawn(validate_receive(rx_out, expected));

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert!(output.is_ok());
    assert!(output.unwrap().is_ok());

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn single_clear() {
    let (tx_in, rx_in) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let (tx_out, mut rx_out) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let receiver = tokio::task::spawn(async move { rx_out.recv().await });

    let result = tx_in.send(MapUpdate::Clear).await;
    assert!(result.is_ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert!(output.is_ok());

    assert_eq!(output.unwrap(), Some(MapUpdate::Clear));

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn single_take() {
    let (tx_in, rx_in) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let (tx_out, mut rx_out) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let receiver = tokio::task::spawn(async move { rx_out.recv().await });

    let result = tx_in.send(MapUpdate::Take(4)).await;
    assert!(result.is_ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert!(output.is_ok());

    assert_eq!(output.unwrap(), Some(MapUpdate::Take(4)));
    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn single_skip() {
    let (tx_in, rx_in) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let (tx_out, mut rx_out) = mpsc::channel::<MapUpdate<Value, Value>>(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let receiver = tokio::task::spawn(async move { rx_out.recv().await });

    let result = tx_in.send(MapUpdate::Drop(4)).await;
    assert!(result.is_ok());

    let output = timeout(TIMEOUT, receiver).await.unwrap();
    assert!(output.is_ok());

    assert_eq!(output.unwrap(), Some(MapUpdate::Drop(4)));

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn special_action_ordering() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let modifications = vec![
        update(1, 5),
        update(2, 8),
        update(3, 21),
        MapUpdate::Drop(2),
        update(1, 42),
    ];

    let mut expected = BTreeMap::new();
    expected.insert(1, 42);
    expected.insert(3, 21);

    let receiver = tokio::task::spawn(validate_receive(rx_out, expected));

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert!(output.is_ok());

    assert!(output.unwrap().is_ok());

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn overflow_active_keys() {
    let (tx_in, rx_in) = mpsc::channel(8);
    let (tx_out, rx_out) = mpsc::channel(8);
    let release_task = super::release_pressure(
        rx_in,
        for_mpsc_sender(tx_out),
        yield_after(),
        buffer_size(),
        max_active_keys(),
        buffer_size(),
    );

    let release_result = tokio::task::spawn(release_task);

    let mut modifications = (1..5).into_iter().map(|i| update(i, i)).collect::<Vec<_>>();

    modifications.push(update(1, -1));

    let mut expected = BTreeMap::new();
    expected.insert(1, -1);
    for i in 2..5 {
        expected.insert(i, i);
    }

    let receiver = tokio::task::spawn(validate_receive(rx_out, expected));

    for m in modifications.into_iter() {
        let result = tx_in.send(m).await;
        assert!(result.is_ok());
    }

    let output = timeout(TIMEOUT, receiver).await.unwrap();

    assert!(output.is_ok());

    assert!(output.unwrap().is_ok());

    drop(tx_in);
    assert!(matches!(release_result.await, Ok(Ok(_))));
}
