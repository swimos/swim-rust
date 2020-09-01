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

use crate::agent::dispatch::selector::Selector;
use tokio::sync::mpsc;
use swim_runtime::time::timeout::timeout;
use std::time::Duration;

#[tokio::test]
async fn empty_selector() {

    let mut selector: Selector<i32, String> = Selector::default();

    assert!(selector.is_empty());
    assert!(selector.select().await.is_none());

}

#[tokio::test]
async fn selector_add_sender() {
    let mut selector: Selector<i32, String> = Selector::default();

    let (tx, _rx) = mpsc::channel(1);

    selector.add("a".to_string(), tx);

    assert!(!selector.is_empty());

}

#[tokio::test]
async fn selector_select_available() {

    let mut selector: Selector<i32, String> = Selector::default();

    let (tx, mut rx) = mpsc::channel(1);

    selector.add("a".to_string(), tx);

    let selected = selector.select().await;

    assert!(selector.is_empty());

    assert!(selected.is_some());
    let (label, result) = selected.unwrap();
    assert_eq!(label, "a");
    assert!(result.is_ok());
    let mut tx = result.unwrap();

    assert!(tx.send(4).await.is_ok());

    assert_eq!(rx.recv().await, Some(4));

}

#[tokio::test]
async fn selector_correct() {

    let mut selector: Selector<i32, String> = Selector::default();

    let (mut tx1, _rx1) = mpsc::channel(1);
    let (tx2, mut rx2) = mpsc::channel(1);

    selector.add("a".to_string(), tx1.clone());
    selector.add("b".to_string(), tx2);

    assert!(tx1.send(0).await.is_ok());

    let selected = selector.select().await;

    assert!(!selector.is_empty());

    assert!(selected.is_some());
    let (label, result) = selected.unwrap();
    assert_eq!(label, "b");
    assert!(result.is_ok());
    let mut tx = result.unwrap();

    assert!(tx.send(4).await.is_ok());

    assert_eq!(rx2.recv().await, Some(4));
}

#[tokio::test]
async fn selector_select_dropped() {

    let mut selector: Selector<i32, String> = Selector::default();

    let (tx, rx) = mpsc::channel(1);

    selector.add("a".to_string(), tx);

    drop(rx);

    assert!(!selector.is_empty());

    let selected = selector.select().await;

    assert!(selector.is_empty());

    assert!(selected.is_some());
    let (label, result) = selected.unwrap();
    assert_eq!(label, "a");
    assert!(result.is_err());

}

#[tokio::test]
async fn selector_none_available() {

    let mut selector: Selector<i32, String> = Selector::default();

    let (mut tx1, _rx1) = mpsc::channel(1);
    let (mut tx2, _rx2) = mpsc::channel(1);

    selector.add("a".to_string(), tx1.clone());
    selector.add("b".to_string(), tx2.clone());

    assert!(tx1.send(0).await.is_ok());
    assert!(tx2.send(0).await.is_ok());

    let result = timeout(Duration::from_millis(5), selector.select()).await;

    assert!(!selector.is_empty());
    assert!(result.is_err());

}

#[tokio::test]
async fn selector_after_failure() {

    let mut selector: Selector<i32, String> = Selector::default();

    let (mut tx1, mut rx1) = mpsc::channel(1);
    let (mut tx2, _rx2) = mpsc::channel(1);

    selector.add("a".to_string(), tx1.clone());
    selector.add("b".to_string(), tx2.clone());

    assert!(tx1.send(0).await.is_ok());
    assert!(tx2.send(0).await.is_ok());

    let result = timeout(Duration::from_millis(5), selector.select()).await;

    assert!(!selector.is_empty());
    assert!(result.is_err());

    assert_eq!(rx1.recv().await, Some(0));

    let selected = selector.select().await;

    assert!(selected.is_some());
    let (label, result) = selected.unwrap();
    assert_eq!(label, "a");
    assert!(result.is_ok());
    let mut tx = result.unwrap();

    assert!(tx.send(7).await.is_ok());

    assert_eq!(rx1.recv().await, Some(7));
}