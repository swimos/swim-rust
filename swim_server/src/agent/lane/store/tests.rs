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

use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler};
use crate::agent::lane::store::task::{NodeStoreErrors, NodeStoreTask};
use crate::agent::lane::store::StoreIo;
use crate::agent::store::mock::MockNodeStore;
use futures::future::pending;
use futures::future::BoxFuture;
use futures::Future;
use std::collections::HashMap;
use store::{StoreError, StoreInfo};
use swim_common::model::time::Timestamp;
use swim_runtime::time::timeout::timeout;
use tokio::time::{sleep, Duration};
use utilities::sync::trigger;

fn info() -> StoreInfo {
    StoreInfo {
        path: "/tmp/rocks".to_string(),
        kind: "Rocks DB".to_string(),
    }
}

pub fn store_err_partial_eq(expected: Vec<StoreError>, actual: Vec<(Timestamp, StoreError)>) {
    let mut expected_iter = expected.into_iter();
    let mut actual_iter = actual.into_iter();

    while let Some(expected) = expected_iter.next() {
        match actual_iter.next() {
            Some((_timestamp, actual)) => {
                assert_eq!(expected, actual);
            }
            None => {
                panic!("Expected: {:?}", expected);
            }
        }
    }
}

#[test]
fn error_collector_0() {
    let mut handler = StoreErrorHandler::new(0, info());
    let error = StoreError::KeyNotFound;
    let result = handler.on_error(error);

    assert!(result.is_err());

    match result {
        Ok(_) => panic!("Expected an error"),
        Err(e) => {
            assert_eq!(info(), e.store_info);
            store_err_partial_eq(vec![StoreError::KeyNotFound], e.errors);
        }
    }
}

#[test]
fn capped_error_collector() {
    let mut handler = StoreErrorHandler::new(4, info());

    assert!(handler
        .on_error(StoreError::Encoding("Failed to encode".to_string()))
        .is_ok());
    assert!(handler.on_error(StoreError::KeyNotFound).is_ok());
    assert!(handler
        .on_error(StoreError::Snapshot("Snapshot err".to_string()))
        .is_ok());

    let result = handler.on_error(StoreError::Decoding("Failed to decode".to_string()));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(info(), err.store_info);

    let expected_errors = vec![
        StoreError::Encoding("Failed to encode".to_string()),
        StoreError::KeyNotFound,
        StoreError::Snapshot("Snapshot err".to_string()),
        StoreError::Decoding("Failed to decode".to_string()),
    ];

    store_err_partial_eq(expected_errors, err.errors);
}

#[test]
fn err_operational() {
    let mut handler = StoreErrorHandler::new(4, info());
    assert!(handler
        .on_error(StoreError::Encoding("Failed to encode".to_string()))
        .is_ok());

    let result = handler.on_error(StoreError::Closing);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(info(), err.store_info);

    store_err_partial_eq(
        vec![
            StoreError::Encoding("Failed to encode".to_string()),
            StoreError::Closing,
        ],
        err.errors,
    );
}

struct TestStoreIo<T>(T);
impl<T> StoreIo<MockNodeStore> for TestStoreIo<T>
where
    T: Future<Output = Result<(), LaneStoreErrorReport>> + Send + 'static,
{
    fn attach(
        self,
        _store: MockNodeStore,
        _lane_uri: String,
        _error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move { self.0.await })
    }

    fn attach_boxed(
        self: Box<Self>,
        store: MockNodeStore,
        lane_uri: String,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(store, lane_uri, error_handler)
    }
}

#[tokio::test]
async fn task_ok() {
    let test_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(async { Ok(()) }));
    let (_trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore);
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let result = store_task.run(tasks, 0).await;
    assert!(result.is_ok())
}

fn store_info() -> StoreInfo {
    StoreInfo {
        path: "Mock".to_string(),
        kind: "Mock".to_string(),
    }
}

#[tokio::test]
async fn task_err() {
    let test_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(async {
        Err(LaneStoreErrorReport::for_error(
            store_info(),
            StoreError::KeyNotFound,
        ))
    }));
    let (_trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore);
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let result = store_task.run(tasks, 0).await;
    assert!(result.is_err());
    let NodeStoreErrors { failed, mut errors } = result.unwrap_err();
    assert!(failed);
    assert_eq!(errors.len(), 1);
    store_err_partial_eq(
        vec![StoreError::KeyNotFound],
        errors.pop().unwrap().1.errors,
    );
}

#[tokio::test]
async fn triggers() {
    let test_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(pending()));
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore);
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let future = tokio::spawn(store_task.run(tasks, 0));
    assert!(trigger_tx.trigger());

    let result = timeout(Duration::from_secs(5), future).await;
    match result {
        Ok(Ok(Ok(report))) => {
            assert!(!report.failed);
            assert_eq!(report.errors.len(), 0);
        }
        _ => panic!("Task finished with an incorrect result"),
    }
}

#[tokio::test]
async fn multiple_ios() {
    let failing_io_uri = "err_io".to_string();
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore);

    let mut tasks = HashMap::new();

    let pending_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(pending()));
    tasks.insert("pending_io".to_string(), pending_io);

    let ok_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(async { Ok(()) }));
    tasks.insert("ok_io".to_string(), ok_io);

    let err_io: Box<dyn StoreIo<MockNodeStore>> = Box::new(TestStoreIo(async {
        Err(LaneStoreErrorReport::for_error(
            store_info(),
            StoreError::KeyNotFound,
        ))
    }));
    tasks.insert(failing_io_uri.clone(), err_io);

    let future = tokio::spawn(store_task.run(tasks, 0));
    sleep(Duration::from_secs(5)).await;
    assert!(trigger_tx.trigger());

    let result = timeout(Duration::from_secs(5), future).await;
    match result {
        Ok(Ok(Err(mut report))) => {
            assert!(report.failed);
            assert_eq!(report.errors.len(), 1);

            let (lane_uri, errors) = report.errors.pop().unwrap();
            assert_eq!(failing_io_uri, lane_uri);

            store_err_partial_eq(vec![StoreError::KeyNotFound], errors.errors)
        }
        _ => panic!("Task finished with an incorrect result"),
    }
}
