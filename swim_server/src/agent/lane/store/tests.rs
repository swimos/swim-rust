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

use crate::agent::lane::store::error::{LaneStoreErrorReport, StoreErrorHandler, StoreTaskError};
use crate::agent::lane::store::task::NodeStoreTask;
use crate::agent::lane::store::StoreIo;
use crate::agent::store::mock::MockNodeStore;
use futures::future::pending;
use futures::future::BoxFuture;
use futures::Future;
use std::collections::HashMap;
use store::StoreError;
use swim_async_runtime::time::timeout::timeout;
use swim_model::time::Timestamp;
use swim_utilities::trigger;
use tokio::time::{sleep, Duration};

pub fn store_err_partial_eq(expected: Vec<StoreError>, actual: Vec<StoreTaskError>) {
    let mut expected_iter = expected.into_iter();
    let mut actual_iter = actual.into_iter();

    while let Some(expected) = expected_iter.next() {
        match actual_iter.next() {
            Some(task_error) => {
                assert_eq!(expected, task_error.error);
            }
            None => {
                panic!("Expected: {:?}", expected);
            }
        }
    }
}

fn make_error(error: StoreError) -> StoreTaskError {
    StoreTaskError {
        timestamp: Timestamp::now(),
        error,
    }
}

#[test]
fn error_collector_0() {
    let mut handler = StoreErrorHandler::new(0);
    let error = StoreError::KeyNotFound;
    let result = handler.on_error(make_error(error));

    assert!(result.is_err());

    match result {
        Ok(_) => panic!("Expected an error"),
        Err(e) => {
            store_err_partial_eq(vec![StoreError::KeyNotFound], e.errors);
        }
    }
}

#[test]
fn capped_error_collector() {
    let mut handler = StoreErrorHandler::new(4);

    assert!(handler
        .on_error(make_error(StoreError::Encoding(
            "Failed to encode".to_string()
        )))
        .is_ok());
    assert!(handler
        .on_error(make_error(StoreError::KeyNotFound))
        .is_ok());
    assert!(handler
        .on_error(make_error(StoreError::DelegateMessage(
            "Snapshot err".to_string()
        )))
        .is_ok());

    let result = handler.on_error(make_error(StoreError::Decoding(
        "Failed to decode".to_string(),
    )));
    assert!(result.is_err());
    let err = result.unwrap_err();

    let expected_errors = vec![
        StoreError::Encoding("Failed to encode".to_string()),
        StoreError::KeyNotFound,
        StoreError::DelegateMessage("Snapshot err".to_string()),
        StoreError::Decoding("Failed to decode".to_string()),
    ];

    store_err_partial_eq(expected_errors, err.errors);
}

#[test]
fn err_operational() {
    let mut handler = StoreErrorHandler::new(4);
    assert!(handler
        .on_error(make_error(StoreError::Encoding(
            "Failed to encode".to_string()
        )))
        .is_ok());

    let result = handler.on_error(make_error(StoreError::Closing));
    assert!(result.is_err());
    let err = result.unwrap_err();

    store_err_partial_eq(
        vec![
            StoreError::Encoding("Failed to encode".to_string()),
            StoreError::Closing,
        ],
        err.errors,
    );
}

struct TestStoreIo<T>(T);
impl<T> StoreIo for TestStoreIo<T>
where
    T: Future<Output = Result<(), LaneStoreErrorReport>> + Send + 'static,
{
    fn attach(
        self,
        _error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        Box::pin(async move { self.0.await })
    }

    fn attach_boxed(
        self: Box<Self>,
        error_handler: StoreErrorHandler,
    ) -> BoxFuture<'static, Result<(), LaneStoreErrorReport>> {
        (*self).attach(error_handler)
    }
}

#[tokio::test]
async fn task_ok() {
    let test_io: Box<dyn StoreIo> = Box::new(TestStoreIo(async { Ok(()) }));
    let (_trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore::mock());
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let result = store_task.run(tasks, 0).await;
    assert!(result.is_ok())
}

#[tokio::test]
async fn task_err() {
    let test_io: Box<dyn StoreIo> = Box::new(TestStoreIo(async {
        Err(LaneStoreErrorReport::for_error(make_error(
            StoreError::KeyNotFound,
        )))
    }));
    let (_trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore::mock());
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let result = store_task.run(tasks, 0).await;
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert!(errors.failed());
    assert_eq!(errors.len(), 1);
    store_err_partial_eq(
        vec![StoreError::KeyNotFound],
        errors.expect_err().pop().unwrap().1.errors,
    );
}

#[tokio::test]
async fn triggers() {
    let test_io: Box<dyn StoreIo> = Box::new(TestStoreIo(pending()));
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore::mock());
    let mut tasks = HashMap::new();
    tasks.insert("lane".to_string(), test_io);

    let future = tokio::spawn(store_task.run(tasks, 0));
    assert!(trigger_tx.trigger());

    let result = timeout(Duration::from_secs(5), future).await;
    match result {
        Ok(Ok(Ok(report))) => {
            assert!(!report.failed());
            assert_eq!(report.len(), 0);
        }
        _ => panic!("Task finished with an incorrect result"),
    }
}

#[tokio::test]
async fn multiple_ios() {
    let failing_io_uri = "err_io".to_string();
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let store_task = NodeStoreTask::new(trigger_rx, MockNodeStore::mock());

    let mut tasks = HashMap::new();

    let pending_io: Box<dyn StoreIo> = Box::new(TestStoreIo(pending()));
    tasks.insert("pending_io".to_string(), pending_io);

    let ok_io: Box<dyn StoreIo> = Box::new(TestStoreIo(async { Ok(()) }));
    tasks.insert("ok_io".to_string(), ok_io);

    let err_io: Box<dyn StoreIo> = Box::new(TestStoreIo(async {
        Err(LaneStoreErrorReport::for_error(make_error(
            StoreError::KeyNotFound,
        )))
    }));
    tasks.insert(failing_io_uri.clone(), err_io);

    let future = tokio::spawn(store_task.run(tasks, 0));
    sleep(Duration::from_secs(5)).await;
    assert!(trigger_tx.trigger());

    let result = timeout(Duration::from_secs(5), future).await;
    match result {
        Ok(Ok(Err(report))) => {
            assert!(report.failed());
            assert_eq!(report.len(), 1);

            let (lane_uri, errors) = report.expect_err().pop().unwrap();
            assert_eq!(failing_io_uri, lane_uri);

            store_err_partial_eq(vec![StoreError::KeyNotFound], errors.errors)
        }
        _ => panic!("Task finished with an incorrect result"),
    }
}
