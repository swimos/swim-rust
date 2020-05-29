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

use crate::stm::{self, Constant, Abort, Stm, StmEither};
use super::atomically;
use futures::stream::{Empty, empty};
use std::fmt::{Display, Formatter, Debug};
use std::error::Error;
use std::sync::Arc;
use crate::transaction::{TransactionError, RetryManager};
use crate::var::TVar;
use tokio::task::JoinHandle;
use futures::Stream;
use std::task::Context;
use futures::task::Poll;
use std::pin::Pin;
use tokio::time::Duration;

struct ExactlyOnce;

#[derive(Clone)]
struct RetryFor(usize);

impl RetryManager for RetryFor {
    type ContentionManager = RetryFor;

    fn contention_manager(&self) -> Self::ContentionManager {
        self.clone()
    }

    fn max_retries(&self) -> usize {
       self.0
    }
}

impl Stream for RetryFor {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let n = self.0;
        if n == 0 {
            Poll::Ready(None)
        } else {
            self.get_mut().0 -= 1;
            Poll::Ready(Some(()))
        }
    }
}

impl RetryManager for ExactlyOnce {
    type ContentionManager = Empty<()>;

    fn contention_manager(&self) -> Self::ContentionManager {
        empty()
    }

    fn max_retries(&self) -> usize {
        0
    }
}

fn retry_for(n: usize) -> RetryFor {
    RetryFor(n)
}

fn forever() -> RetryFor {
    retry_for(usize::max_value())
}

#[tokio::test(threaded_scheduler)]
async fn constant_transaction() {

    let stm = Constant(3);

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(3)));

}

#[derive(Debug, PartialEq, Eq, Clone)]
struct TestError(String);

impl TestError {

    fn new<S: Into<String>>(txt: S) -> TestError {
        TestError(txt.into())
    }

}

impl Display for TestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TestError {}

fn assert_aborts_with<T: Debug>(result: Result<T, TransactionError>, expected: TestError) {
    match result {
        Ok(v) => panic!("Expected to fail with {} but succeeded with {:?}.", expected, v),
        Err(TransactionError::Aborted { error }) => {
            match error.downcast_ref::<TestError>() {
                Some(e) => {
                    assert_eq!(e, &expected);
                },
                _ => panic!("Error had the wrong type."),
            }
        }
        err => panic!("Unexpected error: {:?}", err),
    }
}

#[tokio::test(threaded_scheduler)]
async fn immediate_abort() {

    let stm: Abort<TestError, i32> = stm::abort(TestError::new("Boom"));

    let result = atomically(&stm, ExactlyOnce).await;

    assert_aborts_with(result, TestError::new("Boom"));
}

#[tokio::test(threaded_scheduler)]
async fn map_constant_transaction() {

    let stm = Constant(2).map(|n| n * 2);

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(4)));
}

#[tokio::test(threaded_scheduler)]
async fn and_then_constant_transaction() {

    let stm = Constant(5).and_then(|n| {
       if n % 2 == 0 {
           Constant("Even")
       } else {
           Constant("Odd")
       }
    });

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok("Odd")));
}

#[tokio::test(threaded_scheduler)]
async fn followed_constant_transaction() {

    let stm = Constant(5).followed_by(Constant(10));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(10)));
}

#[tokio::test(threaded_scheduler)]
async fn either_constant_transaction() {

    let stm_left: StmEither<Constant<i32>, Constant<i32>> = StmEither::Left(Constant(2));
    let result_left = atomically(&stm_left, ExactlyOnce).await;
    assert!(matches!(result_left, Ok(2)));

    let stm_right: StmEither<Constant<i32>, Constant<i32>> = StmEither::Right(Constant(4));
    let result_right = atomically(&stm_right, ExactlyOnce).await;
    assert!(matches!(result_right, Ok(4)));
}

#[tokio::test(threaded_scheduler)]
async fn invalid_retry() {
    let stm = stm::retry::<i32>();
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Err(TransactionError::InvalidRetry)));
}

#[tokio::test(threaded_scheduler)]
async fn single_read() {
    let var = TVar::new(2);
    let stm = var.get();
    let content = var.load().await;
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Ok(v) if Arc::ptr_eq(&v, &content)));
}

#[tokio::test(threaded_scheduler)]
async fn single_put() {
    let var = TVar::new(2);
    let stm = var.put(5);
    let before = var.load().await;
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Ok(_)));
    let after = var.load().await;
    assert_eq!(*before, 2);
    assert_eq!(*after, 5);
}

#[tokio::test(threaded_scheduler)]
async fn get_and_set() {
    let var = TVar::new(2);
    let stm = var.get().and_then(|n| var.put(*n + 1));
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Ok(_)));
    let after = var.load().await;
    assert_eq!(*after, 3);
}

#[tokio::test(threaded_scheduler)]
async fn set_get_and_set() {
    let var = TVar::new(2);
    let stm = var.put(12)
        .followed_by(var.get().and_then(|n| var.put(*n + 1)));
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Ok(_)));
    let after = var.load().await;
    assert_eq!(*after, 13);
}

#[tokio::test(threaded_scheduler)]
async fn increment_concurrently() {

    let var = TVar::new(0);
    let var_copy = var.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let barrier_copy = barrier.clone();

    let task: JoinHandle<Result<(), TransactionError>> = tokio::task::spawn(async move {
        barrier.wait().await;
        for _ in 0..10 {
            let stm = var_copy.get().and_then(|n| var_copy.put(*n + 1));
            atomically(&stm, forever()).await?;
        }
        Ok(())
    });

    barrier_copy.wait().await;
    for _ in 0..10 {
        let stm = var.get().and_then(|n| var.put(*n + 1));
        assert!(atomically(&stm, forever()).await.is_ok());
    }

    let result = task.await;

    assert!(matches!(result, Ok(Ok(_))));

    let after: Arc<i32> = var.load().await;
    assert_eq!(*after, 20);

}

#[tokio::test(threaded_scheduler)]
async fn simple_retry() {

    let var = TVar::new(0i32);

    let var_copy = var.clone();

    let task = tokio::task::spawn(async move {
        let stm = var_copy.get().and_then(|n| {
            if *n == 1 {
                stm::left(Constant("Done".to_string()))
            } else {
                stm::right(stm::retry())
            }
        });
        atomically(&stm, retry_for(1)).await
    });

    tokio::time::delay_for(Duration::from_millis(100u64)).await;

    var.store(1).await;

    let result = task.await;

    assert!(matches!(result, Ok(Ok(s)) if s == "Done"));

}

#[tokio::test(threaded_scheduler)]
async fn fail_after_retry() {

    let var = TVar::new(0i32);

    let var_copy = var.clone();

    let task = tokio::task::spawn(async move {
        let stm = var_copy.get().and_then(|n| {
            if *n == 1 {
                stm::left(Constant("Done".to_string()))
            } else {
                stm::right(stm::retry())
            }
        });
        atomically(&stm, retry_for(1)).await
    });

    tokio::time::delay_for(Duration::from_millis(100u64)).await;

    var.store(2).await;

    let result = task.await;

    assert!(matches!(result, Ok(Err(TransactionError::TooManyAttempts { num_attempts: 2}))));

}

#[tokio::test(threaded_scheduler)]
async fn eventual_retry() {

    let var = TVar::new(0i32);

    let var_copy = var.clone();

    let task = tokio::task::spawn(async move {
        let stm = var_copy.get().and_then(|n| {
            if *n == 1 {
                stm::left(Constant("Done".to_string()))
            } else {
                stm::right(stm::retry())
            }
        });
        atomically(&stm, retry_for(10)).await
    });

    tokio::time::delay_for(Duration::from_millis(10u64)).await;

    var.store(2).await;
    tokio::time::delay_for(Duration::from_millis(10u64)).await;

    var.store(3).await;
    tokio::time::delay_for(Duration::from_millis(10u64)).await;

    var.store(4).await;
    tokio::time::delay_for(Duration::from_millis(10u64)).await;

    var.store(5).await;
    tokio::time::delay_for(Duration::from_millis(10u64)).await;

    var.store(1).await;

    let result = task.await;

    assert!(matches!(result, Ok(Ok(s)) if s == "Done"));

}
