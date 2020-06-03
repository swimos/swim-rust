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

use super::atomically;
use crate::stm::{self, Abort, Catch, Choice, Constant, Retry, Stm, StmEither};
use crate::transaction::{RetryManager, TransactionError};
use crate::var::TVar;
use futures::stream::{empty, Empty};
use futures::task::Poll;
use futures::Stream;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use tokio::task::JoinHandle;
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
        Ok(v) => panic!(
            "Expected to fail with {} but succeeded with {:?}.",
            expected, v
        ),
        Err(TransactionError::Aborted { error }) => match error.downcast_ref::<TestError>() {
            Some(e) => {
                assert_eq!(e, &expected);
            }
            _ => panic!("Error had the wrong type."),
        },
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
    let stm = var
        .put(12)
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

    assert!(matches!(
        result,
        Ok(Err(TransactionError::TooManyAttempts { num_attempts: 2 }))
    ));
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

#[tokio::test(threaded_scheduler)]
async fn boxed_transaction() {
    let stm = Box::new(Constant(3));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(3)));
}

#[tokio::test(threaded_scheduler)]
async fn ref_transaction() {
    let stm = Constant(3);

    let result = atomically(&&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(3)));
}

#[tokio::test(threaded_scheduler)]
async fn arc_transaction() {
    let stm = Arc::new(Constant(3));

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(3)));
}

#[tokio::test(threaded_scheduler)]
async fn dyn_boxed_transaction() {
    let stm = Constant(3).boxed();

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(3)));
}

#[tokio::test(threaded_scheduler)]
async fn catch_no_abort() {

    let stm = Catch::new(Constant(0), |_: TestError| Constant(1));
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Ok(0)));
}

fn stack_size<T: Stm>(_: &T) -> Option<usize> {
    T::required_stack()
}

#[test]
fn zero_stack_sizes() {
    let var = TVar::new(0);

    assert_eq!(stack_size(&Constant(1)), Some(0));
    assert_eq!(stack_size(&Constant(1).followed_by(Constant(1))), Some(0));
    assert_eq!(stack_size(&Constant(1).map(|n| n * 2)), Some(0));
    assert_eq!(stack_size(&var.get()), Some(0));
    assert_eq!(stack_size(&var.put(1)), Some(0));
    assert_eq!(
        stack_size::<Abort<TestError, i32>>(&stm::abort(TestError("Boom".to_string()))),
        Some(0)
    );
    assert_eq!(stack_size::<Retry<i32>>(&stm::retry()), Some(0));
}

#[test]
fn increase_stack_sizes() {
    let catch = Catch::new(Constant(1), |_: TestError| Constant(1));
    assert_eq!(stack_size(&catch), Some(1));
    let catch2 = Catch::new(catch, |_: TestError| Constant(1));
    assert_eq!(stack_size(&catch2), Some(2));

    let choice = Choice::new(Constant(1), Constant(1));
    assert_eq!(stack_size(&choice), Some(1));
    let choice2 = Choice::new(choice, Constant(1));
    assert_eq!(stack_size(&choice2), Some(2));
}

fn catch<S: Stm<Result = i32>>(s: S) -> impl Stm<Result = i32> {
    Catch::new(s, |_: TestError| Constant(1))
}

#[test]
fn greater_of_two_stack_sizes() {
    let seq1 = catch(Constant(1)).followed_by(Constant(1));
    let seq2 = Constant(1).followed_by(catch(Constant(1)));
    let seq3 = catch(Constant(1)).followed_by(catch(Constant(1)));
    assert_eq!(stack_size(&seq1), Some(1));
    assert_eq!(stack_size(&seq2), Some(1));
    assert_eq!(stack_size(&seq3), Some(1));

    let and_then1 = Constant(1).and_then(|_| catch(Constant(1)));
    let and_then2 = catch(Constant(1)).and_then(|_| Constant(1));
    let and_then3 = catch(Constant(1)).and_then(|_| catch(Constant(1)));
    assert_eq!(stack_size(&and_then1), Some(1));
    assert_eq!(stack_size(&and_then2), Some(1));
    assert_eq!(stack_size(&and_then3), Some(1));

    let longer_recovery1 = Catch::new(Constant(1), |_: TestError| catch(catch(Constant(1))));
    let longer_recovery2 = Choice::new(Constant(1), catch(catch(Constant(1))));
    assert_eq!(stack_size(&longer_recovery1), Some(2));
    assert_eq!(stack_size(&longer_recovery2), Some(2));
}

#[test]
fn dyn_stm_erases_stack_size() {
    assert!(stack_size(&Constant(1).boxed()).is_none());
    let catch = Catch::new(Constant(1).boxed(), |_: TestError| Constant(1));
    assert!(stack_size(&catch).is_none());
    let choice = Choice::new(Constant(1).boxed(), Constant(1));
    assert!(stack_size(&choice).is_none());

    assert!(stack_size(&Constant(1).boxed().followed_by(Constant(1))).is_none());
    assert!(stack_size(&Constant(1).followed_by(Constant(1).boxed())).is_none());
    assert!(stack_size(&Constant(1).boxed().and_then(|_| Constant(1))).is_none());
    assert!(stack_size(&Constant(1).and_then(|_| Constant(1).boxed())).is_none());
    assert!(stack_size(&Constant(1).boxed().map(|i| i * 2)).is_none());
}
