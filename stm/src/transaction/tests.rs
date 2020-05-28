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
use crate::transaction::{TransactionError, RetryManager};

struct ExactlyOnce;

impl RetryManager for ExactlyOnce {
    type ContentionManager = Empty<()>;

    fn contention_manager(&self) -> Self::ContentionManager {
        empty()
    }

    fn max_retries(&self) -> usize {
        0
    }
}

#[tokio::test]
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

#[tokio::test]
async fn immediate_abort() {

    let stm: Abort<TestError, i32> = stm::abort(TestError::new("Boom"));

    let result = atomically(&stm, ExactlyOnce).await;

    assert_aborts_with(result, TestError::new("Boom"));
}

#[tokio::test]
async fn map_constant_transaction() {

    let stm = Constant(2).map(|n| n * 2);

    let result = atomically(&stm, ExactlyOnce).await;

    assert!(matches!(result, Ok(4)));
}

#[tokio::test]
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

#[tokio::test]
async fn either_constant_transaction() {

    let stm_left: StmEither<Constant<i32>, Constant<i32>> = StmEither::Left(Constant(2));
    let result_left = atomically(&stm_left, ExactlyOnce).await;
    assert!(matches!(result_left, Ok(2)));

    let stm_right: StmEither<Constant<i32>, Constant<i32>> = StmEither::Right(Constant(4));
    let result_right = atomically(&stm_right, ExactlyOnce).await;
    assert!(matches!(result_right, Ok(4)));
}

#[tokio::test]
async fn invalid_retry() {
    let stm = stm::retry::<i32>();
    let result = atomically(&stm, ExactlyOnce).await;
    assert!(matches!(result, Err(TransactionError::InvalidRetry)))
}