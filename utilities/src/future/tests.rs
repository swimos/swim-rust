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

use super::{SwimFutureExt, SwimStreamExt, SwimTryFutureExt, TransformMut};
use futures::executor::block_on;
use futures::future::{ready, Ready};
use futures::stream::{iter, FusedStream};
use futures::StreamExt;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;

#[test]
fn future_into() {
    let fut = ready(4);
    let n: i64 = block_on(fut.output_into());
    assert_that!(n, eq(4));
}

#[test]
fn ok_into_ok_case() {
    let fut: Ready<Result<i32, String>> = ready(Ok(4));
    let r: Result<i64, String> = block_on(fut.ok_into());
    assert_that!(r, eq(Ok(4i64)));
}

#[test]
fn ok_into_err_case() {
    let fut: Ready<Result<i32, String>> = ready(Err("hello".to_string()));
    let r: Result<i64, String> = block_on(fut.ok_into());
    assert_that!(r, eq(Err("hello".to_string())));
}

struct Plus(i32);

impl TransformMut<i32> for Plus {
    type Out = i32;

    fn transform(&mut self, input: i32) -> Self::Out {
        input + self.0
    }
}

struct PlusReady(i32);

impl TransformMut<i32> for PlusReady {
    type Out = Ready<i32>;

    fn transform(&mut self, input: i32) -> Self::Out {
        ready(input + self.0)
    }
}

#[test]
fn transform_future() {
    let fut = ready(2);
    let plus = Plus(3);
    let n = block_on(fut.transform(plus));
    assert_that!(n, eq(5));
}

#[test]
fn chain_future() {
    let fut = ready(2);
    let plus = PlusReady(3);
    let n = block_on(fut.chain(plus));
    assert_that!(n, eq(5));
}

#[test]
fn transform_stream() {
    let inputs = iter((0..5).into_iter());

    let outputs: Vec<i32> = block_on(inputs.transform(Plus(3)).collect::<Vec<i32>>());

    assert_that!(outputs, eq(vec![3, 4, 5, 6, 7]));
}

#[test]
fn transform_stream_fut() {
    let inputs = iter((0..5).into_iter());

    let outputs: Vec<i32> = block_on(inputs.transform_fut(PlusReady(3)).collect::<Vec<i32>>());

    assert_that!(outputs, eq(vec![3, 4, 5, 6, 7]));
}

#[test]
fn transform_stream_fut_done() {
    let inputs = iter((0..5).into_iter());

    let mut stream = inputs.transform_fut(PlusReady(3));

    assert!(!stream.is_terminated());

    block_on((&mut stream).collect::<Vec<i32>>());

    assert!(stream.is_terminated());
}

struct PlusIfNonNeg(i32);

impl TransformMut<i32> for PlusIfNonNeg {
    type Out = Result<i32, ()>;

    fn transform(&mut self, input: i32) -> Self::Out {
        if input >= 0 {
            Ok(input + self.0)
        } else {
            Err(())
        }
    }
}

#[test]
fn until_failure() {
    let inputs = iter(vec![0, 1, 2, -3, 4].into_iter());
    let outputs: Vec<i32> = block_on(inputs.until_failure(PlusIfNonNeg(3)).collect::<Vec<i32>>());
    assert_eq!(outputs, vec![3, 4, 5]);
}

#[tokio::test]
async fn unit_future() {
    let fut = async { 5 };

    assert_eq!(fut.unit().await, ());
}
