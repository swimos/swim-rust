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

use super::*;

use hamcrest2::assert_that;
use hamcrest2::prelude::*;

#[tokio::test]
async fn send_left() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = EitherSink::new(left, right);
    let result = either_sink.send_item(Either::Left(7)).await;
    assert_that!(result, ok());
    let expected: Vec<i32> = vec![7];
    assert_that!(&either_sink.left, eq(&expected));
    assert_that!(&either_sink.right, empty());
}

#[tokio::test]
async fn send_right() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = EitherSink::new(left, right);
    let result = either_sink
        .send_item(Either::Right("hello".to_string()))
        .await;
    assert_that!(result, ok());
    let expected: Vec<String> = vec!["hello".to_string()];
    assert_that!(&either_sink.left, empty());
    assert_that!(&either_sink.right, eq(&expected));
}

#[tokio::test]
async fn send_both() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = EitherSink::new(left, right);
    let result = either_sink.send_item(Either::Left(7)).await;
    assert_that!(result, ok());
    let result = either_sink
        .send_item(Either::Right("hello".to_string()))
        .await;
    assert_that!(result, ok());
    let expected_left: Vec<i32> = vec![7];
    assert_that!(&either_sink.left, eq(&expected_left));
    let expected_right: Vec<String> = vec!["hello".to_string()];
    assert_that!(&either_sink.right, eq(&expected_right));
}

#[tokio::test]
async fn send_interleaved() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = EitherSink::new(left, right);

    let inputs = vec![
        Either::Left(12),
        Either::Right("hello".to_string()),
        Either::Left(-1),
        Either::Right("world".to_string()),
    ];

    for input in inputs.into_iter() {
        let result = either_sink.send_item(input).await;
        assert_that!(result, ok());
    }

    let expected_left: Vec<i32> = vec![12, -1];
    assert_that!(&either_sink.left, eq(&expected_left));
    let expected_right: Vec<String> = vec!["hello".to_string(), "world".to_string()];
    assert_that!(&either_sink.right, eq(&expected_right));
}
