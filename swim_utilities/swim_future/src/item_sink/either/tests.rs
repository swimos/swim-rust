// Copyright 2015-2021 Swim Inc.
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

#[tokio::test]
async fn split_send_left() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = SplitSink::new(left, right);
    let result = either_sink.send_item(Either::Left(7)).await;
    assert!(result.is_ok());
    let expected: Vec<i32> = vec![7];
    assert_eq!(&either_sink.left, &expected);
    assert!(either_sink.right.is_empty());
}

#[tokio::test]
async fn split_send_right() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = SplitSink::new(left, right);
    let result = either_sink
        .send_item(Either::Right("hello".to_string()))
        .await;
    assert!(result.is_ok());
    let expected: Vec<String> = vec!["hello".to_string()];
    assert!(either_sink.left.is_empty());
    assert_eq!(&either_sink.right, &expected);
}

#[tokio::test]
async fn split_send_both() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = SplitSink::new(left, right);
    let result = either_sink.send_item(Either::Left(7)).await;
    assert!(result.is_ok());
    let result = either_sink
        .send_item(Either::Right("hello".to_string()))
        .await;
    assert!(result.is_ok());
    let expected_left: Vec<i32> = vec![7];
    assert_eq!(&either_sink.left, &expected_left);
    let expected_right: Vec<String> = vec!["hello".to_string()];
    assert_eq!(&either_sink.right, &expected_right);
}

#[tokio::test]
async fn split_send_interleaved() {
    let left: Vec<i32> = vec![];
    let right: Vec<String> = vec![];
    let mut either_sink = SplitSink::new(left, right);

    let inputs = vec![
        Either::Left(12),
        Either::Right("hello".to_string()),
        Either::Left(-1),
        Either::Right("world".to_string()),
    ];

    for input in inputs.into_iter() {
        let result = either_sink.send_item(input).await;
        assert!(result.is_ok());
    }

    let expected_left: Vec<i32> = vec![12, -1];
    assert_eq!(&either_sink.left, &expected_left);
    let expected_right: Vec<String> = vec!["hello".to_string(), "world".to_string()];
    assert_eq!(&either_sink.right, &expected_right);
}

#[tokio::test]
async fn either_send_left() {
    let left: Vec<i32> = vec![];
    let mut either_sink: EitherSink<Vec<i32>, Vec<i32>> = EitherSink::left(left);
    let result = either_sink.send_item(7).await;
    assert!(result.is_ok());
    assert!(matches!(either_sink, EitherSink(Either::Left(v)) if v == vec![7]));
}

#[tokio::test]
async fn either_send_right() {
    let right: Vec<i32> = vec![];
    let mut either_sink: EitherSink<Vec<i32>, Vec<i32>> = EitherSink::right(right);
    let result = either_sink.send_item(7).await;
    assert!(result.is_ok());
    assert!(matches!(either_sink, EitherSink(Either::Right(v)) if v == vec![7]));
}
