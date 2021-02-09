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

use crate::routing::ws::protocol::CloseReason;
use crate::routing::ws::stream::selector::{SelectorResult, WsStreamSelector};
use crate::routing::ws::stream::JoinedStreamSink;
use futures::future::{ready, Ready};
use futures::stream::FusedStream;
use futures::task::{AtomicWaker, Context, Poll};
use futures::{Sink, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestError(String);

#[derive(Debug)]
struct Consume {
    on_ready: bool,
    result: Result<(), TestError>,
    expected: i32,
}

#[derive(Debug, Default)]
struct Inner {
    produce: Option<Result<i32, TestError>>,
    consume: Option<Consume>,
    close_error: Option<TestError>,
    produce_waker: AtomicWaker,
    consume_waker: AtomicWaker,
    closed: bool,
}

#[derive(Debug, Default, Clone)]
struct TestWsStream(Arc<Mutex<Inner>>);

impl Stream for TestWsStream {
    type Item = Result<i32, TestError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut lock = self.0.lock();
        let Inner {
            produce,
            produce_waker,
            closed,
            ..
        } = &mut *lock;
        if *closed {
            Poll::Ready(None)
        } else {
            match produce.take() {
                Some(result) => Poll::Ready(Some(result)),
                _ => {
                    produce_waker.register(cx.waker());
                    Poll::Pending
                }
            }
        }
    }
}

impl Sink<i32> for TestWsStream {
    type Error = TestError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = self.0.lock();
        let Inner {
            consume,
            consume_waker,
            closed,
            ..
        } = &mut *lock;
        if *closed {
            Poll::Ready(Err(TestError("Closed!".to_string())))
        } else {
            match consume {
                Some(Consume {
                    on_ready, result, ..
                }) => {
                    if *on_ready {
                        let poll_result = Poll::Ready(result.clone());
                        *result = Ok(());
                        poll_result
                    } else {
                        Poll::Ready(Ok(()))
                    }
                }
                _ => {
                    consume_waker.register(cx.waker());
                    Poll::Pending
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: i32) -> Result<(), Self::Error> {
        let mut lock = self.0.lock();
        let Inner {
            consume, closed, ..
        } = &mut *lock;
        if *closed {
            Err(TestError("Closed!".to_string()))
        } else {
            match consume.take() {
                Some(Consume {
                    result, expected, ..
                }) => {
                    assert_eq!(item, expected);
                    result
                }
                _ => Err(TestError("Not ready to send!".to_string())),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = self.0.lock();
        let Inner {
            produce_waker,
            consume_waker,
            close_error,
            closed,
            ..
        } = &mut *lock;
        produce_waker.wake();
        consume_waker.wake();
        *closed = true;
        if let Some(err) = close_error.take() {
            Poll::Ready(Err(err))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl TestWsStream {
    fn stage_produce(&self, value: i32) {
        self.0.lock().produce = Some(Ok(value))
    }

    fn stage_produce_err(&self, message: &str) {
        self.0.lock().produce = Some(Err(TestError(message.to_string())))
    }

    fn stage_consume(&self, expected: i32) {
        self.0.lock().consume = Some(Consume {
            on_ready: true,
            result: Ok(()),
            expected,
        })
    }

    fn stage_consume_error(&self, expected: i32, message: &str) {
        self.0.lock().consume = Some(Consume {
            on_ready: true,
            result: Err(TestError(message.to_string())),
            expected,
        })
    }

    fn stage_consume_error_on_send(&self, expected: i32, message: &str) {
        self.0.lock().consume = Some(Consume {
            on_ready: false,
            result: Err(TestError(message.to_string())),
            expected,
        })
    }

    fn stage_close_error(&self, message: &str) {
        self.0.lock().close_error = Some(TestError(message.to_string()))
    }

    fn close(&self) {
        self.0.lock().closed = true;
    }
}

#[tokio::test]
async fn produce_available() {
    let test_stream = TestWsStream::default();
    test_stream.stage_produce(12);

    let (_tx, rx) = mpsc::channel(8);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;

    assert_eq!(result, Some(Ok(SelectorResult::Read(12))));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn consume_available() {
    let test_stream = TestWsStream::default();

    let (tx, rx) = mpsc::channel(8);

    assert!(tx.send(1).await.is_ok());
    test_stream.stage_consume(1);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;

    assert_eq!(result, Some(Ok(SelectorResult::Written)));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn both_available() {
    let test_stream = TestWsStream::default();

    let (tx, rx) = mpsc::channel(8);

    test_stream.stage_produce(56);
    assert!(tx.send(4).await.is_ok());
    test_stream.stage_consume(4);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result1 = selector.next().await;
    let result2 = selector.next().await;

    assert_eq!(result1, Some(Ok(SelectorResult::Read(56))));
    assert_eq!(result2, Some(Ok(SelectorResult::Written)));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn produce_error() {
    let test_stream = TestWsStream::default();
    test_stream.stage_produce_err("Boom!");

    let (_tx, rx) = mpsc::channel(8);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;

    assert_eq!(result, Some(Err(TestError("Boom!".to_string()))));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn consume_error_on_ready() {
    let test_stream = TestWsStream::default();

    let (tx, rx) = mpsc::channel(8);

    assert!(tx.send(0).await.is_ok());
    test_stream.stage_consume_error(0, "Boom!");

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;

    assert_eq!(result, Some(Err(TestError("Boom!".to_string()))));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn consume_error_on_send() {
    let test_stream = TestWsStream::default();

    let (tx, rx) = mpsc::channel(8);

    assert!(tx.send(0).await.is_ok());
    test_stream.stage_consume_error_on_send(0, "Boom!");

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;

    assert_eq!(result, Some(Err(TestError("Boom!".to_string()))));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn alternates_produce_and_consume() {
    let test_stream = TestWsStream::default();

    let staging = test_stream.clone();

    let (tx, rx) = mpsc::channel(8);

    assert!(tx.send(1).await.is_ok());
    assert!(tx.send(2).await.is_ok());

    staging.stage_produce(66);
    staging.stage_consume(1);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(66))));

    staging.stage_produce(76);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Written)));

    staging.stage_consume(2);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(76))));

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Written)));

    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn consumption_possible_but_no_data() {
    let test_stream = TestWsStream::default();

    let staging = test_stream.clone();

    let (_tx, rx) = mpsc::channel(8);

    staging.stage_produce(66);
    staging.stage_consume(0);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(66))));

    staging.stage_produce(76);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(76))));
    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn production_continues_after_no_more_consumption() {
    let test_stream = TestWsStream::default();

    let staging = test_stream.clone();

    let (tx, rx) = mpsc::channel(8);

    staging.stage_produce(66);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(66))));

    staging.stage_produce(76);
    drop(tx);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(76))));
    assert!(!selector.is_terminated());
}

#[tokio::test]
async fn production_and_consumption_stop_after_close() {
    let test_stream = TestWsStream::default();

    let staging = test_stream.clone();

    let (tx, rx) = mpsc::channel(8);

    assert!(tx.send(1).await.is_ok());

    staging.stage_produce(66);

    let mut selector = WsStreamSelector::new(test_stream, rx);

    let result = selector.next().await;
    assert_eq!(result, Some(Ok(SelectorResult::Read(66))));

    staging.stage_consume(1);
    staging.stage_produce(88);
    staging.close();

    let result = selector.next().await;
    assert_eq!(result, Some(Err(TestError("Closed!".to_string()))));

    let result = selector.next().await;
    assert!(result.is_none());
    assert!(selector.is_terminated());
}

#[tokio::test]
async fn error_on_close() {
    let test_stream = TestWsStream::default();

    let staging = test_stream.clone();

    let (_tx, rx) = mpsc::channel(8);

    let mut selector = WsStreamSelector::new(test_stream, rx);
    staging.stage_close_error("Boom!");
    staging.close();

    let result = selector.next().await;
    assert_eq!(result, Some(Err(TestError("Boom!".to_string()))));
    assert!(!selector.is_terminated());

    let result = selector.next().await;
    assert!(result.is_none());
    assert!(selector.is_terminated());
}

struct TestStreamSink {
    input: Vec<i32>,
    input_index: usize,
    outputs: Vec<i32>,
    max_outputs: usize,
}

impl TestStreamSink {
    fn new(input: Vec<i32>, max_outputs: usize) -> Self {
        TestStreamSink {
            input,
            input_index: 0,
            outputs: vec![],
            max_outputs,
        }
    }
}

impl Stream for TestStreamSink {
    type Item = Result<i32, ()>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let TestStreamSink {
            input, input_index, ..
        } = self.get_mut();
        if let Some(n) = input.get(*input_index) {
            *input_index += 1;
            Poll::Ready(Some(if *n >= 0 { Ok(*n) } else { Err(()) }))
        } else {
            Poll::Ready(None)
        }
    }
}

impl Sink<i32> for TestStreamSink {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: i32) -> Result<(), Self::Error> {
        let TestStreamSink {
            outputs,
            max_outputs,
            ..
        } = self.get_mut();
        if outputs.len() < *max_outputs {
            outputs.push(item);
            Ok(())
        } else {
            Err(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl JoinedStreamSink<i32, ()> for TestStreamSink {
    type CloseFut = Ready<Result<(), ()>>;

    fn close(self, _reason: Option<CloseReason>) -> Self::CloseFut {
        ready(Ok(()))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Wrapped(i32);
#[derive(Debug, PartialEq, Eq)]
struct TestErr;

impl From<i32> for Wrapped {
    fn from(n: i32) -> Self {
        Wrapped(n)
    }
}

impl From<Wrapped> for i32 {
    fn from(w: Wrapped) -> Self {
        let Wrapped(n) = w;
        n
    }
}

impl From<()> for TestErr {
    fn from(_: ()) -> Self {
        TestErr
    }
}

impl From<TestErr> for () {
    fn from(_: TestErr) {}
}

#[tokio::test]
async fn joined_stream_sink_trans_stream() {
    let test_str_snk = TestStreamSink::new(vec![1, 2, -3], 0);
    let transformed = test_str_snk.transform_data::<Wrapped, TestErr>();

    let results = transformed.collect::<Vec<_>>().await;
    assert_eq!(results, vec![Ok(Wrapped(1)), Ok(Wrapped(2)), Err(TestErr)]);
}

#[tokio::test]
async fn joined_stream_sink_trans_sink() {
    let test_str_snk = TestStreamSink::new(vec![], 2);
    let mut transformed = test_str_snk.transform_data::<Wrapped, TestErr>();

    assert!(transformed.send(Wrapped(3)).await.is_ok());
    assert!(transformed.send(Wrapped(7)).await.is_ok());
    assert_eq!(transformed.send(Wrapped(3)).await, Err(TestErr));

    assert_eq!(transformed.str_sink.outputs, vec![3, 7]);
}
