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
use crate::sync::trigger;
use futures::executor::block_on;
use futures::future::{self, join, ready, select, Either, Ready};
use futures::stream::{self, iter, FusedStream, Iter};
use futures::StreamExt;
use pin_utils::core_reexport::time::Duration;
use pin_utils::pin_mut;
use std::iter::{repeat, Repeat, Take};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::time::timeout;

#[test]
fn future_into() {
    let fut = ready(4);
    let n: i64 = block_on(fut.output_into());
    assert_eq!(n, 4);
}

#[test]
fn ok_into_ok_case() {
    let fut: Ready<Result<i32, String>> = ready(Ok(4));
    let r: Result<i64, String> = block_on(fut.ok_into());
    assert_eq!(r, Ok(4i64));
}

#[test]
fn ok_into_err_case() {
    let fut: Ready<Result<i32, String>> = ready(Err("hello".to_string()));
    let r: Result<i64, String> = block_on(fut.ok_into());
    assert_eq!(r, Err("hello".to_string()));
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

struct RepeatStream(usize);

impl TransformMut<i32> for RepeatStream {
    type Out = Iter<Take<Repeat<i32>>>;

    fn transform(&mut self, input: i32) -> Self::Out {
        iter(repeat(input).take(self.0))
    }
}

#[test]
fn transform_future() {
    let fut = ready(2);
    let plus = Plus(3);
    let n = block_on(fut.transform(plus));
    assert_eq!(n, 5);
}

#[test]
fn chain_future() {
    let fut = ready(2);
    let plus = PlusReady(3);
    let n = block_on(fut.chain(plus));
    assert_eq!(n, 5);
}

#[test]
fn transform_stream() {
    let inputs = iter((0..5).into_iter());

    let outputs: Vec<i32> = block_on(inputs.transform(Plus(3)).collect::<Vec<i32>>());

    assert_eq!(outputs, vec![3, 4, 5, 6, 7]);
}

#[test]
fn transform_stream_fut() {
    let inputs = iter((0..5).into_iter());

    let outputs: Vec<i32> = block_on(inputs.transform_fut(PlusReady(3)).collect::<Vec<i32>>());

    assert_eq!(outputs, vec![3, 4, 5, 6, 7]);
}

#[test]
fn flatmap_stream() {
    let inputs = iter((0..5).into_iter());

    let outputs: Vec<i32> = block_on(
        inputs
            .transform_flat_map(RepeatStream(3))
            .collect::<Vec<i32>>(),
    );

    assert_eq!(outputs, vec![0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]);
}

#[test]
fn flatmap_stream_done() {
    let inputs = iter((0..5).into_iter());

    let mut stream = inputs.transform_flat_map(RepeatStream(3));

    assert!(!stream.is_terminated());

    block_on((&mut stream).collect::<Vec<i32>>());

    assert!(stream.is_terminated());
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

#[tokio::test]
async fn owning_scan() {
    let inputs = iter(vec![1, 2, 3, 4].into_iter());

    let (tx, rx) = mpsc::channel(10);

    let scan_stream = inputs.owning_scan(tx, |sender, i| async move {
        assert!(sender.send(i).await.is_ok());

        Some((sender, i + 1))
    });

    let results = scan_stream.collect::<Vec<_>>().await;
    let sent = rx.collect::<Vec<_>>().await;

    assert_eq!(results, vec![2, 3, 4, 5]);
    assert_eq!(sent, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn owning_scan_done() {
    let inputs = iter(vec![1, 2, 3, 4].into_iter());

    let (tx, _rx) = mpsc::channel(10);

    let scan_stream = inputs.owning_scan(tx, |sender, i| async move {
        assert!(sender.send(i).await.is_ok());
        if i < 3 {
            Some((sender, i + 1))
        } else {
            None
        }
    });

    pin_mut!(scan_stream);

    assert!(!scan_stream.is_terminated());

    loop {
        if scan_stream.next().await.is_none() {
            break;
        }
    }

    assert!(scan_stream.is_terminated());
}

#[tokio::test]
async fn future_notify_on_blocked() {
    let (tx, rx) = trigger::trigger();
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let blocker = async move {
        let result = select(future::pending::<()>().notify_on_blocked(notify_cpy), rx).await;
        assert!(matches!(result, Either::Right((Ok(_), _))));
    };

    let unblocker = async move {
        notify.notified().await;
        tx.trigger();
    };

    let result = timeout(Duration::from_secs(5), join(blocker, unblocker)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn stream_notify_on_blocked() {
    let (tx, rx) = trigger::trigger();
    let notify = Arc::new(Notify::new());
    let notify_cpy = notify.clone();

    let blocker = async move {
        let mut stream = stream::pending::<()>().notify_on_blocked(notify_cpy);
        let result = select(stream.next(), rx).await;
        assert!(matches!(result, Either::Right((Ok(_), _))));
    };

    let unblocker = async move {
        notify.notified().await;
        tx.trigger();
    };

    let result = timeout(Duration::from_secs(5), join(blocker, unblocker)).await;
    assert!(result.is_ok());
}
