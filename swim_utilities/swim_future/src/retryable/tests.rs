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

use crate::retryable::strategy::RetryStrategy;
use crate::retryable::{ResettableFuture, RetryableFuture};
use futures::task::{Context, Poll};
use futures::Future;
use pin_project::pin_project;
use std::num::NonZeroUsize;
use std::pin::Pin;
use tokio::sync::mpsc;

#[pin_project]
struct MpscSender<F, Fut> {
    tx: mpsc::Sender<i32>,
    make_fut: F,
    #[pin]
    current: Option<Fut>,
    value: i32,
}

impl<F, Fut> MpscSender<F, Fut>
where
    F: Fn(mpsc::Sender<i32>, i32) -> Fut,
{
    fn new(tx: mpsc::Sender<i32>, value: i32, make_fut: F) -> Self {
        let init = make_fut(tx.clone(), value);
        MpscSender {
            tx,
            make_fut,
            current: Some(init),
            value,
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
enum SendErr {
    Err,
}

impl<F, Fut> Future for MpscSender<F, Fut>
where
    F: Fn(mpsc::Sender<i32>, i32) -> Fut,
    Fut: Future<Output = Result<i32, SendErr>>,
{
    type Output = Result<i32, SendErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let mut current: Pin<&mut Option<Fut>> = projected.current;
        if let Some(fut) = current.as_mut().as_pin_mut() {
            let result = fut.poll(cx);
            if result.is_ready() {
                current.set(None);
            }
            result
        } else {
            panic!("Used twice!");
        }
    }
}

impl<F, Fut> ResettableFuture for MpscSender<F, Fut>
where
    F: Fn(mpsc::Sender<i32>, i32) -> Fut,
    Fut: Future<Output = Result<i32, SendErr>>,
{
    fn reset(self: Pin<&mut Self>) -> bool {
        let mut projected = self.project();
        let fut = (projected.make_fut)(projected.tx.clone(), *projected.value);
        projected.current.set(Some(fut));
        true
    }
}

async fn send(tx: mpsc::Sender<i32>, value: i32) -> Result<i32, SendErr> {
    if tx.send(value).await.is_err() {
        Err(SendErr::Err)
    } else {
        Ok(value)
    }
}

#[tokio::test]
async fn test() {
    let p = 5;
    let (tx, mut rx) = mpsc::channel(1);
    let sender = MpscSender::new(tx, p, send);

    let retry: Result<i32, SendErr> = RetryableFuture::new(
        sender,
        RetryStrategy::immediate(NonZeroUsize::new(2).unwrap()),
    )
    .await;
    assert_eq!(retry.unwrap(), p);

    let result = rx.recv().await;
    assert_eq!(p, result.unwrap())
}
