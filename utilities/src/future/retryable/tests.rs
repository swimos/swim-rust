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

use futures::task::{Context, Poll};
use futures::Future;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;

use crate::future::retryable::strategy::RetryStrategy;
use crate::future::retryable::{ResettableFuture, RetryableFuture};
use std::num::NonZeroUsize;
use tokio::sync::mpsc::error::TrySendError;

struct MpscSender<P>
where
    P: Clone,
{
    tx: mpsc::Sender<P>,
    p: P,
}

#[derive(Eq, PartialEq, Debug)]
enum SendErr {
    Err,
}

impl<P> Future for MpscSender<P>
where
    P: Clone + Unpin,
{
    type Output = Result<P, SendErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let MpscSender { tx, p } = self.get_mut();

        match tx.poll_ready(cx) {
            Poll::Ready(Ok(_)) => match tx.try_send(p.clone()) {
                Ok(_) => Poll::Ready(Ok(p.clone())),
                Err(TrySendError::Closed(_)) => Poll::Ready(Err(SendErr::Err)),
                Err(TrySendError::Full(_)) => unreachable!(),
            },

            Poll::Ready(Err(_)) => Poll::Ready(Err(SendErr::Err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P> ResettableFuture for MpscSender<P>
where
    P: Clone,
{
    fn reset(self: Pin<&mut Self>) -> bool {
        true
    }
}

#[tokio::test]
async fn test() {
    let p = 5;
    let (tx, mut rx) = mpsc::channel(1);
    let sender = MpscSender { tx, p };

    let retry: Result<i32, SendErr> = RetryableFuture::new(
        sender,
        RetryStrategy::immediate(NonZeroUsize::new(2).unwrap()),
    )
    .await;
    assert_eq!(retry.unwrap(), p);

    let result = rx.recv().await;
    assert_eq!(p, result.unwrap())
}
