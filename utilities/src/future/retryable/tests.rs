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
use tokio::sync::mpsc::error::TrySendError;

use crate::future::retryable::strategy::RetryStrategy;
use crate::future::retryable::{FutureFactory, Retry, RetryContext, RetryableFuture};

#[derive(Clone)]
struct TestSender<P> {
    tx: mpsc::Sender<P>,
    payload: P,
}

impl<'f, P: 'f, Err> FutureFactory<'f, (), Err> for TestSender<P> {
    type Future = Self;

    fn future(&'f mut self, _ctx: &mut RetryContext<Err>) -> Self::Future {
        self.clone()
    }
}

impl<P> RetryableFuture for TestSender<P>
where
    P: Send + Unpin + Clone,
{
    type Ok = ();
    type Err = FutErr;

    fn retry(&mut self, _ctx: &mut RetryContext<Self::Err>) -> bool {
        true
    }
}

#[derive(Clone)]
enum FutErr {
    Err,
}

impl<P> Future for TestSender<P>
where
    P: Send + Unpin + Clone,
{
    type Output = Result<(), FutErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let TestSender { tx, payload } = self.get_mut();

        match tx.poll_ready(cx) {
            Poll::Ready(Ok(_)) => match tx.try_send(payload.clone()) {
                Ok(_) => Poll::Ready(Ok(())),
                Err(TrySendError::Closed(_)) => Poll::Ready(Err(FutErr::Err)),
                Err(TrySendError::Full(_)) => unreachable!(),
            },

            Poll::Ready(Err(_)) => Poll::Ready(Err(FutErr::Err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::test]
async fn testy() {
    let payload = 5;
    let (tx, mut rx) = mpsc::channel(1);
    let retry: Result<(), FutErr> =
        Retry::new(TestSender { tx, payload }, RetryStrategy::immediate(2)).await;

    assert!(retry.is_ok());

    let result = rx.recv().await;
    assert_eq!(payload, result.unwrap())
}
