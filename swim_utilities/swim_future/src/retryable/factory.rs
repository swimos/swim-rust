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

use futures::task::{Context, Poll};
use futures::Future;
use pin_project::pin_project;

use crate::retryable::ResettableFuture;
use std::pin::Pin;

/// A [`ResettableFuture`] which uses the provided factory for resets.
#[pin_project]
pub struct ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    factory: F,
    #[pin]
    current: F::Future,
}

impl<F, O, E> ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    /// Wraps the given [`FutureFactory`] with a [`ResettableFuture`] implementation. Allowing it to
    /// be used with a [`crate::retryable::RetryableFuture`].
    pub fn wrap(mut factory: F) -> Self {
        let current = factory.future();

        ResetabbleFutureFactory { factory, current }
    }
}

/// A factory which produces a new future with a [`'static`] lifetime each time `future` is invoked.
pub trait FutureFactory<Ok, Err> {
    type Future: Future<Output = Result<Ok, Err>> + 'static;

    /// Creates a new future.
    fn future(&mut self) -> Self::Future;
}

impl<F, O, E> ResettableFuture for ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    /// Resets the current future to a new instance provided by the factory.
    fn reset(self: Pin<&mut Self>) -> bool {
        let mut this = self.project();
        let future = this.factory.future();
        this.current.set(future);

        true
    }
}

impl<F, O, E> Future for ResetabbleFutureFactory<F, O, E>
where
    F: FutureFactory<O, E>,
{
    type Output = Result<O, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.current.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    mod simple {
        use futures::task::{Context, Poll};
        use futures::Future;

        use crate::retryable::factory::{FutureFactory, ResetabbleFutureFactory};
        use crate::retryable::RetryableFuture;
        use std::pin::Pin;

        struct ReadyFactory;

        impl ReadyFactory {
            fn new() -> Self {
                ReadyFactory {}
            }
        }

        #[derive(Debug, Eq, PartialEq)]
        enum FutErr {}

        struct TestFuture;

        impl Future for TestFuture {
            type Output = Result<(), FutErr>;

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Ready(Ok(()))
            }
        }

        impl FutureFactory<(), FutErr> for ReadyFactory {
            type Future = TestFuture;

            fn future(&mut self) -> Self::Future {
                TestFuture {}
            }
        }

        #[tokio::test]
        async fn factory() {
            let factory = ReadyFactory::new();
            let wrapper = ResetabbleFutureFactory::wrap(factory);

            let retryable = RetryableFuture::new(wrapper, Default::default());
            let r = retryable.await;
            assert_eq!(r, Ok(()))
        }
    }

    mod tokio {
        use crate::retryable::factory::{FutureFactory, ResetabbleFutureFactory};

        use crate::retryable::RetryableFuture;
        use futures::future::BoxFuture;
        use futures::FutureExt;
        use tokio::sync::mpsc;

        #[derive(Copy, Clone, Eq, PartialEq, Debug)]
        enum SendErr {
            Err,
        }

        #[derive(Clone)]
        struct Send(mpsc::Sender<i32>, i32);

        impl FutureFactory<i32, SendErr> for Send {
            type Future = BoxFuture<'static, Result<i32, SendErr>>;

            fn future(&mut self) -> Self::Future {
                let Send(tx, value) = self.clone();
                async move {
                    if tx.send(value).await.is_err() {
                        Err(SendErr::Err)
                    } else {
                        Ok(value)
                    }
                }
                .boxed()
            }
        }

        #[tokio::test]
        async fn test_send() {
            let payload = 5;
            let (tx, mut rx) = mpsc::channel(1);
            let wrapper = ResetabbleFutureFactory::wrap(Send(tx, payload));
            let retry = RetryableFuture::new(wrapper, Default::default()).await;
            let result = rx.recv().await;

            assert_eq!(payload, retry.unwrap());
            assert_eq!(payload, result.unwrap())
        }
    }
}
