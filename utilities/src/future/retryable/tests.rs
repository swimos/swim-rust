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

#[cfg(test)]
mod simple {
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::Future;

    use crate::future::retryable::strategy::RetryStrategy;
    use crate::future::retryable::{Retry, RetryContext, RetryableFuture};

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum FutErr {
        Err,
    }

    struct ReqSender<R> {
        resolve_after: usize,
        retries: usize,
        _pd: PhantomData<R>,
    }

    struct OneShotSender<R>
    where
        R: Clone,
    {
        _pd: PhantomData<R>,
        resolve: bool,
    }

    impl<R> Future for OneShotSender<R>
    where
        R: Clone + Unpin,
    {
        type Output = Result<(), FutErr>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            match &self.resolve {
                true => Poll::Ready(Ok(())),
                false => Poll::Ready(Err(FutErr::Err)),
            }
        }
    }

    impl<R> RetryableFuture for ReqSender<R>
    where
        R: Clone + Unpin + Send + 'static,
    {
        type Ok = ();
        type Err = FutErr;
        type Future = OneShotSender<R>;

        fn future(self: &mut Self, _ctx: &mut RetryContext<Self>) -> Self::Future {
            let resolve = self.retries == self.resolve_after;

            OneShotSender {
                _pd: PhantomData,
                resolve,
            }
        }

        fn retry(&mut self, _ctx: &mut RetryContext<Self>) -> bool {
            self.retries += 1;
            true
        }
    }

    #[tokio::test]
    async fn test_recover() {
        let retry = Retry::new(
            ReqSender::<i32> {
                resolve_after: 2,
                retries: 0,
                _pd: Default::default(),
            },
            RetryStrategy::immediate(2),
        )
        .await;

        assert_eq!(Ok(()), retry)
    }

    #[tokio::test]
    async fn test_ok() {
        let retry = Retry::new(
            ReqSender::<i32> {
                resolve_after: 0,
                retries: 0,
                _pd: Default::default(),
            },
            RetryStrategy::immediate(2),
        )
        .await;

        assert_eq!(Ok(()), retry)
    }

    #[tokio::test]
    async fn test_err() {
        let retry = Retry::new(
            ReqSender::<i32> {
                resolve_after: 1337,
                retries: 0,
                _pd: Default::default(),
            },
            RetryStrategy::immediate(2),
        )
        .await;

        assert_eq!(Err(FutErr::Err), retry)
    }
}

#[cfg(test)]
mod tokio {
    use futures::task::{Context, Poll};
    use futures::Future;
    use tokio::macros::support::Pin;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TrySendError;

    use crate::future::retryable::strategy::RetryStrategy;
    use crate::future::retryable::{Retry, RetryContext, RetryableFuture};

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum SendErr {
        Err,
    }

    struct TestSender<P>
    where
        P: Clone,
    {
        tx: mpsc::Sender<P>,
        payload: P,
    }

    struct SendFuture<P>
    where
        P: Clone,
    {
        tx: mpsc::Sender<P>,
        payload: P,
    }

    impl<P> Future for SendFuture<P>
    where
        P: Clone + Unpin,
    {
        type Output = Result<P, SendErr>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let SendFuture { tx, payload } = self.get_mut();

            match tx.poll_ready(cx) {
                Poll::Ready(Ok(_)) => match tx.try_send(payload.clone()) {
                    Ok(_) => Poll::Ready(Ok(payload.clone())),
                    Err(TrySendError::Closed(_)) => Poll::Ready(Err(SendErr::Err)),
                    Err(TrySendError::Full(_)) => unreachable!(),
                },

                Poll::Ready(Err(_)) => Poll::Ready(Err(SendErr::Err)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<P> RetryableFuture for TestSender<P>
    where
        P: Clone + Send + Unpin + 'static,
    {
        type Ok = P;
        type Err = SendErr;
        type Future = SendFuture<P>;

        fn future(&mut self, _ctx: &mut RetryContext<Self>) -> Self::Future {
            SendFuture {
                tx: self.tx.clone(),
                payload: self.payload.clone(),
            }
        }

        fn retry(&mut self, _ctx: &mut RetryContext<Self>) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_send() {
        let payload = 5;
        let (tx, mut rx) = mpsc::channel(1);
        let retry = Retry::new(TestSender { tx, payload }, RetryStrategy::immediate(2)).await;

        assert!(retry.is_ok());
        let result = rx.recv().await;
        assert_eq!(payload, result.unwrap())
    }
}
