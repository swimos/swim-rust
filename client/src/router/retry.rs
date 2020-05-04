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

use futures::ready;
use futures::task::{Context, Poll};
use futures::Future;
use tokio::macros::support::Pin;

use pin_project::pin_project;
use utilities::future::retryable::ResettableFuture;

use crate::router::RoutingError;

/// A retryable request used by the router. The [`RetryableRequest`] is provided with a future factory
/// which is used for creating new requests after a failure. The factory is a function provided with
/// a [`bool`] which determines if the instance that is being created is the first [`true`] or a
/// retry [`false`].
#[pin_project]
pub struct RetryableRequest<F, Fac> {
    factory: Fac,
    #[pin]
    f: F,
    err: Option<RetryErr>,
}

impl<F, Fac> RetryableRequest<F, Fac>
where
    Fac: Fn(bool) -> F,
{
    pub fn new(factory: Fac) -> Self {
        let f = (factory)(false);

        RetryableRequest {
            factory,
            f,
            err: None,
        }
    }
}

impl<F, Fac> ResettableFuture for RetryableRequest<F, Fac>
where
    Fac: Fn(bool) -> F,
{
    fn reset(self: Pin<&mut Self>) -> bool {
        let mut this = self.project();

        match &this.err {
            None => {
                let future = (this.factory)(true);
                this.f.set(future);

                true
            }
            Some(e) => match e {
                RetryErr::Transient => {
                    let future = (this.factory)(true);
                    this.f.set(future);

                    true
                }
                RetryErr::Permanent => false,
            },
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum RetryErr {
    Transient,
    Permanent,
}

impl<F, Fac, O> Future for RetryableRequest<F, Fac>
where
    F: Future<Output = Result<O, RoutingError>>,
{
    type Output = Result<O, RoutingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match ready!(this.f.poll(cx)) {
            Ok(o) => Poll::Ready(Ok(o)),
            Err(e) => match e {
                RoutingError::Transient | RoutingError::ConnectionError => {
                    *this.err = Some(RetryErr::Transient);
                    Poll::Ready(Err(e))
                }
                e => {
                    *this.err = Some(RetryErr::Permanent);
                    Poll::Ready(Err(e))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use futures::TryFutureExt;

    use utilities::future::retryable::strategy::RetryStrategy;
    use utilities::future::retryable::RetryableFuture;

    use crate::router::retry::RetryableRequest;
    use crate::router::RoutingError;

    #[tokio::test]
    async fn test_chaining_err() {
        let f = |_| {
            async {
                if true {
                    return Err(RoutingError::Transient);
                }
                Ok(1)
            }
            .and_then(|_| async { Ok(1) })
        };

        let retryable = RetryableRequest::new(f);
        let retry = RetryableFuture::new(
            retryable,
            RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()),
        );
        let r = retry.await;

        assert_eq!(r, Err(RoutingError::Transient))
    }

    #[tokio::test]
    async fn test_chaining_ok() {
        let f = |_| async { Ok(1) }.and_then(|_| async { Ok(2) });
        let retryable = RetryableRequest::new(f);
        let retry = RetryableFuture::new(
            retryable,
            RetryStrategy::immediate(NonZeroUsize::new(1).unwrap()),
        );
        let r = retry.await;

        assert_eq!(r.unwrap(), 2)
    }
}
