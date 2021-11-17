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

pub mod delay {
    use futures::task::{Context, Poll};
    use futures::Future;
    use pin_project::*;
    use std::pin::Pin;
    use std::time::Duration;

    /// `Delay` is a future that will complete at a specific time.
    #[pin_project]
    pub struct Delay {
        #[cfg(not(target_arch = "wasm32"))]
        #[pin]
        inner: tokio::time::Sleep,
        #[cfg(target_arch = "wasm32")]
        #[pin]
        inner: wasm_timer::Delay,
    }

    impl Future for Delay {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.project().inner.poll(cx)
            }
            #[cfg(target_arch = "wasm32")]
            {
                match self.project().inner.poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(_)) => Poll::Ready(()),
                    Poll::Ready(Err(e)) => panic!("{:?}", e),
                }
            }
        }
    }

    /// Creates a new [`Delay`] instance that will complete at the current time + the provided
    /// [`Duration`].
    pub fn delay_for(duration: Duration) -> Delay {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Delay {
                inner: tokio::time::sleep(duration),
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            Delay {
                inner: wasm_timer::Delay::new(duration),
            }
        }
    }
}

pub mod interval {
    use futures::task::{Context, Poll};
    use futures::Stream;
    use pin_project::*;
    use std::pin::Pin;
    use std::time::Duration;

    /// [`Interval`] is a stream that yields a value when the provided period elapses.
    #[pin_project(project = IntervalProject)]
    #[derive(Debug)]
    pub struct Interval {
        #[cfg(not(target_arch = "wasm32"))]
        #[pin]
        inner: tokio_stream::wrappers::IntervalStream,
        #[cfg(target_arch = "wasm32")]
        #[pin]
        inner: wasm_timer::Interval,
    }

    impl Stream for Interval {
        type Item = ();

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.project().inner.poll_next(cx).map(|_| Some(()))
            }
            #[cfg(target_arch = "wasm32")]
            {
                self.project().inner.poll_next(cx)
            }
        }
    }

    /// Create a new [`Interval`] that will yield a value each time the provided `period` elapses.
    pub fn interval(period: Duration) -> Interval {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let inner = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(period));
            Interval { inner }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let inner = wasm_timer::Interval::new(period);
            Interval { inner }
        }
    }
}

pub mod instant {
    use std::time::Duration;

    /// An [`Instant`] is a measurement of the system clock.
    #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
    pub struct Instant {
        #[cfg(not(target_arch = "wasm32"))]
        inner: tokio::time::Instant,
        #[cfg(target_arch = "wasm32")]
        inner: wasm_timer::Instant,
    }

    impl Instant {
        /// Construct a new [`Instant`] corresponding to "now".
        pub fn now() -> Instant {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let inner = tokio::time::Instant::now();
                Instant { inner }
            }

            #[cfg(target_arch = "wasm32")]
            {
                let inner = wasm_timer::Instant::now();
                Instant { inner }
            }
        }

        /// Returns the amount of time (as a [`Duration`] that has passed since this [`Instant`]
        /// was created.  
        pub fn elapsed(self) -> Duration {
            self.inner.elapsed()
        }
    }
}

pub mod clock {

    use super::delay;
    use std::fmt::Debug;
    use std::future::Future;
    use std::time::Duration;

    /// Trait for tracking the passage of time in asynchronous code. Implementations should ensure that
    /// time is monotonically non-decreasing.
    pub trait Clock: Debug + Clone + Send + Sync + 'static {
        /// The type of futures tracking a delay.
        type DelayFuture: Future<Output = ()> + Send + 'static;

        /// Create a future that will complete after a fixed delay.
        fn delay(&self, duration: Duration) -> Self::DelayFuture;
    }

    #[derive(Debug, Clone)]
    pub struct RuntimeClock;

    impl Clock for RuntimeClock {
        type DelayFuture = delay::Delay;

        fn delay(&self, duration: Duration) -> Self::DelayFuture {
            delay::delay_for(duration)
        }
    }

    /// A clock that uses delays as provided by the runtime.
    pub fn runtime_clock() -> RuntimeClock {
        RuntimeClock
    }
}

pub mod timeout {

    use futures::task::{Context, Poll};
    use futures::{ready, Future};
    use pin_project::*;
    use std::pin::Pin;
    use std::time::Duration;

    #[cfg(target_arch = "wasm32")]
    mod dummy {

        use futures::task::{Context, Poll};
        use futures::Future;
        use pin_project::*;
        use std::pin::Pin;

        pub(super) struct DummyErr;

        impl From<std::io::Error> for DummyErr {
            fn from(_: std::io::Error) -> Self {
                DummyErr
            }
        }

        #[pin_project]
        pub(super) struct WithDummy<F>(#[pin] F);

        impl<F> WithDummy<F> {
            pub(super) fn new(future: F) -> Self {
                WithDummy(future)
            }
        }

        impl<F: Future> Future for WithDummy<F> {
            type Output = Result<F::Output, DummyErr>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.project().0.poll(cx).map(Ok)
            }
        }
    }

    #[pin_project]
    pub struct Timeout<F: Future> {
        #[cfg(not(target_arch = "wasm32"))]
        #[pin]
        inner: tokio::time::Timeout<F>,
        #[cfg(target_arch = "wasm32")]
        #[pin]
        inner: wasm_timer::ext::Timeout<dummy::WithDummy<F>>,
    }

    pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Timeout {
                inner: tokio::time::timeout(duration, future),
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            Timeout {
                inner: wasm_timer::ext::TryFutureExt::timeout(
                    dummy::WithDummy::new(future),
                    duration,
                ),
            }
        }
    }

    #[derive(Debug)]
    pub struct TimeoutExpired;

    impl<F: Future> Future for Timeout<F> {
        type Output = Result<F::Output, TimeoutExpired>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let result = ready!(self.project().inner.poll(cx));
            Poll::Ready(result.map_err(|_| TimeoutExpired))
        }
    }
}
