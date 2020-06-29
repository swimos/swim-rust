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

/// [`Delay`] is a future that will complete at a specific time.
pub mod delay {
    use futures::task::{Context, Poll};
    use futures::Future;
    use pin_project::*;
    use std::pin::Pin;
    use std::time::Duration;

    #[pin_project]
    pub struct Delay {
        #[cfg(not(target_arch = "wasm32"))]
        #[pin]
        inner: tokio::time::Delay,
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
                inner: tokio::time::delay_for(duration),
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

/// [`Interval`] is a stream that yields a value when the provided [`period`] elapses.
pub mod interval {
    use futures::task::{Context, Poll};
    use futures::Stream;
    use pin_project::*;
    use std::pin::Pin;
    use std::time::Duration;

    #[pin_project(project = IntervalProject)]
    #[derive(Debug)]
    pub struct Interval {
        #[cfg(not(target_arch = "wasm32"))]
        #[pin]
        inner: tokio::time::Interval,
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

    /// Create a new [`Interval`] that will yield a value each time the provided [`period`] elapses.
    pub fn interval(period: Duration) -> Interval {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let inner = tokio::time::interval(period);
            Interval { inner }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let inner = wasm_timer::Interval::new(period);
            Interval { inner }
        }
    }
}

/// An [`Instant`] is a measurement of the system clock.
pub mod instant {
    use std::time::Duration;

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
