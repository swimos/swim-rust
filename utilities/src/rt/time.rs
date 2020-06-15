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

pub mod delay {
    use futures::task::{Context, Poll};
    use futures::Future;
    use pin_project::*;
    use std::fmt::Debug;
    use std::pin::Pin;
    use std::time::Duration;

    #[pin_project]
    pub struct Delay {
        #[pin]
        f: Pin<Box<dyn Future<Output = ()> + Unpin + Send>>,
    }

    impl Future for Delay {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.project().f.poll(cx)
        }
    }

    pub fn delay_for(duration: Duration) -> Delay {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let delay = tokio::time::delay_for(duration);

            Delay {
                f: Pin::new(Box::new(delay)),
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let delay = wasm_timer::Delay::new(duration);
            let delay = WasmTimerWrapper { f: delay };

            Delay {
                f: Pin::new(Box::new(delay)),
            }
        }
    }

    #[pin_project]
    struct WasmTimerWrapper<F> {
        #[pin]
        f: F,
    }

    impl<F, O, E> Future for WasmTimerWrapper<F>
    where
        F: Future<Output = Result<O, E>>,
        E: Debug,
    {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.project().f.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(_)) => Poll::Ready(()),
                Poll::Ready(Err(e)) => panic!("{:?}", e),
            }
        }
    }
}
