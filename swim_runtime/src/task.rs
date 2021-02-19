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

use futures::Future;
use std::fmt::Debug;

#[cfg(not(target_arch = "wasm32"))]
pub use self::tokio::TaskHandle;
#[cfg(target_arch = "wasm32")]
pub use self::wasm::TaskHandle;

#[derive(Debug)]
pub struct TaskError;

/// Spawns a new asynchronous task on the current runtime. a `TaskHandle` is returned that
/// corresponds to the current runtime. See the documentation for the underlying executor for
/// guarantees that are made about the executing of the task.
pub fn spawn<F>(f: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + Debug + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    {
        self::tokio::spawn(f)
    }

    #[cfg(target_arch = "wasm32")]
    {
        self::wasm::spawn(f)
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::TaskError;
    use futures::Future;
    use pin_project::*;
    use std::fmt::Debug;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::sync::oneshot;

    /// A handle to a returned task by the current runtime.
    #[pin_project]
    #[derive(Debug)]
    pub struct TaskHandle<R> {
        /// As WASM futures may not return a value, when a future is spawned on the WASM runtime it
        /// the returned value is sent over a channel to this [`TaskHandle`] and awaiting its
        /// termination awaits the value on the channel.
        #[pin]
        inner: oneshot::Receiver<R>,
    }

    pub fn spawn<F>(f: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + Debug + 'static,
    {
        let (task, task_handle) = Task::new(f);
        wasm_bindgen_futures::spawn_local(task);
        task_handle
    }

    impl<R> Future for TaskHandle<R> {
        type Output = Result<R, TaskError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.project().inner.poll(cx) {
                Poll::Pending => Poll::Pending,
                // If the sender has been dropped then the task has panicked.
                Poll::Ready(r) => Poll::Ready(r.map_err(|_| TaskError)),
            }
        }
    }

    #[pin_project]
    struct Task<F>
    where
        F: Future,
    {
        #[pin]
        tx: Option<oneshot::Sender<F::Output>>,
        #[pin]
        f: F,
    }

    impl<F> Task<F>
    where
        F: Future,
    {
        fn new(f: F) -> (Task<F>, TaskHandle<F::Output>) {
            let (tx, rx) = oneshot::channel();
            let task = Task { tx: Some(tx), f };
            let task_handle = TaskHandle { inner: rx };

            (task, task_handle)
        }
    }

    impl<F> Future for Task<F>
    where
        F: Future,
    {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut this = self.project();

            match this.f.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(r) => match this.tx.take() {
                    Some(sender) => {
                        let _ = sender.send(r);
                        Poll::Ready(())
                    }
                    None => panic!("Future used twice"),
                },
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod tokio {
    use super::TaskError;
    use futures::task::{Context, Poll};
    use futures::Future;
    use pin_project::*;
    use std::fmt::Debug;
    use std::pin::Pin;
    use tokio::task::JoinHandle;

    #[pin_project]
    #[derive(Debug)]
    pub struct TaskHandle<R> {
        #[pin]
        inner: JoinHandle<R>,
    }

    impl<R> Future for TaskHandle<R> {
        type Output = Result<R, TaskError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.project().inner.poll(cx).map_err(|_| TaskError)
        }
    }

    pub fn spawn<F>(f: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + Debug + 'static,
    {
        let jh = tokio::spawn(f);
        TaskHandle { inner: jh }
    }
}

#[cfg(test)]
mod tests {
    use super::spawn;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn panic_ok() {
        let f = async { panic!() };
        let handle = spawn(f);
        let r = handle.await;

        assert!(r.is_err());
    }

    #[tokio::test]
    async fn ok() {
        let f = async { 5 };
        let handle = spawn(f);
        let r = handle.await;

        assert_eq!(r.unwrap(), 5);
    }

    #[tokio::test]
    async fn ok_dropped_handle() {
        let (tx, rx) = oneshot::channel();
        {
            let f = async { tx.send(5) };
            let _ = spawn(f);
        }

        assert_eq!(rx.await.unwrap(), 5);
    }
}
