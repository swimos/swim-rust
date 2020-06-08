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
use futures::{Future, FutureExt};
use pin_project::*;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct TaskError;

#[pin_project]
#[derive(Debug)]
pub struct TaskHandle<R> {
    #[pin]
    inner: oneshot::Receiver<R>,
}

impl<R> Future for TaskHandle<R> {
    type Output = Result<R, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => Poll::Ready(r.map_err(|_| TaskError)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F>(f: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + Debug + 'static,
{
    let (tx, rx) = oneshot::channel();
    // todo: remove unwraps + debug requirement
    let _f = tokio::spawn(f.then(|r| async {
        tx.send(r).unwrap();
    }));

    TaskHandle { inner: rx }
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F>(f: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + Debug + 'static,
{
    let (tx, rx) = oneshot::channel();
    // todo: remove unwraps + debug requirement
    let f = f.then(|r| async {
        tx.send(r);
    });
    wasm_bindgen_futures::spawn_local(f);

    TaskHandle { inner: rx }
}

#[tokio::test]
async fn t() {
    let f = async {
        panic!("1");
    };
    let r = tokio::spawn(f).await;
    println!("{:?}", r);
}
