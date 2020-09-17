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

use futures::future::FutureObj;
use futures::stream::FusedStream;
use futures::task::{Context, Poll, Spawn, SpawnError};
use futures::{Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

pub struct TokioSpawn;

pub struct TokioSpawner<Fut: Future> {
    _consume: PhantomData<fn(Fut)>,
    reporter: mpsc::Sender<Fut::Output>,
    receiver: mpsc::Receiver<Fut::Output>,
    count: AtomicUsize,
}

impl Spawn for TokioSpawn {
    fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        tokio::task::spawn(future);
        Ok(())
    }
}

pub trait Spawner<F: Future>: FusedStream<Item = F::Output> {
    fn add(&self, fut: F);

    fn is_empty(&self) -> bool;
}

impl<F> Spawner<F> for FuturesUnordered<F>
where
    F: Future + Send + 'static,
{
    fn add(&self, fut: F) {
        self.push(fut);
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<Fut> Stream for TokioSpawner<Fut>
where
    Fut: Future + Send + 'static,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let TokioSpawner {
            receiver, count, ..
        } = self.get_mut();
        let poll_result = receiver.poll_next_unpin(cx);
        if matches!(poll_result, Poll::Ready(Some(_))) {
            count.fetch_sub(1, Ordering::Relaxed);
        }
        poll_result
    }
}

impl<Fut> FusedStream for TokioSpawner<Fut>
where
    Fut: Future + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }
}

impl<Fut> Spawner<Fut> for TokioSpawner<Fut>
where
    Fut: Future + Send + Sync + 'static,
    Fut::Output: Send,
{
    fn add(&self, fut: Fut) {
        let mut tx = self.reporter.clone();
        let task = async move {
            let _ = tx.send(fut.await).await;
        };
        self.count.fetch_add(1, Ordering::Relaxed);
        tokio::task::spawn(task);
    }

    fn is_empty(&self) -> bool {
        self.count.load(Ordering::Relaxed) == 0
    }
}
