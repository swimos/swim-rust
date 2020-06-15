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

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Weak};

use crate::request::request_future::{send_and_await, RequestError, SendAndAwait};
use crate::request::Request;
use crate::sink::item::ItemSink;
use either::Either;
use futures::future::{ready, BoxFuture};
use futures::future::{ErrInto, Ready};
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use futures::{future, ready, Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures_util::select_biased;
use pin_project::pin_project;
use std::num::NonZeroUsize;
use tokio::sync::broadcast::RecvError;
use tokio::sync::{broadcast, mpsc, watch};
use utilities::rt::task::{spawn, TaskHandle};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TopicError {
    TopicClosed,
}

impl Display for TopicError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "All receivers for the topic were dropped.")
    }
}

impl Error for TopicError {}

impl From<RequestError> for TopicError {
    fn from(_: RequestError) -> Self {
        TopicError::TopicClosed
    }
}

/// A trait for one-to many channels. A topic may have any number of subscribers which are added
/// asynchronously using the `subscribe` method. Each subscription can be consumed as a stream.
pub trait Topic<T> {
    type Receiver: Stream<Item = T> + Send + 'static;
    type Fut: Future<Output = Result<Self::Receiver, TopicError>> + Send + 'static;

    /// Asynchronously add a new subscriber to the topic.
    fn subscribe(&mut self) -> Self::Fut;

    /// Box the topic so that it can be subscribed to dynamically and will hand out boxed
    /// streams to subscribers.
    fn boxed_topic(self) -> BoxTopic<T>
    where
        T: Send + 'static,
        Self: Sized + 'static,
    {
        let boxing_topic = BoxingTopic(self);
        let boxed: BoxTopic<T> = Box::new(boxing_topic);
        boxed
    }
}

#[derive(Clone, Debug)]
pub struct WatchStream<T> {
    inner: watch::Receiver<Option<T>>,
}

impl<T: Clone> WatchStream<T> {
    pub async fn recv(&mut self) -> Option<T> {
        match self.inner.recv().await {
            Some(Some(t)) => Some(t),
            _ => None,
        }
    }
}

impl<T: Clone> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().inner.poll_next_unpin(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Some(t))) => Poll::Ready(Some(t)),
            _ => Poll::Pending,
        }
    }
}

/// A topic implementation backed by a Tokio watch channel. Subscribers will only see the latest
/// output record since the last time the polled and so may (and likely will) miss outputs.
#[derive(Debug)]
pub struct WatchTopic<T> {
    receiver: watch::Receiver<Option<T>>,
}

impl<T> Clone for WatchTopic<T> {
    fn clone(&self) -> Self {
        WatchTopic {
            receiver: self.receiver.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WatchTopicReceiver<T> {
    receiver: WatchStream<T>,
}

/// A topic implementation backed by a Tokio broadcast channel. The topic has an intermediate
/// queue from which all subscribers read. If the queue fills (records are being produced faster
/// than they are consumed) the topic will begin discarding records, starting with the oldest.
#[derive(Debug)]
pub struct BroadcastTopic<T> {
    pub sender: Arc<broadcast::Sender<T>>,
}

#[derive(Debug)]
pub struct BroadcastSender<T> {
    sender: Weak<broadcast::Sender<T>>,
}

impl<T> BroadcastSender<T> {
    pub fn send(&self, value: T) -> Result<(), broadcast::SendError<T>> {
        match self.sender.upgrade() {
            Some(inner) => {
                //A failure here just means that there are no subscribers so we can safely
                //ignore it.
                let _ = broadcast::Sender::send(&*inner, value);
                Ok(())
            }
            _ => Err(broadcast::SendError(value)), //The topic and all subscribers have been dropped.
        }
    }
}

impl<'a, T: Send + 'a> ItemSink<'a, T> for BroadcastSender<T> {
    type Error = broadcast::SendError<T>;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        ready(self.send(value))
    }
}

impl<T> Clone for BroadcastTopic<T> {
    fn clone(&self) -> Self {
        BroadcastTopic {
            sender: self.sender.clone(),
        }
    }
}

impl<T: Clone> BroadcastTopic<T> {
    pub fn new(
        buffer_size: usize,
    ) -> (BroadcastTopic<T>, BroadcastSender<T>, BroadcastReceiver<T>) {
        let (tx, rx) = broadcast::channel(buffer_size);
        let inner = Arc::new(tx);
        let topic = BroadcastTopic {
            sender: inner.clone(),
        };
        let rec = BroadcastReceiver {
            sender: inner.clone(),
            receiver: rx,
        };
        let sender = BroadcastSender {
            sender: Arc::downgrade(&inner),
        };
        (topic, sender, rec)
    }
}

#[derive(Debug)]
pub struct BroadcastReceiver<T> {
    sender: Arc<broadcast::Sender<T>>,
    receiver: broadcast::Receiver<T>,
}

type SubRequest<T> = Request<MpscTopicReceiver<T>>;

#[derive(Debug)]
pub struct MpscTopicReceiver<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T> MpscTopicReceiver<T> {
    fn new(receiver: mpsc::Receiver<T>) -> MpscTopicReceiver<T> {
        MpscTopicReceiver { receiver }
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }
}

impl<T: Clone + Send> Stream for MpscTopicReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_next_unpin(cx)
    }
}

/// A topic where every subscriber is represented by a Tokio MPSC queue. If any one subscriber falls
/// behind, all of the subscribers will block until it catches up.
#[derive(Debug)]
pub struct MpscTopic<T> {
    sub_sender: mpsc::Sender<SubRequest<T>>,
    task: Arc<TaskHandle<()>>,
}

impl<T> Clone for MpscTopic<T> {
    fn clone(&self) -> Self {
        MpscTopic {
            sub_sender: self.sub_sender.clone(),
            task: self.task.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> MpscTopic<T> {
    pub fn new(
        input: mpsc::Receiver<T>,
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
    ) -> (MpscTopic<T>, MpscTopicReceiver<T>) {
        let (sub_tx, sub_rx) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(buffer_size.get());

        let task_fut = mpsc_topic_task(input, tx, sub_rx, buffer_size, yield_after);
        let task = spawn(task_fut);
        (
            MpscTopic {
                sub_sender: sub_tx,
                task: Arc::new(task),
            },
            MpscTopicReceiver::new(rx),
        )
    }
}

impl<T: Clone + Send> WatchTopic<T> {
    pub fn new(receiver: watch::Receiver<Option<T>>) -> (Self, WatchTopicReceiver<T>) {
        let duplicate = receiver.clone();
        let topic = WatchTopic { receiver };
        let rec = WatchTopicReceiver {
            receiver: WatchStream { inner: duplicate },
        };
        (topic, rec)
    }
}

impl<T: Clone + Send> Stream for WatchTopicReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_next_unpin(cx)
    }
}

impl<T: Clone + Send + Sync + 'static> Topic<T> for WatchTopic<T> {
    type Receiver = WatchTopicReceiver<T>;
    type Fut = Ready<Result<Self::Receiver, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        ready(Ok(WatchTopicReceiver {
            receiver: WatchStream {
                inner: self.receiver.clone(),
            },
        }))
    }
}

impl<T: Clone> Stream for BroadcastReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut pinned_rec = Pin::new(&mut self.get_mut().receiver);
        loop {
            match ready!(pinned_rec.as_mut().poll_next(cx)) {
                Some(Ok(t)) => return Poll::Ready(Some(t)),
                Some(Err(RecvError::Lagged(_))) => {
                    continue;
                }
                _ => return Poll::Ready(None),
            }
        }
    }
}

impl<T: Clone> Clone for BroadcastReceiver<T> {
    fn clone(&self) -> Self {
        let BroadcastReceiver { sender, .. } = self;
        BroadcastReceiver {
            sender: sender.clone(),
            receiver: sender.subscribe(),
        }
    }
}

impl<T: Clone + Send + 'static> Topic<T> for BroadcastTopic<T> {
    type Receiver = BroadcastReceiver<T>;
    type Fut = Ready<Result<Self::Receiver, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        let result = Ok(BroadcastReceiver {
            sender: self.sender.clone(),
            receiver: self.sender.subscribe(),
        });
        ready(result)
    }
}

impl<T: Clone + Send + 'static> Topic<T> for MpscTopic<T> {
    type Receiver = MpscTopicReceiver<T>;
    type Fut = ErrInto<SendAndAwait<Request<Self::Receiver>, Self::Receiver>, TopicError>;

    fn subscribe(&mut self) -> Self::Fut {
        let MpscTopic { sub_sender, .. } = self;

        let fut: SendAndAwait<Request<Self::Receiver>, Self::Receiver> = send_and_await(sub_sender);

        TryFutureExt::err_into::<TopicError>(fut)
    }
}

/// The internal task that keeps the queues of an MPSC topic filled.
async fn mpsc_topic_task<T: Clone>(
    input: mpsc::Receiver<T>,
    init_sender: mpsc::Sender<T>,
    subscriptions: mpsc::Receiver<SubRequest<T>>,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
) {
    let mut subs_fused = subscriptions.fuse();
    let mut in_fused = input.fuse();

    let mut outputs: Vec<mpsc::Sender<T>> = vec![init_sender];

    let yield_mod = yield_after.get();
    let mut iteration_count: usize = 0;

    loop {
        let item = select_biased! {
            sub_req = subs_fused.next() => sub_req.map(Either::Left),
            value = in_fused.next() => value.map(Either::Right),
        };
        match item {
            Some(Either::Left(req)) => {
                let (tx, rx) = mpsc::channel(buffer_size.get());
                if req.send(MpscTopicReceiver::new(rx)).is_ok() {
                    outputs.push(tx);
                }
            }
            Some(Either::Right(value)) => match outputs.len() {
                0 => {}
                1 => {
                    let result = outputs[0].send(value).await;
                    if result.is_err() {
                        outputs.clear();
                    }
                }
                _ => {
                    let results = future::join_all(
                        outputs.iter_mut().map(|sender| sender.send(value.clone())),
                    )
                    .await;

                    let num_terminated = results.iter().filter(|r| r.is_err()).count();
                    if num_terminated > 0 {
                        outputs = remove_terminated(results, std::mem::take(&mut outputs));
                        if outputs.is_empty() {
                            break;
                        }
                    }
                }
            },
            _ => {
                break;
            }
        }
        iteration_count += 1;
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
}

fn remove_terminated<T: Clone>(
    results: Vec<Result<(), mpsc::error::SendError<T>>>,
    old_outputs: Vec<mpsc::Sender<T>>,
) -> Vec<mpsc::Sender<T>> {
    old_outputs
        .into_iter()
        .zip(results.into_iter())
        .filter_map(|(sender, result)| match result {
            Ok(_) => Some(sender),
            _ => None,
        })
        .collect()
}

#[pin_project]
pub struct BoxResult<F> {
    #[pin]
    f: F,
}

impl<S, T, F> Future for BoxResult<F>
where
    F: Future<Output = Result<S, TopicError>>,
    S: Stream<Item = T> + Send + 'static,
{
    type Output = Result<BoxStream<'static, T>, TopicError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().f.poll(cx) {
            Poll::Ready(stream_result) => Poll::Ready(stream_result.map(StreamExt::boxed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A topic that boxes the subscription method return type for a wrapped topic.
struct BoxingTopic<Top>(Top);

impl<T: Send + 'static, Top: Topic<T>> Topic<T> for BoxingTopic<Top> {
    type Receiver = BoxStream<'static, T>;
    type Fut = BoxFuture<'static, Result<Self::Receiver, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        FutureExt::boxed(BoxResult {
            f: self.0.subscribe(),
        })
    }
}

/// The type of boxed topics.
pub type BoxTopic<T> = Box<
    dyn Topic<
        T,
        Receiver = BoxStream<'static, T>,
        Fut = BoxFuture<'static, Result<BoxStream<'static, T>, TopicError>>,
    >,
>;

impl<T: Clone + 'static> Topic<T> for BoxTopic<T> {
    type Receiver = BoxStream<'static, T>;
    type Fut = BoxFuture<'static, Result<BoxStream<'static, T>, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        (**self).subscribe()
    }

    fn boxed_topic(self) -> BoxTopic<T>
    where
        T: Send + 'static,
        Self: Sized + 'static,
    {
        self
    }
}
