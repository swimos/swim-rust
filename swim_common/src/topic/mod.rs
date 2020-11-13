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
use std::fmt::{Display, Formatter, Debug};
use std::pin::Pin;
use std::sync::{Arc, Weak};

use crate::request::request_future::{send_and_await, RequestError};
use crate::request::Request;
use crate::sink::item::ItemSink;
use either::Either;
use futures::future::Ready;
use futures::future::{ready, BoxFuture};
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use futures::{future, Future, FutureExt, Stream, StreamExt};
use futures_util::select_biased;
use pin_project::pin_project;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use swim_runtime::task::{spawn, TaskHandle};
use tokio::sync::{broadcast, mpsc, watch};
use utilities::future::{FlatmapStream, SwimStreamExt, TransformMut, TransformOnce};
use utilities::sync::{watch_option_rx_to_stream, broadcast_rx_to_stream};

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

    /// Asynchronously add a new subscriber to the topic.
    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>>;

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

    /// Create a new topic that transforms the subscription streams.
    fn transform<Trans>(self, transform: Trans) -> TransformedTopic<T, Self, Trans>
    where
        Self: Sized,
        Trans: TransformMut<T> + Clone + Send + 'static,
        Trans::Out: Stream + Send + 'static,
    {
        TransformedTopic::new(self, transform)
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
    pub fn send(&self, value: T) -> Result<(), broadcast::error::SendError<T>> {
        match self.sender.upgrade() {
            Some(inner) => {
                //A failure here just means that there are no subscribers so we can safely
                //ignore it.
                let _ = broadcast::Sender::send(&*inner, value);
                Ok(())
            }
            _ => Err(broadcast::error::SendError(value)), //The topic and all subscribers have been dropped.
        }
    }

    pub fn try_into_inner(self) -> Option<broadcast::Sender<T>> {
        self.sender.upgrade().map(|tx| (*tx).clone())
    }
}

impl<'a, T: Send + 'a> ItemSink<'a, T> for BroadcastSender<T> {
    type Error = broadcast::error::SendError<T>;
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

impl<T> BroadcastTopic<T>
where
    T: Clone + Send + 'static,
{
    pub fn new(
        buffer_size: usize,
    ) -> (BroadcastTopic<T>, BroadcastSender<T>, BroadcastReceiver<T>) {
        let (tx, rx) = broadcast::channel(buffer_size);
        let inner = Arc::new(tx);
        let topic = BroadcastTopic {
            sender: inner.clone(),
        };
        let rec = broadcast_rx_to_stream(rx).boxed();
        let sender = BroadcastSender {
            sender: Arc::downgrade(&inner),
        };
        (topic, sender, rec.into())
    }
}

pub struct BroadcastReceiver<T>(BoxStream<'static, T>);

impl<T> From<BoxStream<'static, T>> for BroadcastReceiver<T> {
    fn from(inner: BoxStream<'static, T>) -> Self {
        BroadcastReceiver(inner)
    }
}

impl<T> Stream for BroadcastReceiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl<T> Debug for BroadcastReceiver<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BroadcastReceiver").finish()
    }
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

pub struct WatchTopicReceiver<T>(BoxStream<'static, T>);

impl<T> From<BoxStream<'static, T>> for WatchTopicReceiver<T> {
    fn from(inner: BoxStream<'static, T>) -> Self {
        WatchTopicReceiver(inner)
    }
}

impl<T> Stream for WatchTopicReceiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl<T> Debug for WatchTopicReceiver<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WatchTopicReceiver").finish()
    }
}

impl<T> WatchTopic<T>
where
    T: Send + Sync + Clone + 'static,
{
    pub fn new(receiver: watch::Receiver<Option<T>>) -> (Self, WatchTopicReceiver<T>) {
        let duplicate = receiver.clone();
        let topic = WatchTopic { receiver };
        let rec = watch_option_rx_to_stream(duplicate).boxed();
        (topic, rec.into())
    }
}



impl<T: Clone + Send + Sync + 'static> Topic<T> for WatchTopic<T> {
    type Receiver = WatchTopicReceiver<T>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        ready(Ok(watch_option_rx_to_stream(self.receiver.clone()).boxed().into()))
        .boxed()
    }
}

impl<T: Clone + Send + Sync + 'static> Topic<T> for BroadcastTopic<T> {
    type Receiver = BroadcastReceiver<T>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        let rx = self.sender.subscribe();
        ready(Ok(broadcast_rx_to_stream(rx).boxed().into()))
            .boxed()
    }
}

impl<T: Clone + Send + 'static> Topic<T> for MpscTopic<T> {
    type Receiver = MpscTopicReceiver<T>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        async move {
            let MpscTopic { sub_sender, .. } = self;
            Ok(send_and_await(sub_sender).await?)
        }
        .boxed()
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

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        BoxResult {
            f: self.0.subscribe(),
        }
        .boxed()
    }
}

/// The type of boxed topics.
pub type BoxTopic<T> = Box<dyn Topic<T, Receiver = BoxStream<'static, T>>>;

impl<T: Clone + 'static> Topic<T> for BoxTopic<T> {
    type Receiver = BoxStream<'static, T>;

    fn subscribe(&mut self) -> BoxFuture<Result<BoxStream<'static, T>, TopicError>> {
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

/// Transforms a single topic subscription.
pub struct ApplyTopicTransform<Trans>(Trans);

impl<Str, Trans> TransformOnce<Result<Str, TopicError>> for ApplyTopicTransform<Trans>
where
    Str: Stream,
    Trans: TransformMut<Str::Item> + Clone,
    Trans::Out: Stream,
{
    type Out = Result<FlatmapStream<Str, Trans>, TopicError>;

    fn transform(self, result: Result<Str, TopicError>) -> Self::Out {
        result.map(|input| input.transform_flat_map(self.0))
    }
}

/// Applies a transformation to the stream produced when subscribing to a topic.
pub struct TransformedTopic<T, Top: Topic<T>, Trans> {
    _type: PhantomData<fn(T) -> T>,
    inner: Top,
    transform: Trans,
}

impl<T, Top, Trans> TransformedTopic<T, Top, Trans>
where
    Top: Topic<T>,
    Trans: TransformMut<T> + Clone,
    Trans::Out: Stream,
{
    pub fn new(inner: Top, transform: Trans) -> Self {
        TransformedTopic {
            _type: PhantomData,
            inner,
            transform,
        }
    }
}

impl<T, Top, Trans> Topic<<<Trans as TransformMut<T>>::Out as Stream>::Item>
    for TransformedTopic<T, Top, Trans>
where
    Top: Topic<T> + Send,
    Trans: TransformMut<T> + Clone + Send + 'static,
    Trans::Out: Stream + Send + 'static,
{
    type Receiver = FlatmapStream<Top::Receiver, Trans>;

    fn subscribe(&mut self) -> BoxFuture<Result<Self::Receiver, TopicError>> {
        let trans = self.transform.clone();
        async move {
            self.inner
                .subscribe()
                .await
                .map(|rec| rec.transform_flat_map(trans))
        }
        .boxed()
    }
}
