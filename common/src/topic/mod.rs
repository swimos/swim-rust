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
use std::fmt::Display;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use either::Either;
use futures::future::Ready;
use futures::future::{ready, BoxFuture};
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use futures::{future, Future, FutureExt, Stream, StreamExt};
use futures_util::select_biased;
use pin_utils::unsafe_pinned;
use serde::export::Formatter;
use tokio::sync::broadcast::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubscriptionError {
    TopicClosed,
}

impl Display for SubscriptionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            _ => write!(f, "All receivers for the topic were dropped."),
        }
    }
}

impl Error for SubscriptionError {}

/// A trait for one-to many channels. A topic may have any number of subscribers which are added
/// asynchronously using the `subscribe` method. Each subscription can be consumed as a stream.
pub trait Topic<T: Clone> {
    type Receiver: Stream<Item = T> + Send + 'static;
    type Fut: Future<Output = Result<Self::Receiver, SubscriptionError>> + Send + 'static;

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

impl<T> WatchStream<T> {
    unsafe_pinned!(inner: watch::Receiver<Option<T>>);
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
        match self.inner().poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Some(t))) => Poll::Ready(Some(t)),
            _ => Poll::Pending,
        }
    }
}

/// A topic implementation backed by a Tokio watch channel. Subscribers will only see the latest
/// output record since the last time the polled and so may (and likely will) miss outputs.
#[derive(Clone, Debug)]
pub struct WatchTopic<T> {
    receiver: Weak<watch::Receiver<Option<T>>>,
}

#[derive(Clone, Debug)]
pub struct WatchTopicReceiver<T> {
    _owner: Arc<watch::Receiver<Option<T>>>,
    receiver: WatchStream<T>,
}

/// A topic implementation backed by a Tokio broadcast channel. The topic has an intermediate
/// queue from which all subscribers read. If the queue fills (records are being produced faster
/// than they are consumed) the topic will begin discarding records, starting with the oldest.
#[derive(Clone, Debug)]
pub struct BroadcastTopic<T> {
    pub sender: broadcast::Sender<T>,
}

impl<T: Clone> BroadcastTopic<T> {
    pub fn new(
        buffer_size: usize,
    ) -> (
        BroadcastTopic<T>,
        broadcast::Sender<T>,
        BroadcastReceiver<T>,
    ) {
        let (tx, rx) = broadcast::channel(buffer_size);
        let topic = BroadcastTopic { sender: tx.clone() };
        let rec = BroadcastReceiver {
            sender: tx.clone(),
            receiver: rx,
        };
        (topic, tx, rec)
    }
}

#[derive(Debug)]
pub struct BroadcastReceiver<T> {
    sender: broadcast::Sender<T>,
    receiver: broadcast::Receiver<T>,
}

struct SubRequest<T>(oneshot::Sender<mpsc::Receiver<T>>);

impl<T: Clone> BroadcastReceiver<T> {
    unsafe_pinned!(receiver: broadcast::Receiver<T>);
}

/// A topic where every subscriber is represented by a Tokio MPSC queue. If any one subscriber falls
/// behind, all of the subscribers will block until it catches up.
#[derive(Clone, Debug)]
pub struct MpscTopic<T> {
    sub_sender: mpsc::Sender<SubRequest<T>>,
    task: Arc<JoinHandle<()>>,
}

impl<T: Clone + Send + Sync + 'static> MpscTopic<T> {
    pub fn new(input: mpsc::Receiver<T>, buffer_size: usize) -> (MpscTopic<T>, mpsc::Receiver<T>) {
        assert!(buffer_size > 0, "MPSC buffer size must be positive.");
        let (sub_tx, sub_rx) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(buffer_size);
        let task_fut = mpsc_topic_task(input, tx, sub_rx, buffer_size);
        let task = tokio::task::spawn(task_fut);
        (
            MpscTopic {
                sub_sender: sub_tx,
                task: Arc::new(task),
            },
            rx,
        )
    }
}

impl<T: Clone + Send> WatchTopic<T> {
    pub fn new(receiver: watch::Receiver<Option<T>>) -> (Self, WatchTopicReceiver<T>) {
        let duplicate = receiver.clone();
        let owner = Arc::new(receiver);
        let topic = WatchTopic {
            receiver: Arc::downgrade(&owner),
        };
        let rec = WatchTopicReceiver {
            _owner: owner,
            receiver: WatchStream { inner: duplicate },
        };
        (topic, rec)
    }
}

impl<T: Clone + Send> WatchTopicReceiver<T> {
    unsafe_pinned!(receiver: WatchStream<T>);
}

impl<T: Clone + Send> Stream for WatchTopicReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver().poll_next(cx)
    }
}

impl<T: Clone + Send + Sync + 'static> Topic<T> for WatchTopic<T> {
    type Receiver = WatchTopicReceiver<T>;
    type Fut = Ready<Result<Self::Receiver, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        ready(match self.receiver.upgrade() {
            Some(owner) => {
                let receiver = owner.as_ref().clone();
                Ok(WatchTopicReceiver {
                    _owner: owner,
                    receiver: WatchStream { inner: receiver },
                })
            }
            _ => Err(SubscriptionError::TopicClosed),
        })
    }
}

impl<T: Clone> Stream for BroadcastReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut pinned_rec = self.receiver();
        loop {
            match pinned_rec.poll_recv(cx) {
                Poll::Ready(r) => match r {
                    Ok(t) => return Poll::Ready(Some(t)),
                    Err(e) => match e {
                        RecvError::Closed => return Poll::Ready(None),
                        RecvError::Lagged(_) => continue,
                    },
                },
                Poll::Pending => return Poll::Pending,
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
    type Fut = Ready<Result<Self::Receiver, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        let result = if self.sender.receiver_count() == 0 {
            Err(SubscriptionError::TopicClosed)
        } else {
            Ok(BroadcastReceiver {
                sender: self.sender.clone(),
                receiver: self.sender.subscribe(),
            })
        };
        ready(result)
    }
}

pub struct SendRequest<T> {
    sender: mpsc::Sender<SubRequest<T>>,
    request: Option<SubRequest<T>>,
}

impl<T> SendRequest<T> {
    fn new(sender: mpsc::Sender<SubRequest<T>>, request: SubRequest<T>) -> SendRequest<T> {
        SendRequest {
            sender,
            request: Some(request),
        }
    }
}

pub struct Sequenced<F1: Unpin, F2: Unpin> {
    first: Option<F1>,
    second: Option<F2>,
}

impl<F1: Unpin, F2: Unpin> Sequenced<F1, F2> {
    fn new(first: F1, second: F2) -> Sequenced<F1, F2> {
        Sequenced {
            first: Some(first),
            second: Some(second),
        }
    }
}

impl<F1, F2, T1, T2, E1, E2> Future for Sequenced<F1, F2>
where
    F1: Future<Output = Result<T1, E1>> + Unpin,
    F2: Future<Output = Result<T2, E2>> + Unpin,
{
    type Output = Result<T2, SubscriptionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Sequenced {
            first: maybe_first,
            second: maybe_second,
        } = self.get_mut();
        if let Some(first) = maybe_first {
            match Pin::new(first).poll(cx) {
                Poll::Ready(result) => {
                    maybe_first.take();
                    match result {
                        Ok(_) => {}
                        Err(_) => return Poll::Ready(Err(SubscriptionError::TopicClosed)),
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        match maybe_second {
            Some(second) => Pin::new(second)
                .poll(cx)
                .map_err(|_| SubscriptionError::TopicClosed),
            _ => panic!("Sequenced future used twice."),
        }
    }
}

impl<T> Future for SendRequest<T> {
    type Output = Result<(), SubscriptionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SendRequest { sender, request } = self.get_mut();
        sender.poll_ready(cx).map(|r| match r {
            Ok(_) => match request.take() {
                Some(req) => match sender.try_send(req) {
                    Ok(_) => Ok(()),
                    _ => Err(SubscriptionError::TopicClosed),
                },
                _ => panic!("Send future used twice."),
            },
            Err(_) => Err(SubscriptionError::TopicClosed),
        })
    }
}

impl<T: Clone + Send + 'static> Topic<T> for MpscTopic<T> {
    type Receiver = mpsc::Receiver<T>;
    type Fut = Sequenced<SendRequest<T>, oneshot::Receiver<mpsc::Receiver<T>>>;

    fn subscribe(&mut self) -> Self::Fut {
        let MpscTopic { sub_sender, .. } = self;
        let (sub_tx, sub_rx) = oneshot::channel::<mpsc::Receiver<T>>();

        Sequenced::new(
            SendRequest::new(sub_sender.clone(), SubRequest(sub_tx)),
            sub_rx,
        )
    }
}

/// The internal task that keeps the queues of an MPSC topic filled.
async fn mpsc_topic_task<T: Clone>(
    input: mpsc::Receiver<T>,
    init_sender: mpsc::Sender<T>,
    subscriptions: mpsc::Receiver<SubRequest<T>>,
    buffer_size: usize,
) {
    let mut subs_fused = subscriptions.fuse();
    let mut in_fused = input.fuse();

    let mut outputs: Vec<mpsc::Sender<T>> = vec![init_sender];

    loop {
        let item = select_biased! {
            sub_req = subs_fused.next() => sub_req.map(Either::Left),
            value = in_fused.next() => value.map(Either::Right),
        };
        match item {
            Some(Either::Left(SubRequest(cb))) => {
                let (tx, rx) = mpsc::channel(buffer_size);
                if cb.send(rx).is_ok() {
                    outputs.push(tx);
                }
            }
            Some(Either::Right(value)) => match outputs.len() {
                0 => break,
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

pub struct BoxResult<F> {
    f: F,
}

impl<F> BoxResult<F> {
    unsafe_pinned!(f: F);
}

impl<S, T, F> Future for BoxResult<F>
where
    F: Future<Output = Result<S, SubscriptionError>>,
    S: Stream<Item = T> + Send + 'static,
{
    type Output = Result<BoxStream<'static, T>, SubscriptionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.f().poll(cx) {
            Poll::Ready(stream_result) => Poll::Ready(stream_result.map(StreamExt::boxed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A topic that boxes the subscription method return type for a wrapped topic.
struct BoxingTopic<Top>(Top);

impl<T: Clone + Send + 'static, Top: Topic<T>> Topic<T> for BoxingTopic<Top> {
    type Receiver = BoxStream<'static, T>;
    type Fut = BoxFuture<'static, Result<Self::Receiver, SubscriptionError>>;

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
        Fut = BoxFuture<'static, Result<BoxStream<'static, T>, SubscriptionError>>,
    >,
>;

impl<T: Clone + 'static> Topic<T> for BoxTopic<T> {
    type Receiver = BoxStream<'static, T>;
    type Fut = BoxFuture<'static, Result<BoxStream<'static, T>, SubscriptionError>>;

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
