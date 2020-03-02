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

use std::pin::Pin;
use std::sync::{Arc, Weak};

use either::Either;
use futures::future::ready;
use futures::future::Ready;
use futures::task::{Context, Poll};
use futures::{future, Future, Stream, StreamExt};
use futures_util::select_biased;
use pin_utils::unsafe_pinned;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

pub enum SubscriptionError {
    TopicClosed,
}

pub trait Topic<T: Clone> {
    type Receiver: Stream<Item = T>;
    type Fut: Future<Output = Result<Self::Receiver, SubscriptionError>> + 'static;

    fn subscribe(&mut self) -> Self::Fut;
}

#[derive(Clone, Debug)]
pub struct WatchTopic<T: Clone> {
    receiver: Weak<watch::Receiver<T>>,
}

#[derive(Clone, Debug)]
pub struct WatchTopicReceiver<T: Clone> {
    _owner: Arc<watch::Receiver<T>>,
    receiver: watch::Receiver<T>,
}

#[derive(Clone, Debug)]
pub struct BroadcastTopic<T: Clone> {
    sender: broadcast::Sender<T>,
}

#[derive(Debug)]
pub struct BroadcastReceiver<T: Clone> {
    sender: broadcast::Sender<T>,
    receiver: broadcast::Receiver<T>,
}

struct SubRequest<T>(oneshot::Sender<mpsc::Receiver<T>>);

impl<T: Clone> BroadcastReceiver<T> {
    unsafe_pinned!(receiver: broadcast::Receiver<T>);
}

#[derive(Clone, Debug)]
pub struct MpscTopic<T: Clone> {
    sub_sender: mpsc::Sender<SubRequest<T>>,
    task: Arc<JoinHandle<()>>,
}

impl<T: Clone + Send + Sync + 'static> MpscTopic<T> {
    pub async fn new(
        input: mpsc::Receiver<T>,
        buffer_size: usize,
    ) -> (MpscTopic<T>, mpsc::Receiver<T>) {
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

impl<T: Clone> WatchTopic<T> {
    pub fn new(receiver: watch::Receiver<T>) -> (Self, WatchTopicReceiver<T>) {
        let duplicate = receiver.clone();
        let owner = Arc::new(receiver);
        let topic = WatchTopic {
            receiver: Arc::downgrade(&owner),
        };
        let rec = WatchTopicReceiver {
            _owner: owner,
            receiver: duplicate,
        };
        (topic, rec)
    }
}

impl<T: Clone> WatchTopicReceiver<T> {
    unsafe_pinned!(receiver: watch::Receiver<T>);
}

impl<T: Clone> Stream for WatchTopicReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver().poll_next(cx)
    }
}

impl<T: Clone + 'static> Topic<T> for WatchTopic<T> {
    type Receiver = WatchTopicReceiver<T>;
    type Fut = Ready<Result<Self::Receiver, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        ready(match self.receiver.upgrade() {
            Some(owner) => {
                let receiver = owner.as_ref().clone();
                Ok(WatchTopicReceiver {
                    _owner: owner,
                    receiver,
                })
            }
            _ => Err(SubscriptionError::TopicClosed),
        })
    }
}

impl<T: Clone> Stream for BroadcastReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver().poll_next(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(value)),
            Poll::Ready(Some(Err(broadcast::RecvError::Lagged(_)))) => Poll::Pending,
            Poll::Ready(Some(Err(broadcast::RecvError::Closed))) => Poll::Ready(None),
            Poll::Ready(_) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
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

impl<T: Clone + 'static> Topic<T> for BroadcastTopic<T> {
    type Receiver = BroadcastReceiver<T>;
    type Fut = Ready<Result<Self::Receiver, SubscriptionError>>;

    fn subscribe(&mut self) -> Self::Fut {
        ready(Ok(BroadcastReceiver {
            sender: self.sender.clone(),
            receiver: self.sender.subscribe(),
        }))
    }
}

pub struct SendFuture<T> {
    sender: mpsc::Sender<SubRequest<T>>,
    request: Option<SubRequest<T>>,
    result: oneshot::Receiver<mpsc::Receiver<T>>,
}

impl<T> Future for SendFuture<T> {
    type Output = Result<mpsc::Receiver<T>, SubscriptionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SendFuture {
            sender,
            request,
            result,
        } = self.get_mut();
        if request.is_some() {
            let mut pinned_sender = Pin::new(sender);
            match pinned_sender.poll_ready(cx) {
                Poll::Ready(Err(_)) => match request.take() {
                    None => {
                        unreachable!();
                    }
                    Some(_) => Poll::Ready(Err(SubscriptionError::TopicClosed)),
                },
                Poll::Ready(_) => match request.take() {
                    None => {
                        panic!("Send future used more than once.");
                    }
                    Some(v) => match pinned_sender.try_send(v) {
                        Ok(_) => Poll::Pending,
                        Err(mpsc::error::TrySendError::Full(_)) => unreachable!(),
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            Poll::Ready(Err(SubscriptionError::TopicClosed))
                        }
                    },
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            let pinned_res = Pin::new(result);
            match pinned_res.poll(cx) {
                Poll::Ready(Err(_)) => Poll::Ready(Err(SubscriptionError::TopicClosed)),
                Poll::Ready(Ok(rec)) => Poll::Ready(Ok(rec)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

impl<T: Clone + 'static> Topic<T> for MpscTopic<T> {
    type Receiver = mpsc::Receiver<T>;
    type Fut = SendFuture<T>;

    fn subscribe(&mut self) -> Self::Fut {
        let MpscTopic { sub_sender, .. } = self;
        let (sub_tx, sub_rx) = oneshot::channel::<mpsc::Receiver<T>>();

        SendFuture {
            sender: sub_sender.clone(),
            request: Some(SubRequest(sub_tx)),
            result: sub_rx,
        }
    }
}

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
                match cb.send(rx) {
                    Ok(_) => outputs.push(tx),
                    _ => {}
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
