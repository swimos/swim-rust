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
use futures::{Future, ready};
use futures_util::future::FutureExt;
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TrySendError, SendError};

use crate::future::retryable::strategy::RetryStrategy;
use crate::future::retryable::{self, RetryState, ResettableFuture, RetryableFuture};

#[derive(Clone)]
struct TestSender<P> {
    tx: mpsc::Sender<P>,
    payload: P,
}

pub struct MpscSender<'a, T> {
    current: Option<MpscSend<'a, T>>,
    error: Option<mpsc::error::SendError<T>>
}

impl <'a, T> MpscSender<'a, T> {

    fn new(sender: &'a mut mpsc::Sender<T>, payload: T) -> Self {
        let current = MpscSend::new(sender, payload);
        MpscSender {
            current: Some(current),
            error: None,
        }
    }

}

impl<'a, T: Unpin> Unpin for MpscSender<'a, T> {}

impl<'a, T: Unpin> Future for MpscSender<'a, T> {
    type Output = Result<(), String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(current) = &mut this.current {
            match ready!(current.poll_unpin(cx)) {
                Ok(out) => Poll::Ready(Ok(())),
                Err(e) => {
                    this.error = Some(e);
                    Poll::Ready(Err(String::from("Send failed.")))
                }
            }
        } else {
            unreachable!()
        }

    }
}

impl<'a, T: Unpin> ResettableFuture for MpscSender<'a, T> {
    fn reset(self: Pin<&mut Self>) -> bool {
        let this = self.get_mut();
        match this.error.take() {
            Some(e) => {
                if let Some(current) = this.current.take() {
                    let sender = current.dissolve();
                    this.current = Some(MpscSend::new(sender, e.0));
                    true
                } else {
                    unreachable!()
                }
            },
            _ => {
                false
            }
        }
    }
}


pub struct MpscSend<'a, T> {
    sender: Pin<&'a mut mpsc::Sender<T>>,
    value: Option<T>,
}

impl<'a, T> MpscSend<'a, T> {
    pub fn new(sender: &'a mut mpsc::Sender<T>, value: T) -> MpscSend<'a, T> {
        MpscSend {
            sender: Pin::new(sender),
            value: Some(value),
        }
    }

    pub fn dissolve(self) -> &'a mut mpsc::Sender<T> {
        let MpscSend { sender, ..} = self;
        sender.get_mut()
    }
}

impl<'a, T> Future for MpscSend<'a, T>
where
    T: Unpin,
{
    type Output = Result<(), mpsc::error::SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let MpscSend { sender, value, .. } = self.get_mut();

        match sender.poll_ready(cx) {
            Poll::Ready(Ok(_)) => match value.take() {
                Some(t) => match sender.try_send(t) {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(mpsc::error::TrySendError::Closed(t)) => {
                        Poll::Ready(Err(mpsc::error::SendError(t)))
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => unreachable!(),
                },
                _ => panic!("Send future evaluated twice."),
            },
            Poll::Ready(Err(_)) => match value.take() {
                Some(t) => Poll::Ready(Err(mpsc::error::SendError(t))),
                _ => panic!("Send future evaluated twice."),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
enum FutErr {
    Err,
}

impl<P> Future for TestSender<P>
where
    P: Send + Unpin + Clone,
{
    type Output = Result<(), FutErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let TestSender { tx, payload } = self.get_mut();

        match tx.poll_ready(cx) {
            Poll::Ready(Ok(_)) => match tx.try_send(payload.clone()) {
                Ok(_) => Poll::Ready(Ok(())),
                Err(TrySendError::Closed(_)) => Poll::Ready(Err(FutErr::Err)),
                Err(TrySendError::Full(_)) => unreachable!(),
            },

            Poll::Ready(Err(_)) => Poll::Ready(Err(FutErr::Err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::test]
async fn testy() {
    let (mut tx, rx) = mpsc::channel::<i32>(2);
    let payload = 7;

    let sender = MpscSender::new(&mut tx, 7);
    let retry = RetryableFuture::new(sender, RetryStrategy::immediate(2));
    let result = retry.await;
    assert_eq!(result, Ok(()));

}
