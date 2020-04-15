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

use futures::task::{Context, Poll};
use futures::Future;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};

use super::*;

pub struct Sequenced<F1: Unpin, F2: Unpin> {
    first: Option<F1>,
    second: Option<F2>,
}

pub type SendAndAwait<T1, T2> = Sequenced<RequestFuture<T1>, oneshot::Receiver<T2>>;

impl<F1: Unpin, F2: Unpin> Sequenced<F1, F2> {
    pub fn new(first: F1, second: F2) -> Sequenced<F1, F2> {
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
    E2: Into<E1>,
{
    type Output = Result<T2, E1>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Sequenced {
            first: maybe_first,
            second: maybe_second,
        } = self.get_mut();
        if let Some(first) = maybe_first {
            match Pin::new(first).poll(cx) {
                Poll::Ready(result) => {
                    maybe_first.take();
                    result?;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        match maybe_second {
            Some(second) => Pin::new(second).poll(cx).map_err(Into::into),
            _ => panic!("Sequenced future used twice."),
        }
    }
}

pub struct RequestFuture<T: Unpin + Send + 'static> {
    sender: mpsc::Sender<T>,
    request: Option<T>,
}

impl<T: Unpin + Send + 'static> RequestFuture<T> {
    pub fn new(sender: mpsc::Sender<T>, req: T) -> RequestFuture<T> {
        RequestFuture {
            sender,
            request: Some(req),
        }
    }
}

pub fn send_and_await<T: Unpin + Send + 'static>(
    sender: &mpsc::Sender<Request<T>>,
) -> SendAndAwait<Request<T>, T> {
    let (tx, rx) = oneshot::channel();
    Sequenced::new(RequestFuture::new(sender.clone(), Request::new(tx)), rx)
}

pub struct RequestError {}

impl From<RecvError> for RequestError {
    fn from(_: RecvError) -> Self {
        RequestError::new()
    }
}

impl RequestError {
    fn new() -> Self {
        RequestError {}
    }
}

impl<T: Unpin + Send + 'static> Future for RequestFuture<T> {
    type Output = Result<(), RequestError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let RequestFuture { sender, request } = self.get_mut();
        sender.poll_ready(cx).map(|r| match r {
            Ok(_) => match request.take() {
                Some(req) => match sender.try_send(req) {
                    Ok(_) => Ok(()),
                    _ => Err(RequestError::new()),
                },
                _ => panic!("Send future used twice."),
            },
            Err(_) => Err(RequestError::new()),
        })
    }
}
