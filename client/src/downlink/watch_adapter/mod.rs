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

use tokio::sync::watch;
use futures::{Stream, ready};
use futures::task::{Context, Poll};
use pin_project::pin_project;
use std::pin::Pin;

pub mod map;
pub mod value;

fn channel<T: Clone>(init: T) -> (EpochSender<T>, EpochReceiver<T>) {
    let (tx, rx) = watch::channel::<(u64, T)>((1, init));
    (EpochSender::new(tx), EpochReceiver::new(rx))
}

#[derive(Debug)]
struct EpochSender<T> {
    inner: watch::Sender<(u64, T)>,
    epoch: u64,
}

impl<T: Clone> EpochSender<T> {

    fn new(sender: watch::Sender<(u64, T)>) -> Self {
        EpochSender {
            inner: sender,
            epoch: 2,
        }
    }

    fn broadcast(&mut self, value: T) -> Result<(), watch::error::SendError<(u64, T)>>{
        let current_epoch = self.epoch;
        self.inner.broadcast((current_epoch, value))?;
        self.epoch += 1;
        Ok(())
    }

}

#[pin_project]
#[derive(Debug)]
struct EpochReceiver<T> {
    #[pin]
    inner: watch::Receiver<(u64, T)>,
    prev_epoch: u64,
}

impl<T: Clone> EpochReceiver<T> {

    fn new(receiver: watch::Receiver<(u64, T)>) -> Self {
        EpochReceiver {
            inner: receiver,
            prev_epoch: 0,
        }
    }

    async fn recv(&mut self) -> Option<T> {
        loop {
            let (epoch, value) = self.inner.recv().await?;
            if epoch != self.prev_epoch {
                self.prev_epoch = epoch;
                return Some(value)
            }
        }
    }

}

impl<T: Clone> EpochReceiver<Option<T>> {

    async fn recv_defined(&mut self) -> Option<T> {
        loop {
            let next = self.recv().await?;
            if next.is_some() {
                break next;
            }
        }
    }

}

impl<T: Clone> Stream for EpochReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projected = self.project();
        let mut inner = projected.inner;
        let prev_epoch = projected.prev_epoch;
        loop {
            match ready!(inner.as_mut().poll_next(cx)) {
                Some((epoch, value)) => {
                    if epoch != *prev_epoch {
                        *prev_epoch = epoch;
                        break Poll::Ready(Some(value));
                    }
                },
                _ => {
                    break Poll::Ready(None);
                }
            }
        }
    }
}
