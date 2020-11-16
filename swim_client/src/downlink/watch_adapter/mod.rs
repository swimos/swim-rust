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

use futures::stream::unfold;
use futures::Stream;
use pin_project::pin_project;
use tokio::sync::watch;

pub mod map;
pub mod value;

#[cfg(test)]
mod tests;

/// Creates a wrapper around a [`tokio::sync::watch`] channel that removes any duplicate
/// observations by attaching an epoch to values passing through the channel.
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

    fn broadcast(&mut self, value: T) -> Result<(), watch::error::SendError<(u64, T)>> {
        let current_epoch = self.epoch;
        self.inner.send((current_epoch, value))?;
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
            if self.inner.changed().await.is_ok() {
                let (epoch, value) = &*self.inner.borrow();
                if *epoch != self.prev_epoch {
                    self.prev_epoch = *epoch;
                    return Some(value.clone());
                }
            } else {
                return None;
            }
        }
    }

    fn into_stream(self) -> impl Stream<Item = T> {
        let EpochReceiver { inner, prev_epoch } = self;
        unfold(
            (inner, prev_epoch),
            move |(mut inner, prev_epoch)| async move {
                loop {
                    if inner.changed().await.is_ok() {
                        let contents = inner.borrow();
                        let epoch = contents.0;
                        if epoch != prev_epoch {
                            let value = contents.1.clone();
                            drop(contents);
                            break Some((value, (inner, epoch)));
                        }
                    } else {
                        break None;
                    }
                }
            },
        )
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
