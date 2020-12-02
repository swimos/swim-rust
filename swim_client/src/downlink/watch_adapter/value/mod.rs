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

use futures::future::{ready, Ready};
use std::num::NonZeroUsize;
use swim_common::routing::RoutingError;
use swim_common::sink::item::{ItemSender, ItemSink};
use swim_runtime::task::{spawn, TaskHandle};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

#[cfg(test)]
mod tests;

/// Joins a watch receiver to an MPSC sender to prevent back-pressure propagating between
/// two queues.
pub struct ValuePump<T> {
    sender: broadcast::Sender<T>,
    _task: TaskHandle<()>,
}

impl<'a, T: Clone> ItemSink<'a, T> for ValuePump<T> {
    type Error = RoutingError;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        ready(
            self.sender
                .send(value)
                .map(|_| ())
                .map_err(|_| RoutingError::RouterDropped),
        )
    }
}

impl<T> ValuePump<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub async fn new<Snk>(sink: Snk, yield_after: NonZeroUsize) -> Self
    where
        Snk: ItemSender<T, RoutingError> + Send + 'static,
    {
        let (tx, rx) = broadcast::channel(1);
        let task = ValuePumpTask::new(rx, sink, yield_after);
        ValuePump {
            sender: tx,
            _task: spawn(task.run()),
        }
    }
}

struct ValuePumpTask<T, Snk> {
    receiver: broadcast::Receiver<T>,
    sender: Snk,
    yield_after: NonZeroUsize,
}

impl<T, Snk> ValuePumpTask<T, Snk>
where
    T: Clone,
    Snk: ItemSender<T, RoutingError>,
{
    fn new(rx: broadcast::Receiver<T>, sink: Snk, yield_after: NonZeroUsize) -> Self {
        ValuePumpTask {
            receiver: rx,
            sender: sink,
            yield_after,
        }
    }

    async fn run(self) {
        let ValuePumpTask {
            mut receiver,
            mut sender,
            yield_after,
        } = self;
        let yield_mod = yield_after.get();
        let mut iteration_count: usize = 0;
        loop {
            match receiver.recv().await {
                Ok(value) => {
                    if sender.send_item(value).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Closed) => {
                    break;
                }
                _ => {}
            }
            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}
