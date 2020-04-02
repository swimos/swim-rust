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

use crate::router::RoutingError;
use common::sink::item::{ItemSender, ItemSink};
use futures::future::{ready, Ready};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[cfg(test)]
mod tests;

/// Joins a watch receiver to an MPSC sender to prevent back-pressure propagating between
/// two queues.
pub struct ValuePump<T> {
    sender: watch::Sender<Option<T>>,
    _task: JoinHandle<()>,
}

impl<'a, T> ItemSink<'a, T> for ValuePump<T> {
    type Error = RoutingError;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        ready(
            self.sender
                .broadcast(Some(value))
                .map_err(|_| RoutingError::RouterDropped),
        )
    }
}

impl<T> ValuePump<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub async fn new<Snk>(sink: Snk) -> Self
    where
        Snk: ItemSender<T, RoutingError> + Send + 'static,
    {
        let (tx, rx) = watch::channel(None);
        let task = ValuePumpTask::new(rx, sink);
        ValuePump {
            sender: tx,
            _task: tokio::task::spawn(task.run()),
        }
    }
}

struct ValuePumpTask<T, Snk> {
    receiver: watch::Receiver<Option<T>>,
    sender: Snk,
}

impl<T, Snk> ValuePumpTask<T, Snk>
where
    T: Clone,
    Snk: ItemSender<T, RoutingError>,
{
    fn new(rx: watch::Receiver<Option<T>>, sink: Snk) -> Self {
        ValuePumpTask {
            receiver: rx,
            sender: sink,
        }
    }

    async fn run(self) {
        let ValuePumpTask {
            mut receiver,
            mut sender,
        } = self;
        while let Some(maybe) = receiver.recv().await {
            if let Some(value) = maybe {
                if sender.send_item(value).await.is_err() {
                    break;
                }
            }
        }
    }
}
