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

use std::future::Future;

use crate::sink::SinkSendError;
use futures::future::Ready;
use futures::{future, FutureExt};
use tokio::sync::mpsc;
use tokio::sync::watch;

/// An alternative to the [`futures::Sink`] trait for sinks that can consume their inputs in a
/// single operation. This can simplify operations where one can guarantee that the target sink
/// is a queue and will not be performing IO directly (for example in lane models).
pub trait ItemSink<'a, T> {
    type Error;
    type SendFuture: Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Attempt to send an item into the sink.
    fn send_item(&'a mut self, value: T) -> Self::SendFuture;
}

/// Wrap a Tokio MPSC channel sender as an [`ItemSink`]. (We can't implement it directly as the
/// future type returned by the sender is unnameable).
pub fn for_mpsc<'a, T: Send + 'a>(
    sender: mpsc::Sender<T>,
) -> impl ItemSink<'a, T, Error = SinkSendError<T>> {
    ItemSinkWrapper::new(sender, |s: &'a mut mpsc::Sender<T>, t: T| {
        s.send(t).map(|r| {
            r.map_err(|err| {
                let mpsc::error::SendError(val) = err;
                SinkSendError::ClosedOnSend(val)
            })
        })
    })
}

pub struct ItemSinkWrapper<S, F> {
    sink: S,
    send_operation: F,
}

impl<'a, S, F> ItemSinkWrapper<S, F> {
    fn new(sink: S, send_operation: F) -> ItemSinkWrapper<S, F> {
        ItemSinkWrapper {
            sink,
            send_operation,
        }
    }
}

impl<'a, T, E, S, F, Fut> ItemSink<'a, T> for ItemSinkWrapper<S, F>
where
    S: 'a,
    Fut: Future<Output = Result<(), E>> + Send + 'a,
    F: FnMut(&'a mut S, T) -> Fut,
{
    type Error = E;
    type SendFuture = Fut;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        let ItemSinkWrapper {
            sink,
            send_operation,
        } = self;
        send_operation(sink, value)
    }
}

impl<'a, T: Send + 'a> ItemSink<'a, T> for watch::Sender<T> {
    type Error = SinkSendError<T>;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        future::ready(self.broadcast(value).map_err(|_| SinkSendError::Closed))
    }
}
