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
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::Sink;
use tokio::sync::mpsc;
use tokio::sync::watch;

pub mod item;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SinkSendError<T> {
    Closed,
    ClosedOnSend(T),
}

impl<T> Display for SinkSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            SinkSendError::Closed => f.write_str("Sink is closed."),
            SinkSendError::ClosedOnSend(_) => f.write_str("Sink closed while sending."),
        }
    }
}

impl<T: Debug> Error for SinkSendError<T> {}

/// Wrapper for the Tokio MPSC sender to allow it to implement [`Sink`].
#[derive(Clone, Debug)]
pub struct MpscSink<T>(mpsc::Sender<T>);

impl<T> MpscSink<T> {
    pub fn wrap(sender: mpsc::Sender<T>) -> MpscSink<T> {
        MpscSink(sender)
    }
}

/// Wrapper for the Tokio watch sender to allow it to implement [`Sink`].
#[derive(Debug)]
pub struct WatchSink<T>(watch::Sender<T>);

impl<T> WatchSink<T> {
    pub fn wrap(sender: watch::Sender<T>) -> WatchSink<T> {
        WatchSink(sender)
    }
}

impl<T> From<watch::Sender<T>> for WatchSink<T> {
    fn from(sender: watch::Sender<T>) -> Self {
        WatchSink(sender)
    }
}

impl<T> Sink<T> for WatchSink<T> {
    type Error = watch::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let WatchSink(sender) = self.get_mut();
        sender.send(item)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
