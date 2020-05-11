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

use common::sink::item::ItemSender;
use common::warp::envelope::{Envelope, IncomingLinkMessage, OutgoingLinkMessage};
use common::warp::path::AbsolutePath;
use futures::{Future, Stream};
use std::error::Error;
use std::fmt::{Display, Formatter};
use tokio::sync::mpsc::error::SendError;

pub trait Router: Send {
    type ConnectionStream: Stream<Item = IncomingLinkMessage> + Send + 'static;
    type ConnectionSink: ItemSender<OutgoingLinkMessage, RoutingError>
        + Clone
        + Sync
        + Send
        + 'static;
    type GeneralSink: ItemSender<(String, Envelope), RoutingError> + Clone + Sync + Send + 'static;

    type ConnectionFut: Future<Output = (Self::ConnectionSink, Self::ConnectionStream)> + Send;
    type GeneralFut: Future<Output = Self::GeneralSink> + Send;

    fn connection_for(&mut self, target: &AbsolutePath) -> Self::ConnectionFut;

    fn general_sink(&mut self) -> Self::GeneralFut;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RoutingError {
    RouterDropped,
}

impl Display for RoutingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::RouterDropped => write!(f, "Router was dropped."),
        }
    }
}

impl<T> From<SendError<T>> for RoutingError {
    fn from(_: SendError<T>) -> Self {
        RoutingError::RouterDropped
    }
}

impl Error for RoutingError {}
