// Copyright 2015-2021 SWIM.AI inc.
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

mod imp;
pub mod sender;
pub mod swim_ratchet;

use crate::ws2::sender::WebSocketSender;
use futures::{Future, Stream};

pub trait WsConnections<Sock>
where
    Sock: Send + Sync + Unpin,
{
    type Item;
    type Close;
    type Error;
    type Sink: WebSocketSender<Self::Item, Self::Close, Self::Error>;
    type Stream: Stream<Item = Result<Self::Item, Self::Error>>;
    type Fut: Future<Output = Result<(Self::Sink, Self::Stream), Self::Error>> + Send + 'static;

    /// Negotiate a new client connection.
    fn open_connection(&self, socket: Sock, addr: String) -> Self::Fut;

    /// Negotiate a new server connection.
    fn accept_connection(&self, socket: Sock) -> Self::Fut;
}
