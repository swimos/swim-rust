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

use crate::connections::error::ConnectionError;
use futures::{Future, Sink, Stream};

pub mod error;

/// Trait for factories that asynchronously create web socket connections. This exists primarily
/// to allow for alternative implementations to be provided during testing.
pub trait WebsocketFactory: Send + Sync {
    /// Type of the stream of incoming messages.
    type WsStream: Stream<Item = Result<String, ConnectionError>> + Unpin + Send + 'static;

    /// Type of the sink for outgoing messages.
    type WsSink: Sink<String> + Unpin + Send + 'static;

    type ConnectFut: Future<Output = Result<(Self::WsSink, Self::WsStream), ConnectionError>>
        + Send
        + 'static;

    /// Open a connection to the provided remote URL.
    fn connect(&mut self, url: url::Url) -> Self::ConnectFut;
}
