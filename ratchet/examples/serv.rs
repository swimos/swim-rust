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

use ratchet::{NoExtProxy, ProtocolRegistry, WebSocketConfig, WebSocketResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
    let mut incoming = TcpListenerStream::new(listener);

    while let Some(socket) = incoming.next().await {
        let socket = socket.unwrap();

        let upgrader = ratchet::accept(
            socket,
            WebSocketConfig::default(),
            NoExtProxy,
            ProtocolRegistry::default(),
        )
        .await
        .unwrap();

        upgrader
            .reject(WebSocketResponse::new(500).unwrap())
            .await
            .unwrap();
    }
}
