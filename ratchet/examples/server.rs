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

use bytes::BytesMut;
use ratchet::{
    Error, Message, NoExtProvider, PayloadType, ProtocolRegistry, UpgradedServer, WebSocketConfig,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:9001").await?;
    let mut incoming = TcpListenerStream::new(listener);

    while let Some(socket) = incoming.next().await {
        let socket = socket?;

        let upgrader = ratchet::accept_with(
            socket,
            WebSocketConfig::default(),
            NoExtProvider,
            ProtocolRegistry::default(),
        )
        .await?;

        // You could opt to reject the connection
        // websocket.reject(WebSocketResponse::new(404)?).await?;

        // Or you could opt to reject the connection with headers
        // websocket.reject(WebSocketResponse::with_headers(404, headers)?).await;

        let UpgradedServer { mut websocket, .. } = upgrader.upgrade().await?;
        let mut buf = BytesMut::new();

        loop {
            match websocket.read(&mut buf).await? {
                Message::Text => {
                    websocket.write(&mut buf, PayloadType::Text).await?;
                    buf.clear();
                }
                Message::Binary => {
                    websocket.write(&mut buf, PayloadType::Binary).await?;
                    buf.clear();
                }
                Message::Ping | Message::Pong => {}
                Message::Close(_) => break,
            }
        }
    }

    Ok(())
}
