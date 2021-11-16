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
use ratchet::{subscribe, Error, Message, PayloadType, UpgradedClient, WebSocketConfig};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let stream = TcpStream::connect("127.0.0.1:9001").await?;

    let upgraded = subscribe(WebSocketConfig::default(), stream, "ws://127.0.0.1/hello").await?;

    let UpgradedClient { websocket, .. } = upgraded;
    let mut buf = BytesMut::new();

    let (mut sender, mut receiver) = websocket.split()?;

    loop {
        match receiver.read(&mut buf).await? {
            Message::Text => {
                sender.write(&mut buf, PayloadType::Text).await?;
                buf.clear();
            }
            Message::Binary => {
                sender.write(&mut buf, PayloadType::Binary).await?;
                buf.clear();
            }
            Message::Ping | Message::Pong => {}
            Message::Close(_) => break Ok(()),
        }
    }
}
