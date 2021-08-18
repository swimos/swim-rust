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

use crate::codec::Codec;
use crate::fixture::{mock, poll_fn};
use crate::owned::{WebSocket, WebSocketInner};
use crate::protocol::frame::Message;
use crate::{NoExt, Role};
use futures::{Sink, SinkExt, StreamExt};
use std::pin::Pin;
use tokio_util::codec::{Decoder, Encoder, Framed};

#[tokio::test]
async fn send_receive() {
    let (peer, mut stream) = mock();
    let mut socket = WebSocket {
        inner: WebSocketInner {
            framed: Framed::new(stream, Codec::new(Role::Client, usize::MAX)),
            role: Role::Client,
            extension: NoExt,
            config: Default::default(),
            _priv: (),
        },
    };
    {
        assert!(socket
            .send(Message::Text("Hello".to_string()))
            .await
            .is_ok());

        let rx = &mut *peer.rx_buf.lock().unwrap();
        let result = Codec::new(Role::Server, usize::MAX).decode(rx);
        assert_eq!(result.unwrap(), Some(Message::Text("Hello".to_string())));
    }
    // Guard must be dropped for the message to be received as it needs to lock the mutex
    {
        let tx = &mut *peer.tx_buf.lock().unwrap();
        assert!(Codec::new(Role::Server, usize::MAX)
            .encode(Message::Text("Hello back".to_string()), tx)
            .is_ok());
    }
    assert_eq!(
        socket.next().await.unwrap().unwrap(),
        Message::Text("Hello back".to_string())
    );
}
