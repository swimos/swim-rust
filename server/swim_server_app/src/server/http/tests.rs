// Copyright 2015-2023 Swim Inc.
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

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroUsize,
    pin::pin,
    time::Duration,
};

use bytes::BytesMut;
use futures::{
    future::{join, join3},
    stream::{BoxStream, FuturesUnordered},
    StreamExt,
};
use ratchet::{CloseReason, Message, NoExt, NoExtProvider, WebSocket, WebSocketConfig};
use swim_api::net::Scheme;
use swim_remote::{
    net::{Listener, ListenerResult},
    FindNode,
};
use swim_utilities::non_zero_usize;
use tokio::{io::DuplexStream, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;

const BUFFER_SIZE: usize = 4096;
const MAX_ACTIVE: NonZeroUsize = non_zero_usize!(2);
const REQ_TIMEOUT: Duration = Duration::from_secs(1);
const FIND_CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(8);

async fn client(tx: &mpsc::Sender<DuplexStream>, tag: &str) {
    let (client, server) = tokio::io::duplex(BUFFER_SIZE);

    tx.send(server).await.expect("Failed to open channel.");

    let mut websocket =
        ratchet::subscribe(WebSocketConfig::default(), client, "ws://localhost:8080")
            .await
            .expect("Client handshake failed.")
            .into_websocket();

    websocket
        .write_text(format!("Hello: {}", tag))
        .await
        .expect("Sending message failed.");
    let mut buffer = BytesMut::new();
    loop {
        buffer.clear();
        match websocket.read(&mut buffer).await.expect("Read failed.") {
            Message::Text => {
                let body = std::str::from_utf8(buffer.as_ref()).expect("Bad UTF8 in frame.");
                assert_eq!(body, format!("Received: {}", tag));
                break;
            }
            Message::Binary => panic!("Unexpected binary frame."),
            Message::Close(reason) => panic!("Early close: {:?}", reason),
            _ => {}
        }
    }
    websocket
        .close(CloseReason::new(
            ratchet::CloseCode::GoingAway,
            Some("Client stopping.".to_string()),
        ))
        .await
        .expect("Sending close failed.");
    loop {
        buffer.clear();
        match websocket.read(&mut buffer).await {
            Ok(Message::Text | Message::Binary) => panic!("Unexpected message frame."),
            Ok(Message::Close(_)) => break,
            Err(err) if err.is_close() => break,
            Err(err) => panic!("Closing socket failed failed: {}", err),
            _ => {}
        }
    }
}

struct TestListener {
    rx: mpsc::Receiver<DuplexStream>,
}

impl Listener<DuplexStream> for TestListener {
    type AcceptStream = BoxStream<'static, ListenerResult<(DuplexStream, Scheme, SocketAddr)>>;

    fn into_stream(self) -> Self::AcceptStream {
        let mut n = 0u16;

        ReceiverStream::new(self.rx)
            .map(move |stream| {
                let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, n));
                n += 1;
                Ok((stream, Scheme::Ws, addr))
            })
            .boxed()
    }
}

async fn run_server(rx: mpsc::Receiver<DuplexStream>, find_tx: mpsc::Sender<FindNode>) {
    let listener = TestListener { rx };

    let mut stream = pin!(super::hyper_http_server(
        listener,
        find_tx,
        NoExtProvider,
        None,
        MAX_ACTIVE,
        REQ_TIMEOUT,
    ));

    let handles = FuturesUnordered::new();
    while let Some(result) = stream.next().await {
        let (websocket, _, _) = result.expect("Server handshake failed.");
        handles.push(tokio::spawn(handle_connection(websocket)));
    }

    let results: Vec<_> = handles.collect().await;
    for result in results {
        assert!(result.is_ok());
    }
}

async fn handle_connection(mut websocket: WebSocket<DuplexStream, NoExt>) {
    let mut buffer = BytesMut::new();
    loop {
        buffer.clear();
        match websocket.read(&mut buffer).await.expect("Read failed.") {
            Message::Text => {
                let body = std::str::from_utf8(buffer.as_ref()).expect("Bad UTF8 in frame.");
                let msg = body.strip_prefix("Hello: ").expect("Unexpected body.");
                websocket
                    .write_text(format!("Received: {}", msg))
                    .await
                    .expect("Sending message failed.");
                break;
            }
            Message::Binary => panic!("Unexpected binary frame."),
            Message::Close(reason) => panic!("Early close: {:?}", reason),
            _ => {}
        }
    }
    loop {
        buffer.clear();
        match websocket.read(&mut buffer).await.expect("Read failed.") {
            Message::Text | Message::Binary => panic!("Unexpected message frame."),
            Message::Close(_) => break,
            _ => {}
        }
    }
}

#[tokio::test]
async fn single_client() {
    let (tx, rx) = mpsc::channel(8);
    let (find_tx, _find_rx) = mpsc::channel(FIND_CHANNEL_SIZE.get());
    let server = run_server(rx, find_tx);
    let client = async move {
        client(&tx, "A").await;
        drop(tx);
    };

    join(server, client).await;
}

#[tokio::test]
async fn two_clients() {
    let (tx, rx) = mpsc::channel(8);
    let (find_tx, _find_rx) = mpsc::channel(FIND_CHANNEL_SIZE.get());
    let server = run_server(rx, find_tx);
    let clients = async move {
        join(client(&tx, "A"), client(&tx, "B")).await;
        drop(tx);
    };

    join(server, clients).await;
}

#[tokio::test]
async fn three_clients() {
    let (tx, rx) = mpsc::channel(8);
    let (find_tx, _find_rx) = mpsc::channel(FIND_CHANNEL_SIZE.get());
    let server = run_server(rx, find_tx);
    let clients = async move {
        join3(client(&tx, "A"), client(&tx, "B"), client(&tx, "C")).await;
        drop(tx);
    };

    join(server, clients).await;
}

#[tokio::test]
async fn many_clients() {
    let (tx, rx) = mpsc::channel(8);
    let (find_tx, _find_rx) = mpsc::channel(FIND_CHANNEL_SIZE.get());
    let server = run_server(rx, find_tx);
    let ids = ('A'..='Z').map(|c| c.to_string()).collect::<Vec<_>>();
    let clients = async move {
        let client_tasks = FuturesUnordered::new();
        for tag in ids.iter() {
            client_tasks.push(client(&tx, tag));
        }
        client_tasks.collect::<()>().await;
        drop(tx);
    };

    join(server, clients).await;
}
