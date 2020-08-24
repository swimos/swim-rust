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

use crate::connections::factory::async_factory;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::{future, Stream};
use futures::{Future, FutureExt, StreamExt, TryFutureExt};
use http::{Request, Uri};
use hyper::Body;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::Deref;
use swim_common::request::request_future::SendAndAwait;
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::{WebsocketFactory, WsMessage};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tls::TlsStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tokio_tungstenite::{tungstenite, WebSocketStream};
use url::Url;
use utilities::future::{TransformMut, TransformedSink, TransformedStream};
use warp::test::WsError;
type TungSink = TransformedSink<SplitSink<WsConnection, Message>, SinkTransformer>;
type TungStream = TransformedStream<SplitStream<WsConnection>, StreamTransformer>;
type ConnectionFuture = SendAndAwait<ConnReq, Result<(TungSink, TungStream), ConnectionError>>;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
pub type WsConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type ConnReq = async_factory::ConnReq<TungSink, TungStream>;
type TError = tungstenite::error::Error;
struct TungsteniteError(tungstenite::error::Error);
use futures::future::ready;
use hyper::client::connect::Connect;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::task::JoinHandle;
use tower::Service;
use tracing::Level;
use warp::Filter;

impl Deref for TungsteniteError {
    type Target = TError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TungsteniteError> for ConnectionError {
    fn from(e: TungsteniteError) -> Self {
        match e.deref() {
            TError::ConnectionClosed => ConnectionError::Closed,
            TError::Url(url) => ConnectionError::SocketError(WebSocketError::Url(url.to_string())),
            TError::HttpFormat(_) | TError::Http(_) => {
                ConnectionError::SocketError(WebSocketError::Protocol)
            }
            TError::Io(e) if e.kind() == ErrorKind::ConnectionRefused => {
                ConnectionError::ConnectionRefused
            }
            _ => ConnectionError::ConnectError,
        }
    }
}

pub struct SinkTransformer;
impl TransformMut<WsMessage> for SinkTransformer {
    type Out = Message;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        match input {
            WsMessage::Text(s) => Message::Text(s),
            WsMessage::Binary(v) => Message::Binary(v),
        }
    }
}

pub struct StreamTransformer;
impl TransformMut<Result<Message, TError>> for StreamTransformer {
    type Out = Result<WsMessage, ConnectionError>;

    fn transform(&mut self, input: Result<Message, TError>) -> Self::Out {
        match input {
            Ok(i) => match i {
                Message::Text(s) => Ok(WsMessage::Text(s)),
                Message::Binary(v) => Ok(WsMessage::Binary(v)),
                _ => Err(ConnectionError::ReceiveMessageError),
            },
            Err(_) => Err(ConnectionError::ConnectError),
        }
    }
}

pub struct WarpWsFactory {
    inner: async_factory::AsyncFactory<TungSink, TungStream>,
}

pub struct WsClient {
    _jh: JoinHandle<()>,
    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Result<Message, TError>>,
}

impl WsClient {
    async fn new(url: Url) -> Result<WsClient, WsError> {
        let (upgraded_tx, upgraded_rx) = oneshot::channel();
        let (wr_tx, wr_rx) = mpsc::unbounded_channel();
        let (mut rd_tx, rd_rx) = mpsc::unbounded_channel();

        let join_handle = tokio::spawn(async move {
            let route = warp::ws().map(|ws: warp::ws::Ws| {
                ws.on_upgrade(|websocket| {
                    // Just echo all messages back...
                    let (tx, rx) = websocket.split();
                    rx.forward(tx).map(|result| {
                        if let Err(e) = result {
                            eprintln!("websocket error: {:?}", e);
                        }
                    })
                })
            });

            let (addr, srv) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
            println!("{}", addr);

            let mut req = Request::new(Body::empty());
            let headers = req.headers_mut();

            headers.insert(
                "connection",
                "upgrade".parse().expect("Failed to parse header"),
            );
            headers.insert(
                "upgrade",
                "websocket".parse().expect("Failed to parse header"),
            );
            headers.insert(
                "sec-websocket-version",
                "13".parse().expect("Failed to parse header"),
            );
            headers.insert(
                "sec-websocket-key",
                "dGhlIHNhbXBsZSBub25jZQ=="
                    .parse()
                    .expect("Failed to parse header"),
            );

            let uri = url.path();

            let uri = format!("http://{}{}", addr, uri)
                .parse()
                .expect("Malformatted URL");

            *req.uri_mut() = uri;

            // let mut rt = current_thread::Runtime::new().unwrap();
            tokio::spawn(srv);

            let upgrade = hyper::Client::builder()
                .build(AddrConnect(addr))
                .request(req)
                .and_then(|res| res.into_body().on_upgrade());

            let upgraded = match upgrade.await {
                Ok(up) => {
                    let _ = upgraded_tx.send(Ok(()));
                    up
                }
                Err(err) => {
                    let _ = upgraded_tx.send(Err(err));
                    return;
                }
            };

            let ws =
                WebSocketStream::from_raw_socket(upgraded, Role::Client, Default::default()).await;

            let (tx, rx) = ws.split();
            let write = wr_rx.map(Ok).forward(tx).map(|_| ());

            let read = rx
                .take_while(|result| match result {
                    Ok(m) => future::ready(!m.is_close()),
                    Err(e) => {
                        println!("{}", e);
                        future::ready(false)
                    }
                })
                .for_each(move |item| {
                    rd_tx.send(item).expect("ws receive error");
                    future::ready(())
                });

            future::join(write, read).await;
        });

        match upgraded_rx.await {
            Ok(Ok(())) => Ok(WsClient {
                _jh: join_handle,
                tx: wr_tx,
                rx: rd_rx,
            }),
            Ok(Err(_err)) => panic!(),
            Err(_canceled) => panic!("websocket handshake thread panicked"),
        }
    }
}

#[derive(Clone)]
struct AddrConnect(SocketAddr);

impl Service<::http::Uri> for AddrConnect {
    type Response = tokio::net::TcpStream;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ::http::Uri) -> Self::Future {
        Box::pin(tokio::net::TcpStream::connect(self.0))
    }
}

#[tokio::test(core_threads = 2)]
async fn t() {
    let url = Url::from_str("ws://127.0.0.1/echo/").unwrap();
    let client = WsClient::new(url).await.unwrap();

    // tracing_subscriber::fmt()
    //     .with_max_level(Level::TRACE)
    //     // .with_env_filter(filter)
    //     .init();

    client.tx.send(Message::text("Hello")).unwrap();

    tokio::spawn(async {
        client
            .rx
            .for_each(|m| {
                println!("Received: {:?}", m);
                future::ready(())
            })
            .await;
    });

    tokio::time::delay_for(Duration::from_secs(5)).await;
}
