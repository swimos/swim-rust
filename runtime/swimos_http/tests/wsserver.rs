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

use bytes::BytesMut;
use hyper::service::{make_service_fn, service_fn};
use hyper::{upgrade::Upgraded, Body, Request, Response, Server};
use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    pin::pin,
    sync::Arc,
    time::Duration,
};
use swimos_http::NoUnwrap;

use futures::{
    channel::oneshot,
    future::{join, select, Either},
    Future,
};
use ratchet::{CloseCode, CloseReason, Message, NoExt, NoExtProvider, PayloadType, WebSocket};
use thiserror::Error;
use tokio::{net::TcpSocket, sync::Notify};

async fn run_server(
    bound_to: oneshot::Sender<SocketAddr>,
    done: Arc<Notify>,
) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let bound = listener.local_addr()?;
    let _ = bound_to.send(bound);

    let service = make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(upgrade_server)) });

    let shutdown = Arc::new(Notify::new());
    let shutdown_cpy = shutdown.clone();

    let server = pin!(Server::from_tcp(listener)?
        .serve(service)
        .with_graceful_shutdown(async move {
            shutdown_cpy.notified().await;
        }));

    let stop = pin!(done.notified());
    match select(server, stop).await {
        Either::Left((result, _)) => result?,
        Either::Right((_, server)) => {
            shutdown.notify_one();
            tokio::time::timeout(Duration::from_secs(2), server).await??;
        }
    }

    Ok(())
}

async fn upgrade_server(request: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    let protocols = ["warp0"].into_iter().collect();
    match swimos_http::negotiate_upgrade(&request, &protocols, &NoExtProvider) {
        Ok(Some(negotiated)) => {
            let (response, upgraded) = swimos_http::upgrade(request, negotiated, None, NoUnwrap);
            tokio::spawn(run_websocket(upgraded));
            Ok(response)
        }
        Ok(None) => Response::builder().body(Body::from("Success")),
        Err(err) => Ok(swimos_http::fail_upgrade(err)),
    }
}

async fn run_websocket<F>(upgrade_fut: F)
where
    F: Future<Output = Result<WebSocket<Upgraded, NoExt>, hyper::Error>> + Send,
{
    match upgrade_fut.await {
        Ok(mut websocket) => {
            let mut read_buffer = BytesMut::new();
            loop {
                read_buffer.clear();
                match websocket.read(&mut read_buffer).await {
                    Ok(Message::Text) => {
                        if let Err(err) = websocket.write(&read_buffer, PayloadType::Text).await {
                            panic!("Writing to websocket failed: {}", err);
                        }
                    }
                    Ok(Message::Binary) => {
                        if let Err(err) = websocket.write(&read_buffer, PayloadType::Binary).await {
                            panic!("Writing to websocket failed: {}", err);
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(err) => {
                        panic!("Websocket connection failed: {}", err);
                    }
                    _ => {}
                }
            }
        }
        Err(err) => panic!("Failed to upgrade connection: {}", err),
    }
}

const FRAMES: &[&str] = &[
    "I", "am", "the", "very", "model", "of", "a", "modern", "major", "general.",
];

#[derive(Debug, Error)]
enum TestError {
    #[error("Server task did not start.")]
    ServerFailedToStart,
    #[error("Server stopped while test still running.")]
    ServerStoppedPrematurely,
    #[error("Unexpected binary frame received.")]
    UnexpectedBinaryFrame,
}

async fn run_client(
    addr: oneshot::Receiver<SocketAddr>,
    done: Arc<Notify>,
) -> Result<(), Box<dyn Error>> {
    let addr = if let Ok(addr) = addr.await {
        addr
    } else {
        return Err(TestError::ServerFailedToStart.into());
    };
    let stream = TcpSocket::new_v4()?.connect(addr).await?;
    let request = format!("ws://localhost:{}", addr.port());
    let mut websocket = ratchet::subscribe(Default::default(), stream, request)
        .await?
        .into_websocket();

    let mut read_buffer = BytesMut::new();
    for body in FRAMES {
        websocket.write_text(*body).await?;
        loop {
            read_buffer.clear();
            match websocket.read(&mut read_buffer).await? {
                Message::Text => {
                    let received_body = std::str::from_utf8(&read_buffer)?;
                    assert_eq!(received_body, *body);
                    break;
                }
                Message::Binary => return Err(TestError::UnexpectedBinaryFrame.into()),
                Message::Close(_) => return Err(TestError::ServerStoppedPrematurely.into()),
                _ => {}
            }
        }
    }
    websocket
        .close(CloseReason::new(
            CloseCode::GoingAway,
            Some("Client stopping.".to_string()),
        ))
        .await?;
    done.notify_one();

    Ok(())
}

#[tokio::test]
async fn connect_to_server() {
    let done = Arc::new(Notify::new());
    let done_cpy = done.clone();
    let (bound_to_tx, bound_to_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        assert!(run_server(bound_to_tx, done_cpy).await.is_ok());
    });
    let client = tokio::spawn(async move {
        assert!(run_client(bound_to_rx, done).await.is_ok());
    });
    let (server_result, client_result) = join(server, client).await;
    assert!(server_result.is_ok());
    assert!(client_result.is_ok());
}
