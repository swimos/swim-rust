// Copyright 2015-2024 Swim Inc.
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

use bytes::{Bytes, BytesMut};
use hyper::service::service_fn;
use hyper::{upgrade::Upgraded, Request, Response};
use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    sync::Arc,
    time::Duration,
};
use swimos_http::{NoUnwrap, Upgrade, UpgradeStatus};

use futures::{
    channel::oneshot,
    future::{join, select, Either},
    Future,
};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use ratchet::{
    CloseCode, CloseReason, Message, NoExt, NoExtProvider, PayloadType, SubprotocolRegistry,
    WebSocket,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::{net::TcpSocket, sync::Notify};

async fn run_server(
    bound_to: oneshot::Sender<SocketAddr>,
    done: Arc<Notify>,
) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let bound = listener.local_addr()?;
    let _ = bound_to.send(bound);

    let (io, _) = listener.accept().await?;
    let builder = Builder::new(TokioExecutor::new());

    let connection = builder.serve_connection_with_upgrades(
        TokioIo::new(io),
        service_fn(move |req| {
            let registry =
                SubprotocolRegistry::new(["warp0"]).expect("Failed to build subprotocol registry");
            async move { upgrade_server(req, &registry).await }
        }),
    );
    let shutdown = GracefulShutdown::new();

    let server = pin!(shutdown.watch(connection));
    let stop = pin!(done.notified());

    match select(server, stop).await {
        Either::Left((result, _)) => match result {
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        },
        Either::Right((_, _server)) => {
            tokio::time::timeout(Duration::from_secs(2), shutdown.shutdown()).await?;
            Ok(())
        }
    }
}

async fn upgrade_server(
    request: Request<Incoming>,
    registry: &SubprotocolRegistry,
) -> Result<Response<Full<Bytes>>, ratchet::Error> {
    match swimos_http::negotiate_upgrade(request, registry, &NoExtProvider) {
        UpgradeStatus::Upgradeable {
            result: Ok(parts),
            request,
        } => {
            let Upgrade { response, future } =
                swimos_http::upgrade(parts, request, None, NoUnwrap)?;
            tokio::spawn(run_websocket(future));
            Ok(response)
        }
        UpgradeStatus::Upgradeable {
            result: Err(err), ..
        } => {
            if err.is_io() {
                Err(err)
            } else {
                Ok(swimos_http::fail_upgrade(err))
            }
        }
        UpgradeStatus::NotRequested { request: _ } => Response::builder()
            .body(Full::from("Success"))
            .map_err(Into::into),
    }
}

async fn run_websocket<F>(upgrade_fut: F)
where
    F: Future<Output = Result<WebSocket<TokioIo<Upgraded>, NoExt>, hyper::Error>> + Send,
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
