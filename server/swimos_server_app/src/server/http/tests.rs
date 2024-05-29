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
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroUsize,
    pin::pin,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{
    future::{join, join3, Either},
    stream::{BoxStream, FuturesUnordered, SelectAll},
    Future, StreamExt,
};
use hyper::{
    body::to_bytes, client::conn::http1, header::HeaderValue, Body, Request, Response, Uri,
};
use ratchet::{CloseReason, Message, NoExt, NoExtProvider, WebSocket, WebSocketConfig};
use swimos_api::agent::{HttpLaneRequest, RawHttpLaneResponse};
use swimos_model::{
    http::{StatusCode, Version},
    Text,
};
use swimos_net::Scheme;
use swimos_remote::{
    net::{Listener, ListenerResult},
    AgentResolutionError, FindNode, NoSuchAgent, NodeConnectionRequest,
};
use swimos_utilities::non_zero_usize;
use tokio::{io::DuplexStream, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::config::HttpConfig;

const BUFFER_SIZE: usize = 4096;
const MAX_ACTIVE: NonZeroUsize = non_zero_usize!(2);
const REQ_TIMEOUT: Duration = Duration::from_millis(100);
const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(8);

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
    println!("Server start");
    let listener = TestListener { rx };

    let config = HttpConfig {
        max_http_requests: MAX_ACTIVE,
        http_request_timeout: REQ_TIMEOUT,
        ..Default::default()
    };

    let mut stream = pin!(super::hyper_http_server(
        listener,
        find_tx,
        NoExtProvider,
        config,
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
    let (find_tx, _find_rx) = mpsc::channel(CHANNEL_SIZE.get());
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
    let (find_tx, _find_rx) = mpsc::channel(CHANNEL_SIZE.get());
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
    let (find_tx, _find_rx) = mpsc::channel(CHANNEL_SIZE.get());
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
    let (find_tx, _find_rx) = mpsc::channel(CHANNEL_SIZE.get());
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

#[derive(Debug, Clone, Copy)]
enum FindResponse {
    Ok,
    Drop,
    NotFound,
    Stopping,
    Timeout,
}

#[derive(Clone, Copy)]
enum Provision {
    Immediate,
    Drop,
    Delay,
}

fn wrap_req(provision: Provision) -> impl Fn(HttpLaneRequest) -> (Provision, HttpLaneRequest) {
    move |req| (provision, req)
}

async fn fake_plane(responses: HashMap<Text, FindResponse>, mut find_rx: mpsc::Receiver<FindNode>) {
    let mut receivers = SelectAll::new();
    loop {
        let request = tokio::select! {
            maybe_request = receivers.next(), if !receivers.is_empty() => {
                if let Some(req) = maybe_request {
                    Either::Left(req)
                } else {
                    continue;
                }
            }
            maybe_find = find_rx.recv() => {
                if let Some(find) = maybe_find {
                    Either::Right(find)
                } else {
                    break;
                }
            }
        };

        match request {
            Either::Right(FindNode {
                node,
                lane,
                request,
                ..
            }) => match request {
                NodeConnectionRequest::Warp { .. } => panic!("Unexpected WARP resolution request."),
                NodeConnectionRequest::Http { promise } => match responses.get(&node).copied() {
                    Some(FindResponse::Ok) => {
                        let (tx, rx) = mpsc::channel(CHANNEL_SIZE.get());
                        promise.send(Ok(tx)).expect("Request dropped.");
                        receivers.push(ReceiverStream::new(rx).map(wrap_req(Provision::Immediate)));
                    }
                    Some(FindResponse::Drop) => {
                        let (tx, rx) = mpsc::channel(CHANNEL_SIZE.get());
                        promise.send(Ok(tx)).expect("Request dropped.");
                        receivers.push(ReceiverStream::new(rx).map(wrap_req(Provision::Drop)));
                    }
                    Some(FindResponse::Stopping) => {
                        promise
                            .send(Err(AgentResolutionError::PlaneStopping))
                            .expect("Request dropped.");
                    }
                    Some(FindResponse::Timeout) => {
                        let (tx, rx) = mpsc::channel(CHANNEL_SIZE.get());
                        promise.send(Ok(tx)).expect("Request dropped.");
                        receivers.push(ReceiverStream::new(rx).map(wrap_req(Provision::Delay)));
                    }
                    _ => {
                        promise
                            .send(Err(AgentResolutionError::NotFound(NoSuchAgent {
                                node,
                                lane,
                            })))
                            .expect("Request dropped.");
                    }
                },
            },
            Either::Left((provision, request)) => {
                let (_, response_tx) = request.into_parts();
                match provision {
                    Provision::Immediate => {
                        let response = RawHttpLaneResponse {
                            status_code: StatusCode::OK,
                            version: Version::HTTP_1_1,
                            headers: vec![],
                            payload: Bytes::from("Response"),
                        };
                        response_tx.send(response).expect("Channel dropped.");
                    }
                    Provision::Drop => {
                        drop(response_tx);
                    }
                    Provision::Delay => {
                        tokio::time::sleep(2 * REQ_TIMEOUT).await;
                        let response = RawHttpLaneResponse {
                            status_code: StatusCode::OK,
                            version: Version::HTTP_1_1,
                            headers: vec![],
                            payload: Bytes::from("Response"),
                        };
                        let _ = response_tx.send(response);
                    }
                }
            }
        }
    }
}

fn setup_responses() -> HashMap<Text, FindResponse> {
    [
        (Text::new("/node"), FindResponse::Ok),
        (Text::new("/fail"), FindResponse::Drop),
        (Text::new("/not_found"), FindResponse::NotFound),
        (Text::new("/stopping"), FindResponse::Stopping),
        (Text::new("/timeout"), FindResponse::Timeout),
    ]
    .into_iter()
    .collect()
}

async fn http_client(tx: mpsc::Sender<DuplexStream>, node: &str, lane: &str) -> Response<Body> {
    let (client, server) = tokio::io::duplex(BUFFER_SIZE);
    tx.send(server).await.expect("Failed to open channel.");
    let (mut sender, connection) = http1::handshake(client)
        .await
        .expect("HTTP handshake failed.");

    let send = async move {
        let mut request = Request::<Body>::default();
        let uri =
            Uri::try_from(format!("http://example:8080/{}?lane={}", node, lane)).expect("Bad URI.");
        *request.method_mut() = hyper::Method::GET;
        request.headers_mut().append(
            hyper::header::HOST,
            HeaderValue::from_str(uri.authority().unwrap().as_str()).unwrap(),
        );
        *request.uri_mut() = uri;
        sender.send_request(request).await.expect("Sending")
    };

    let (conn_result, response) = join(connection, send).await;
    assert!(conn_result.is_ok());
    response
}

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn with_timeout<F>(test_case: F) -> F::Output
where
    F: Future,
{
    tokio::time::timeout(TEST_TIMEOUT, test_case)
        .await
        .expect("Test timed out.")
}

#[tokio::test]
async fn good_http_request() {
    with_timeout(async move {
        let (tx, rx) = mpsc::channel(8);
        let (find_tx, find_rx) = mpsc::channel(CHANNEL_SIZE.get());
        let server = run_server(rx, find_tx);
        let responses = setup_responses();
        let agent = fake_plane(responses, find_rx);
        let client = http_client(tx, "node", "name");
        let (_, mut response, _) = join3(server, client, agent).await;
        assert_eq!(response.status(), hyper::StatusCode::OK);
        let body = std::mem::take(response.body_mut());
        let body_bytes = to_bytes(body).await.expect("Failed to read body.");
        assert_eq!(body_bytes.as_ref(), b"Response");
    })
    .await
}

#[tokio::test]
async fn not_found_http_request() {
    with_timeout(async move {
        let (tx, rx) = mpsc::channel(8);
        let (find_tx, find_rx) = mpsc::channel(CHANNEL_SIZE.get());
        let server = run_server(rx, find_tx);
        let responses = setup_responses();
        let agent = fake_plane(responses, find_rx);
        let client = http_client(tx, "not_found", "name");
        let (_, response, _) = join3(server, client, agent).await;
        assert_eq!(response.status(), hyper::StatusCode::NOT_FOUND);
    })
    .await
}

#[tokio::test]
async fn server_stopping_http_request() {
    with_timeout(async move {
        let (tx, rx) = mpsc::channel(8);
        let (find_tx, find_rx) = mpsc::channel(CHANNEL_SIZE.get());
        let server = run_server(rx, find_tx);
        let responses = setup_responses();
        let agent = fake_plane(responses, find_rx);
        let client = http_client(tx, "stopping", "name");
        let (_, response, _) = join3(server, client, agent).await;
        assert_eq!(response.status(), hyper::StatusCode::SERVICE_UNAVAILABLE);
    })
    .await
}

#[tokio::test]
async fn dropped_http_request() {
    with_timeout(async move {
        let (tx, rx) = mpsc::channel(8);
        let (find_tx, find_rx) = mpsc::channel(CHANNEL_SIZE.get());
        let server = run_server(rx, find_tx);
        let responses = setup_responses();
        let agent = fake_plane(responses, find_rx);
        let client = http_client(tx, "fail", "name");
        let (_, response, _) = join3(server, client, agent).await;
        assert_eq!(response.status(), hyper::StatusCode::INTERNAL_SERVER_ERROR);
    })
    .await
}

#[tokio::test]
async fn http_request_timeout() {
    with_timeout(async move {
        let (tx, rx) = mpsc::channel(8);
        let (find_tx, find_rx) = mpsc::channel(CHANNEL_SIZE.get());
        let server = run_server(rx, find_tx);
        let responses = setup_responses();
        let agent = fake_plane(responses, find_rx);
        let client = http_client(tx, "timeout", "name");
        let (_, response, _) = join3(server, client, agent).await;
        assert_eq!(response.status(), hyper::StatusCode::REQUEST_TIMEOUT);
    })
    .await
}
