// Copyright 2015-2021 Swim Inc.
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
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bytes::BytesMut;
use futures::{future::{join3, join}, Future};
use ratchet::{Message, NegotiatedExtension, NoExt, Role, WebSocket, WebSocketConfig};
use swim_form::structural::write::StructuralWritable;
use swim_recon::printer::print_recon_compact;
use swim_utilities::routing::route_pattern::RoutePattern;

use swim_warp::envelope::{peel_envelope_header, RawEnvelope};
use tokio::{
    io::{duplex, DuplexStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

use crate::{plane::PlaneBuilder, Server, ServerHandle, SwimServerConfig};

use self::{
    agent::TestAgent,
    connections::{TestConnections, TestWs},
};

use super::SwimServer;
use agent::{TestMessage, LANE};

mod agent;
mod connections;

struct TestContext {
    report_rx: UnboundedReceiver<i32>,
    incoming_tx: UnboundedSender<(SocketAddr, DuplexStream)>,
    handle: ServerHandle,
}

const NODE: &str = "/node";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const BUFFER_SIZE: usize = 4096;
fn remote_addr(p: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, p)), 50000)
}

async fn run_server<F, Fut>(test_case: F) -> (Result<(), std::io::Error>, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let mut plane_builder = PlaneBuilder::default();
    let pattern = RoutePattern::parse_str(NODE).expect("Invalid route.");

    let (report_tx, report_rx) = mpsc::unbounded_channel();

    plane_builder.add_route(
        pattern,
        TestAgent::new(report_tx, |uri, _conf| {
            assert_eq!(uri, "/node");
        }),
    );

    let plane = plane_builder.build().expect("Invalid plane definition.");
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080);

    let resolve = HashMap::new();
    let remotes = HashMap::new();

    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

    let (networking, networking_task) = TestConnections::new(resolve, remotes, incoming_rx);
    let websockets = TestWs::default();
    let config = SwimServerConfig::default();

    let server = SwimServer::new(plane, addr, networking, websockets, config);

    let (server_task, handle) = server.run();
    let context = TestContext {
        report_rx,
        handle,
        incoming_tx,
    };

    let net = networking_task.run();

    let test_task = test_case(context);

    let (_, task_result, result) =
        tokio::time::timeout(TEST_TIMEOUT, join3(net, server_task, test_task))
            .await
            .expect("Test timed out.");
    (task_result, result)
}

#[tokio::test]
async fn server_clean_shutdown() {
    let (result, _) = run_server(|mut context| async move {
        context.handle.stop();
        context
    })
    .await;
    assert!(result.is_ok());
}

struct TestClient {
    ws: WebSocket<DuplexStream, NoExt>,
    read_buffer: BytesMut,
}

impl TestClient {
    fn new(stream: DuplexStream) -> Self {
        TestClient {
            ws: WebSocket::from_upgraded(
                WebSocketConfig::default(),
                stream,
                NegotiatedExtension::from(None),
                BytesMut::new(),
                Role::Client,
            ),
            read_buffer: BytesMut::new(),
        }
    }

    async fn link(&mut self, node: &str, lane: &str) {
        let envelope = format!("@link(node:\"{}\",lane:{})", node, lane);
        self.ws.write_text(envelope).await.expect("Write failed.");
    }

    async fn sync(&mut self, node: &str, lane: &str) {
        let envelope = format!("@sync(node:\"{}\",lane:{})", node, lane);
        self.ws.write_text(envelope).await.expect("Write failed.");
    }

    async fn unlink(&mut self, node: &str, lane: &str) {
        let envelope = format!("@unlink(node:\"{}\",lane:{})", node, lane);
        self.ws.write_text(envelope).await.expect("Write failed.");
    }

    async fn command<T: StructuralWritable>(&mut self, node: &str, lane: &str, body: T) {
        let body = format!("{}", print_recon_compact(&body));
        let envelope = format!("@command(node:\"{}\",lane:{}) {}", node, lane, body);
        self.ws.write_text(envelope).await.expect("Write failed.")
    }

    async fn expect_envelope<'a>(&'a mut self) -> RawEnvelope<'a> {
        let TestClient { ws, read_buffer } = self;
        read_buffer.clear();
        let message = ws.read(read_buffer).await.expect("Read failed.");
        assert_eq!(message, Message::Text);
        let bytes: &'a [u8] = (*read_buffer).as_ref();
        peel_envelope_header(bytes).expect("Invalid envelope")
    }

    async fn expect_linked(&mut self, node: &str, lane: &str) {
        let envelope = self.expect_envelope().await;
        match envelope {
            RawEnvelope::Linked {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, node);
                assert_eq!(lane_uri, lane);
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }
    }

    async fn expect_synced(&mut self, node: &str, lane: &str) {
        let envelope = self.expect_envelope().await;
        match envelope {
            RawEnvelope::Synced {
                node_uri, lane_uri, ..
            } => {
                assert_eq!(node_uri, node);
                assert_eq!(lane_uri, lane);
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }
    }

    async fn expect_unlinked(&mut self, node: &str, lane: &str, expected_body: &str) {
        let envelope = self.expect_envelope().await;
        match envelope {
            RawEnvelope::Unlinked {
                node_uri,
                lane_uri,
                body,
                ..
            } => {
                assert_eq!(node_uri, node);
                assert_eq!(lane_uri, lane);
                assert_eq!(*body, expected_body);
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }
    }

    async fn expect_event(&mut self, node: &str, lane: &str, expected_body: &str) {
        self.get_event(node, lane, move |body| {
            assert_eq!(body, expected_body);
        }).await
    }

    async fn get_event<F, T>(&mut self, node: &str, lane: &str, f: F) -> T
    where
        F: FnOnce(&str) -> T,
    {
        let envelope = self.expect_envelope().await;
        match envelope {
            RawEnvelope::Event {
                node_uri,
                lane_uri,
                body,
                ..
            } => {
                assert_eq!(node_uri, node);
                assert_eq!(lane_uri, lane);
                f(*body)
            }
            ow => panic!("Unexpected envelope: {:?}", ow),
        }
    }
}

#[tokio::test]
async fn message_for_nonexistent_agent() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext { incoming_tx, .. } = &context;

        let (client_sock, server_sock) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock))
            .expect("Listener closed.");

        let mut client = TestClient::new(client_sock);

        client.link("/other", "lane").await;

        client
            .expect_unlinked("/other", "lane", "@nodeNotFound")
            .await;

        context.handle.stop();
        context
    })
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn command_to_agent() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext {
            incoming_tx,
            report_rx,
            ..
        } = &mut context;

        let (client_sock, server_sock) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock))
            .expect("Listener closed.");

        let mut client = TestClient::new(client_sock);

        client
            .command(NODE, LANE, TestMessage::SetAndReport(56))
            .await;

        assert_eq!(report_rx.recv().await.expect("Agent stopped."), 56);

        context.handle.stop();
        context
    })
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn link_to_agent_lane() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext { incoming_tx, .. } = &mut context;

        let (client_sock, server_sock) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock))
            .expect("Listener closed.");

        let mut client = TestClient::new(client_sock);

        client.link(NODE, LANE).await;

        client.expect_linked(NODE, LANE).await;

        context.handle.stop();

        client.expect_unlinked(NODE, LANE, "").await;

        context
    })
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn sync_with_agent_lane() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext { incoming_tx, .. } = &mut context;

        let (client_sock, server_sock) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock))
            .expect("Listener closed.");

        let mut client = TestClient::new(client_sock);

        client.sync(NODE, LANE).await;

        client.expect_linked(NODE, LANE).await;
        client.expect_event(NODE, LANE, "0").await;
        client.expect_synced(NODE, LANE).await;

        context.handle.stop();

        client.expect_unlinked(NODE, LANE, "").await;

        context
    })
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn trigger_event() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext { incoming_tx, .. } = &mut context;

        let (client_sock, server_sock) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock))
            .expect("Listener closed.");

        let mut client = TestClient::new(client_sock);

        client.link(NODE, LANE).await;

        client.expect_linked(NODE, LANE).await;

        client.command(NODE, LANE, TestMessage::Event).await;

        client.expect_event(NODE, LANE, "0").await;

        context.handle.stop();

        client.expect_unlinked(NODE, LANE, "").await;

        context
    })
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn broadcast_events() {
    let (result, _) = run_server(|mut context| async move {
        let TestContext {
            incoming_tx,
            report_rx,
            ..
        } = &mut context;

        let (client_sock1, server_sock1) = duplex(BUFFER_SIZE);
        let (client_sock2, server_sock2) = duplex(BUFFER_SIZE);

        incoming_tx
            .send((remote_addr(1), server_sock1))
            .expect("Listener closed.");

        incoming_tx
            .send((remote_addr(2), server_sock2))
            .expect("Listener closed.");

        let mut client1 = TestClient::new(client_sock1);
        let mut client2 = TestClient::new(client_sock2);

        let event_consumer = async move {
            client1.link(NODE, LANE).await;

            client1.expect_linked(NODE, LANE).await;

            //Events should be in order and we should eventually see the final value.
            let prev = -1;
            loop {
                let n = client1.get_event(NODE, LANE, |body| {
                    body.parse::<i32>().expect("Invalid body.")
                }).await;
                assert!(prev < n && n < 10);
                if n == 0 {
                    break;
                }
            }
            client1
        };

        let event_generator = async move {
            client2.command(NODE, LANE, TestMessage::Event).await;
            for i in 1..10 {
                client2
                    .command(NODE, LANE, TestMessage::SetAndReport(i))
                    .await;
                client2.command(NODE, LANE, TestMessage::Event).await;
                assert_eq!(report_rx.recv().await.expect("Task stopped."), i);
            }
            client2
        };

        let (mut client1, mut client2) = join(event_consumer, event_generator).await;

        context.handle.stop();

        join(
            client1.expect_unlinked(NODE, LANE, ""), 
            client2.expect_unlinked(NODE, LANE, ""))
        .await;

        context
    })
    .await;
    assert!(result.is_ok());
}
