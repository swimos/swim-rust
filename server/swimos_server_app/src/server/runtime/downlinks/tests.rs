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
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures::{
    future::{join, ready, Either},
    Future, FutureExt, SinkExt, StreamExt,
};
use parking_lot::Mutex;
use swimos_agent_protocol::encoding::{DownlinkOperationEncoder, ValueNotificationDecoder};
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_api::{downlink::DownlinkKind, error::DownlinkRuntimeError};
use swimos_messages::protocol::{
    Operation, RawRequestMessageDecoder, RequestMessage, ResponseMessage, ResponseMessageEncoder,
};
use swimos_model::{address::RelativeAddress, Text};
use swimos_net::SchemeHostPort;
use swimos_remote::net::dns::{DnsFut, DnsResolver};
use swimos_remote::{AttachClient, LinkError};
use swimos_runtime::{
    agent::{CommanderKey, CommanderRequest, DownlinkRequest, LinkRequest},
    downlink::{DownlinkOptions, DownlinkRuntimeConfig, Io},
};
use swimos_utilities::{
    io::byte_channel::{are_connected, ByteReader, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::server::runtime::{ClientRegistration, EstablishedClient, NewClientError};

use super::{downlink_task_connector, DlTaskRequest, DownlinkConnectionTask, ServerConnector};

struct TestContext {
    connector: ServerConnector,
}

struct FakeDns;

fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 5)), port)
}

const HOST: &str = "example.swimos";
const URL: &str = "warp://example.swimos:40000";
const BAD_URL: &str = "warp://other.swimos:40000";
const PORT: u16 = 40000;
const AGENT_ID: Uuid = Uuid::from_u128(5);

impl DnsResolver for FakeDns {
    type ResolveFuture = DnsFut;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        let result = match host.as_str() {
            HOST => Ok(vec![addr(port)]),
            _ => Err(std::io::Error::from(ErrorKind::NotFound)),
        };
        ready(result).boxed()
    }
}

const CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);
const TIMEOUT: Duration = Duration::from_secs(5);

async fn run_downlinks_test<F, Fut>(config: DownlinkRuntimeConfig, test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    tokio::time::timeout(TIMEOUT, async move {
        let (server_end, downlinks_end) = downlink_task_connector(CHAN_SIZE, CHAN_SIZE);

        let task = DownlinkConnectionTask::new(downlinks_end, None, config, FakeDns);

        let test_task = test_case(TestContext {
            connector: server_end,
        });

        let (_, out) = join(task.run(), test_task).await;

        out
    })
    .await
    .expect("Test timed out.")
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_BUFFER: NonZeroUsize = non_zero_usize!(4096);

const CONFIG: DownlinkRuntimeConfig = DownlinkRuntimeConfig {
    empty_timeout: DEFAULT_TIMEOUT,
    attachment_queue_size: CHAN_SIZE,
    abort_on_bad_frames: true,
    remote_buffer_size: DEFAULT_BUFFER,
    downlink_buffer_size: DEFAULT_BUFFER,
};

enum Endpoint {
    TwoWay {
        local: bool,
        address: RelativeAddress<Text>,
        io: Io,
    },
    OneWayLocal {
        address: RelativeAddress<Text>,
        reader: ByteReader,
    },
    OneWayRemote {
        reader: ByteReader,
    },
}

struct FakeServerTask {
    port: u16,
    connector: ServerConnector,
    stop: trigger::Receiver,
    endpoints: Arc<Mutex<HashMap<Uuid, Endpoint>>>,
}

impl FakeServerTask {
    fn new(port: u16, connector: ServerConnector) -> (trigger::Sender, Self) {
        let (tx, rx) = trigger::trigger();
        (
            tx,
            FakeServerTask {
                port,
                connector,
                stop: rx,
                endpoints: Default::default(),
            },
        )
    }
}

struct Endpoints {
    inner: Arc<Mutex<HashMap<Uuid, Endpoint>>>,
}

fn check_hosts(actual: &str, expected: &str) {
    let actual = actual.parse::<SchemeHostPort>().expect("Invalid host.");
    let expected = expected.parse::<SchemeHostPort>().expect("Invalid host.");
    assert_eq!(actual, expected);
}

impl Endpoints {
    fn take_two_way_endpoint(&self, loc: bool, node: &str) -> (Uuid, Io) {
        let mut guard = self.inner.lock();

        let key = guard.iter().find_map(|(k, v)| match v {
            Endpoint::TwoWay { local, address, .. } => {
                if *local == loc && address.node == node {
                    Some(*k)
                } else {
                    None
                }
            }
            _ => None,
        });
        let (id, endpoint) = key
            .and_then(|k| guard.remove(&k).map(|ep| (k, ep)))
            .expect("No such endpoint.");
        (
            id,
            match endpoint {
                Endpoint::TwoWay { io, .. } => io,
                _ => unreachable!(),
            },
        )
    }

    fn take_one_way_endpoint(&self, loc: bool, node: &str) -> (Uuid, ByteReader) {
        let mut guard = self.inner.lock();

        let key = guard.iter().find_map(|(k, v)| match v {
            Endpoint::OneWayLocal { address, .. } if loc => {
                if address.node == node {
                    Some(*k)
                } else {
                    None
                }
            }
            Endpoint::OneWayRemote { .. } if !loc => Some(*k),
            _ => None,
        });
        let (id, endpoint) = key
            .and_then(|k| guard.remove(&k).map(|ep| (k, ep)))
            .expect("No such endpoint.");
        (
            id,
            match endpoint {
                Endpoint::OneWayLocal { reader, .. } | Endpoint::OneWayRemote { reader } => reader,
                _ => unreachable!(),
            },
        )
    }
}

impl FakeServerTask {
    fn endpoints(&self) -> Endpoints {
        Endpoints {
            inner: self.endpoints.clone(),
        }
    }

    async fn run(self) -> ServerConnector {
        let FakeServerTask {
            mut connector,
            mut stop,
            port,
            endpoints,
        } = self;

        let addr = addr(port);
        let (attach_tx, mut attach_rx) = mpsc::channel(CHAN_SIZE.get());
        let mut stopping = false;

        loop {
            let event = if stopping {
                tokio::select! {
                    event = connector.next_message() => event.map(Either::Left),
                    remote_attach = attach_rx.recv() => remote_attach.map(Either::Right),
                }
            } else {
                tokio::select! {
                    biased;
                    _ = &mut stop => {
                        connector.stop();
                        stopping = true;
                        continue;
                    },
                    event = connector.next_message() => event.map(Either::Left),
                    remote_attach = attach_rx.recv() => remote_attach.map(Either::Right),
                }
            };
            match event {
                Some(Either::Left(DlTaskRequest::Registration(ClientRegistration {
                    host,
                    sock_addrs,
                    responder,
                    ..
                }))) => {
                    check_hosts(host.as_str(), URL);
                    let result = if sock_addrs.iter().any(|a| a == &addr) {
                        Ok(EstablishedClient {
                            tx: attach_tx.clone(),
                            sock_addr: addr,
                        })
                    } else {
                        Err(NewClientError::OpeningSocketFailed { errors: vec![] })
                    };
                    assert!(responder.send(result).is_ok());
                }
                Some(Either::Left(DlTaskRequest::Local(local))) => match local {
                    AttachClient::OneWay {
                        agent_id,
                        path,
                        receiver,
                        done,
                    } => {
                        let mut guard = endpoints.lock();
                        assert!(!guard.contains_key(&agent_id));
                        let path = path.expect("Path missing.");
                        let result = if path.node == LOCAL_NODE && path.lane == LANE {
                            guard.insert(
                                agent_id,
                                Endpoint::OneWayLocal {
                                    address: path,
                                    reader: receiver,
                                },
                            );
                            Ok(())
                        } else {
                            Err(LinkError::NoEndpoint(path))
                        };
                        assert!(done.send(result).is_ok());
                    }
                    AttachClient::AttachDownlink {
                        downlink_id,
                        path,
                        sender,
                        receiver,
                        done,
                    } => {
                        let mut guard = endpoints.lock();
                        assert!(!guard.contains_key(&downlink_id));
                        let result = if path.node == LOCAL_NODE && path.lane == LANE {
                            guard.insert(
                                downlink_id,
                                Endpoint::TwoWay {
                                    local: true,
                                    address: path,
                                    io: (sender, receiver),
                                },
                            );
                            Ok(())
                        } else {
                            Err(LinkError::NoEndpoint(path))
                        };
                        assert!(done.send(result).is_ok());
                    }
                },
                Some(Either::Right(req)) => match req {
                    AttachClient::OneWay {
                        agent_id,
                        path,
                        receiver,
                        done,
                        ..
                    } => {
                        let mut guard = endpoints.lock();
                        assert!(!guard.contains_key(&agent_id));
                        assert!(path.is_none());
                        guard.insert(agent_id, Endpoint::OneWayRemote { reader: receiver });
                        assert!(done.send(Ok(())).is_ok());
                    }
                    AttachClient::AttachDownlink {
                        downlink_id,
                        path,
                        sender,
                        receiver,
                        done,
                    } => {
                        assert_eq!(path.node, REM_NODE);
                        assert_eq!(path.lane, LANE);
                        let mut guard = endpoints.lock();
                        assert!(!guard.contains_key(&downlink_id));
                        guard.insert(
                            downlink_id,
                            Endpoint::TwoWay {
                                local: false,
                                address: path,
                                io: (sender, receiver),
                            },
                        );
                        assert!(done.send(Ok(())).is_ok());
                    }
                },
                _ => {
                    break;
                }
            }
        }
        connector
    }
}

#[tokio::test]
async fn clean_shutdown() {
    run_downlinks_test(CONFIG, |mut context| async move {
        let TestContext { connector } = &mut context;
        connector.stop();
        assert!(connector.next_message().await.is_none());
    })
    .await;
}

const REM_NODE: &str = "/remote";
const LOCAL_NODE: &str = "/local";
const BAD_NODE: &str = "/bad";
const LANE: &str = "lane";

fn request_remote(
    kind: DownlinkKind,
    promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
) -> DownlinkRequest {
    let address = RelativeAddress::text(REM_NODE, LANE);
    //Empty options so that downlinks don't try to sync (to reduce noise in the tests).
    DownlinkRequest::new(
        Some(URL.parse().unwrap()),
        address,
        kind,
        DownlinkOptions::empty(),
        promise,
    )
}

fn request_bad_remote(
    kind: DownlinkKind,
    promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
) -> DownlinkRequest {
    let address = RelativeAddress::text(REM_NODE, LANE);
    DownlinkRequest::new(
        Some(BAD_URL.parse().unwrap()),
        address,
        kind,
        DownlinkOptions::empty(),
        promise,
    )
}

fn request_local(
    kind: DownlinkKind,
    promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
) -> DownlinkRequest {
    let address = RelativeAddress::text(LOCAL_NODE, LANE);
    //Empty options so that downlinks don't try to sync (to reduce noise in the tests).
    DownlinkRequest::new(None, address, kind, DownlinkOptions::empty(), promise)
}

fn request_bad_local(
    kind: DownlinkKind,
    promise: oneshot::Sender<Result<Io, DownlinkRuntimeError>>,
) -> DownlinkRequest {
    let address = RelativeAddress::text(BAD_NODE, LANE);
    //Empty options so that downlinks don't try to sync (to reduce noise in the tests).
    DownlinkRequest::new(None, address, kind, DownlinkOptions::empty(), promise)
}

#[tokio::test]
async fn open_remote_downlink() {
    run_downlinks_test(CONFIG, |context| async move {
        let TestContext { connector } = context;

        let requests = connector.link_requests();
        let (stop_server, server_task) = FakeServerTask::new(PORT, connector);

        let endpoints = server_task.endpoints();

        let (connected_tx, connected_rx) = oneshot::channel();
        let request = request_remote(DownlinkKind::Value, connected_tx);

        let test = async move {
            assert!(requests.send(LinkRequest::Downlink(request)).await.is_ok());

            let mut io = connected_rx
                .await
                .expect("Stopped prematurely.")
                .expect("Connection failed.");

            let (dl_id, rem_io) = endpoints.take_two_way_endpoint(false, REM_NODE);

            io = verify_link_value_dl(dl_id, io, rem_io, REM_NODE).await;

            assert!(stop_server.trigger());

            expect_unlinked_value(io).await;
        };

        join(server_task.run(), test).await
    })
    .await;
}

async fn expect_unlinked_value(io: Io) {
    let (_writer, reader) = io;
    let mut read = FramedRead::new(reader, ValueNotificationDecoder::<i32>::default());
    if !matches!(read.next().await, Some(Ok(DownlinkNotification::Unlinked))) {
        panic!("Did not get unlinked.");
    }
}

const REMOTE_ID: Uuid = Uuid::from_u128(1);

fn verify_command_link(cmd_tx: ByteWriter, socket_rx: ByteReader) -> ByteWriter {
    assert!(are_connected(&cmd_tx, &socket_rx));
    cmd_tx
}

async fn verify_link_value_dl(id: Uuid, downlink: Io, socket: Io, node: &str) -> Io {
    let (socket_tx, socket_rx) = socket;
    let (mut dl_tx, mut dl_rx) = downlink;

    let mut sock_writer = FramedWrite::new(socket_tx, ResponseMessageEncoder);
    let mut sock_reader = FramedRead::new(socket_rx, RawRequestMessageDecoder);

    let mut dl_writer = FramedWrite::new(&mut dl_tx, DownlinkOperationEncoder::default());
    let mut dl_reader = FramedRead::new(&mut dl_rx, ValueNotificationDecoder::<i32>::default());

    let env = sock_reader
        .next()
        .await
        .expect("Downlink stopped.")
        .expect("Bad frame.");

    //Expect a link message from the downlink runtime on the socket.
    let RequestMessage {
        origin,
        path,
        envelope,
    } = env;
    assert_eq!(origin, id);
    assert_eq!(path.node.as_str(), node);
    assert_eq!(path.lane.as_str(), LANE);

    match envelope {
        Operation::Link => {}
        ow => panic!("Unexpected envelope: {:?}", ow),
    }

    //Send a linked message to the downlink runtime.
    let addr = RelativeAddress::new(node, LANE);
    sock_writer
        .send(ResponseMessage::<_, i32, Bytes>::linked(REMOTE_ID, addr))
        .await
        .expect("Sending envelope failed.");

    //Expect the linked message to be propagated to the downlink.
    let not = dl_reader
        .next()
        .await
        .expect("Downlink stopped.")
        .expect("Invalid frame.");
    assert!(matches!(not, DownlinkNotification::Linked));

    //Send an outgoing message.
    dl_writer
        .send(DownlinkOperation { body: 5 })
        .await
        .expect("Downlink failed.");

    //Expect the outgoing message at the socket.
    let env = sock_reader
        .next()
        .await
        .expect("Downlink stopped.")
        .expect("Bad frame.");

    let RequestMessage {
        origin,
        path,
        envelope,
    } = env;
    assert_eq!(origin, id);
    assert_eq!(path.node.as_str(), node);
    assert_eq!(path.lane.as_str(), LANE);

    match envelope {
        Operation::Command(body) => {
            let body_str = std::str::from_utf8(body.as_ref()).expect("Invalid UTF8");
            assert_eq!(body_str, "5");
        }
        ow => panic!("Unexpected envelope: {:?}", ow),
    }

    (dl_tx, dl_rx)
}

#[tokio::test]
async fn open_local_downlink() {
    run_downlinks_test(CONFIG, |context| async move {
        let TestContext { connector } = context;

        let requests = connector.link_requests();
        let (stop_server, server_task) = FakeServerTask::new(PORT, connector);

        let endpoints = server_task.endpoints();

        let (connected_tx, connected_rx) = oneshot::channel();
        let request = request_local(DownlinkKind::Value, connected_tx);

        let test = async move {
            assert!(requests.send(LinkRequest::Downlink(request)).await.is_ok());

            let mut io = connected_rx
                .await
                .expect("Stopped prematurely.")
                .expect("Connection failed.");

            let (dl_id, local_io) = endpoints.take_two_way_endpoint(true, LOCAL_NODE);

            io = verify_link_value_dl(dl_id, io, local_io, LOCAL_NODE).await;

            assert!(stop_server.trigger());

            expect_unlinked_value(io).await;
        };

        join(server_task.run(), test).await
    })
    .await;
}

#[tokio::test]
async fn open_unresolvable_remote_downlink() {
    run_downlinks_test(CONFIG, |context| async move {
        let TestContext { connector } = context;

        let requests = connector.link_requests();
        let (stop_server, server_task) = FakeServerTask::new(PORT, connector);

        let (connected_tx, connected_rx) = oneshot::channel();
        let request = request_bad_remote(DownlinkKind::Value, connected_tx);

        let test = async move {
            assert!(requests.send(LinkRequest::Downlink(request)).await.is_ok());

            let error = connected_rx
                .await
                .expect("Stopped prematurely.")
                .expect_err("Resolution should fail.");

            assert!(matches!(
                error,
                DownlinkRuntimeError::DownlinkConnectionFailed(_)
            ));

            assert!(stop_server.trigger());
        };

        join(server_task.run(), test).await
    })
    .await;
}

#[tokio::test]
async fn open_unresolvable_local_downlink() {
    run_downlinks_test(CONFIG, |context| async move {
        let TestContext { connector } = context;

        let requests = connector.link_requests();
        let (stop_server, server_task) = FakeServerTask::new(PORT, connector);

        let (connected_tx, connected_rx) = oneshot::channel();
        let request = request_bad_local(DownlinkKind::Value, connected_tx);

        let test = async move {
            assert!(requests.send(LinkRequest::Downlink(request)).await.is_ok());

            let error = connected_rx
                .await
                .expect("Stopped prematurely.")
                .expect_err("Resolution should fail.");

            assert!(matches!(
                error,
                DownlinkRuntimeError::DownlinkConnectionFailed(_)
            ));

            assert!(stop_server.trigger());
        };

        join(server_task.run(), test).await
    })
    .await;
}

fn request_remote_cmd(
    promise: oneshot::Sender<Result<ByteWriter, DownlinkRuntimeError>>,
) -> CommanderRequest {
    let key = CommanderKey::Remote(URL.parse().unwrap());
    CommanderRequest::new(AGENT_ID, key, promise)
}

#[tokio::test]
async fn open_remote_command_channel() {
    run_downlinks_test(CONFIG, |context| async move {
        let TestContext { connector } = context;

        let requests = connector.link_requests();
        let (stop_server, server_task) = FakeServerTask::new(PORT, connector);

        let endpoints = server_task.endpoints();

        let (connected_tx, connected_rx) = oneshot::channel();
        let request = request_remote_cmd(connected_tx);

        let test = async move {
            assert!(requests.send(LinkRequest::Commander(request)).await.is_ok());

            let writer = connected_rx
                .await
                .expect("Stopped prematurely.")
                .expect("Connection failed.");

            let (id, reader) = endpoints.take_one_way_endpoint(false, REM_NODE);
            assert_eq!(id, AGENT_ID);

            verify_command_link(writer, reader);

            assert!(stop_server.trigger());
        };

        join(server_task.run(), test).await
    })
    .await;
}
