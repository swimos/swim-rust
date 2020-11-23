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

use crate::routing::error::ConnectionError;
use crate::routing::remote::state::{DeferredResult, Event, RemoteTasksState};
use crate::routing::remote::table::{HostAndPort, RoutingTable};
use crate::routing::remote::{
    ConnectionDropped, ResolutionRequest, RoutingRequest, SocketAddrIt, Unresolvable,
};
use crate::routing::{Route, RoutingAddr, TaggedEnvelope};
use futures::{FutureExt, StreamExt};
use std::cell::RefCell;
use std::io::ErrorKind;
use std::net::SocketAddr;
use swim_common::model::Value;
use swim_common::request::Request;
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::Envelope;
use tokio::sync::{mpsc, oneshot};
use utilities::sync::promise::Sender;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeSocket(String);

impl FakeSocket {
    fn new(name: &str) -> Self {
        FakeSocket(name.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FakeWebsocket(String);

impl FakeWebsocket {
    fn new(name: &str) -> Self {
        FakeWebsocket(name.to_string())
    }
}

const RESP: RoutingAddr = RoutingAddr::local(6);

#[derive(Debug, Default)]
struct FakeRemoteState {
    table: RoutingTable,
    recording: RefCell<Vec<StateMutation>>,
}

impl FakeRemoteState {
    fn check(&self, expected: Vec<StateMutation>) {
        assert_eq!(*self.recording.borrow(), expected);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum StateMutation {
    Stop,
    Spawn(SocketAddr, FakeWebsocket, Option<HostAndPort>),
    CheckAddr(HostAndPort, SocketAddr),
    DeferHandshake(FakeSocket, SocketAddr),
    DeferConnect(HostAndPort, SocketAddr, Vec<SocketAddr>),
    DeferDns(HostAndPort),
    FailConnection(HostAndPort, ConnectionError),
    TableRemove(RoutingAddr),
}

impl RemoteTasksState for FakeRemoteState {
    type Socket = FakeSocket;
    type WebSocket = FakeWebsocket;

    fn stop(&mut self) {
        self.recording.get_mut().push(StateMutation::Stop);
    }

    fn spawn_task(
        &mut self,
        sock_addr: SocketAddr,
        ws_stream: Self::WebSocket,
        host: Option<HostAndPort>,
    ) {
        self.recording
            .get_mut()
            .push(StateMutation::Spawn(sock_addr, ws_stream, host))
    }

    fn check_socket_addr(
        &mut self,
        host: HostAndPort,
        sock_addr: SocketAddr,
    ) -> Result<(), HostAndPort> {
        let FakeRemoteState { table, recording } = self;
        recording
            .get_mut()
            .push(StateMutation::CheckAddr(host.clone(), sock_addr.clone()));
        if table.get_resolved(&sock_addr).is_some() {
            Ok(())
        } else {
            Err(host)
        }
    }

    fn defer_handshake(&self, stream: Self::Socket, peer_addr: SocketAddr) {
        self.recording
            .borrow_mut()
            .push(StateMutation::DeferHandshake(stream, peer_addr));
    }

    fn defer_connect_and_handshake(
        &mut self,
        host: HostAndPort,
        sock_addr: SocketAddr,
        remaining: SocketAddrIt,
    ) {
        self.recording.get_mut().push(StateMutation::DeferConnect(
            host,
            sock_addr,
            remaining.collect(),
        ));
    }

    fn defer_dns_lookup(&mut self, target: HostAndPort, request: ResolutionRequest) {
        self.recording
            .get_mut()
            .push(StateMutation::DeferDns(target));
        assert!(request.send_ok(RESP).is_ok());
    }

    fn fail_connection(&mut self, host: &HostAndPort, error: ConnectionError) {
        self.recording
            .get_mut()
            .push(StateMutation::FailConnection(host.clone(), error));
    }

    fn table_resolve(&self, addr: RoutingAddr) -> Option<Route<mpsc::Sender<TaggedEnvelope>>> {
        self.table.resolve(addr)
    }

    fn table_try_resolve(&self, target: &HostAndPort) -> Option<RoutingAddr> {
        self.table.try_resolve(target)
    }

    fn table_remove(&mut self, addr: RoutingAddr) -> Option<Sender<ConnectionDropped>> {
        self.recording
            .get_mut()
            .push(StateMutation::TableRemove(addr));
        self.table.remove(addr)
    }
}

fn sock_addr() -> SocketAddr {
    "192.168.0.1:80".parse().unwrap()
}

fn sock_addr2() -> SocketAddr {
    "192.168.0.2:80".parse().unwrap()
}

#[test]
fn transition_incoming_ok() {
    let fake_sock = FakeSocket::new("a");
    let sa = sock_addr();

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Incoming(Ok((fake_sock.clone(), sa.clone())));
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::DeferHandshake(fake_sock, sa)]);
    assert!(result.is_ok());
}

#[test]
fn transition_incoming_err() {
    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Incoming(Err(ErrorKind::Interrupted.into()));
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::Stop]);
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().kind(), ErrorKind::Interrupted);
}

fn make_env(addr: RoutingAddr) -> TaggedEnvelope {
    TaggedEnvelope(
        addr,
        Envelope::make_event("/node", "lane", Some(Value::text("body"))),
    )
}

#[tokio::test]
async fn transition_request_endpoint_in_table() {
    let sa = sock_addr();
    let addr = RoutingAddr::remote(10);
    let envelope = make_env(addr);
    let (req_tx, req_rx) = oneshot::channel();
    let (route_tx, mut route_rx) = mpsc::channel(8);

    let request = Request::new(req_tx);

    let mut state = FakeRemoteState::default();
    state.table.insert(addr, None, sa, route_tx);
    let mut result = Ok(());

    let event = Event::Request(RoutingRequest::Endpoint { addr, request });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![]);
    assert!(result.is_ok());

    let result = req_rx.await;
    match result {
        Ok(Ok(Route { mut sender, .. })) => {
            assert!(sender.send_item(envelope.clone()).await.is_ok());
            assert_eq!(route_rx.next().now_or_never(), Some(Some(envelope)))
        }
        ow => {
            panic!("Unexpected failure {:?}.", ow);
        }
    }
}

#[tokio::test]
async fn transition_request_endpoint_not_in_table() {
    let addr = RoutingAddr::remote(10);
    let (req_tx, req_rx) = oneshot::channel();

    let request = Request::new(req_tx);

    let mut state = FakeRemoteState::default();

    let mut result = Ok(());

    let event = Event::Request(RoutingRequest::Endpoint { addr, request });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![]);
    assert!(result.is_ok());

    let result = req_rx.await;
    assert!(matches!(result, Ok(Err(Unresolvable(a))) if a == addr));
}

#[tokio::test]
async fn transition_request_resolve_in_table() {
    let sa = sock_addr();
    let addr = RoutingAddr::remote(10);
    let host = "swim://my_host:80".parse().unwrap();
    let (req_tx, req_rx) = oneshot::channel();
    let (route_tx, _route_rx) = mpsc::channel(8);

    let request = Request::new(req_tx);

    let mut state = FakeRemoteState::default();
    state.table.insert(
        addr,
        Some(HostAndPort::new("my_host".to_string(), 80)),
        sa,
        route_tx,
    );
    let mut result = Ok(());

    let event = Event::Request(RoutingRequest::ResolveUrl { host, request });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![]);
    assert!(result.is_ok());

    let result = req_rx.await;
    assert!(matches!(result, Ok(Ok(a)) if a == addr));
}

#[tokio::test]
async fn transition_request_resolve_not_in_table() {
    let host = "swim://my_host:80".parse().unwrap();
    let (req_tx, req_rx) = oneshot::channel();

    let request = Request::new(req_tx);

    let mut state = FakeRemoteState::default();

    let mut result = Ok(());

    let event = Event::Request(RoutingRequest::ResolveUrl { host, request });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::DeferDns(HostAndPort::new(
        "my_host".to_string(),
        80,
    ))]);
    assert!(result.is_ok());

    //Dummy response to ensure the request was forwarded correctly.
    let result = req_rx.await;
    assert!(matches!(result, Ok(Ok(a)) if a == RESP));
}

#[test]
fn transition_deferred_dns_good_in_table() {
    let addr = RoutingAddr::remote(10);
    let sa1 = sock_addr();
    let sa2 = sock_addr2();
    let host = HostAndPort::new("my_host".to_string(), 80);
    let (route_tx, _route_rx) = mpsc::channel(8);

    let dns_response = Ok(vec![sa1, sa2].into_iter());

    let mut state = FakeRemoteState::default();
    state.table.insert(addr, Some(host.clone()), sa1, route_tx);
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::Dns {
        result: dns_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::CheckAddr(host, sa1)]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_dns_good_not_in_table() {
    let sa1 = sock_addr();
    let sa2 = sock_addr2();
    let host = HostAndPort::new("my_host".to_string(), 80);

    let dns_response = Ok(vec![sa1, sa2].into_iter());

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::Dns {
        result: dns_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![
        StateMutation::CheckAddr(host.clone(), sa1),
        StateMutation::DeferConnect(host, sa1, vec![sa2]),
    ]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_dns_empty() {
    let host = HostAndPort::new("my_host".to_string(), 80);

    let dns_response = Ok(vec![].into_iter());
    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::Dns {
        result: dns_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::FailConnection(
        host,
        ConnectionError::Resolution,
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_dns_failed() {
    let host = HostAndPort::new("my_host".to_string(), 80);

    let dns_response = Err(ErrorKind::NotFound.into());
    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::Dns {
        result: dns_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::FailConnection(
        host,
        ConnectionError::Socket(ErrorKind::NotFound),
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_server_handshake_success() {
    let sa = sock_addr();

    let handshake_response = Ok(FakeWebsocket::new("ws"));

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::ServerHandshake {
        result: handshake_response,
        sock_addr: sa,
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::Spawn(
        sa,
        FakeWebsocket::new("ws"),
        None,
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_client_handshake_success() {
    let sa = sock_addr();
    let host = HostAndPort::new("my_host".to_string(), 80);

    let handshake_response = Ok((FakeWebsocket::new("ws"), sa));

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::ClientHandshake {
        result: handshake_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::Spawn(
        sa,
        FakeWebsocket::new("ws"),
        Some(host),
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_server_handshake_failed() {
    let sa = sock_addr();

    let handshake_response = Err(ConnectionError::Socket(ErrorKind::ConnectionReset.into()));

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::ServerHandshake {
        result: handshake_response,
        sock_addr: sa,
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_client_handshake_failed() {
    let host = HostAndPort::new("my_host".to_string(), 80);

    let handshake_response = Err(ConnectionError::Socket(ErrorKind::ConnectionReset.into()));

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::ClientHandshake {
        result: handshake_response,
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::FailConnection(
        host,
        ConnectionError::Socket(ErrorKind::ConnectionReset.into()),
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_connection_failed_with_remaining() {
    let sa = sock_addr2();
    let host = HostAndPort::new("my_host".to_string(), 80);

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::FailedConnection {
        error: ConnectionError::Socket(ErrorKind::ConnectionReset),
        remaining: vec![sa].into_iter(),
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::DeferConnect(host, sa, vec![])]);
    assert!(result.is_ok());
}

#[test]
fn transition_deferred_connection_failed_no_remaining() {
    let host = HostAndPort::new("my_host".to_string(), 80);

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::Deferred(DeferredResult::FailedConnection {
        error: ConnectionError::Socket(ErrorKind::ConnectionReset),
        remaining: vec![].into_iter(),
        host: host.clone(),
    });
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::FailConnection(
        host,
        ConnectionError::Socket(ErrorKind::ConnectionReset),
    )]);
    assert!(result.is_ok());
}

#[test]
fn transition_task_closed() {
    let addr = RoutingAddr::remote(10);

    let mut state = FakeRemoteState::default();
    let mut result = Ok(());

    let event = Event::ConnectionClosed(addr, ConnectionDropped::Closed);
    super::update_state(&mut state, &mut result, event);

    state.check(vec![StateMutation::TableRemove(addr)]);
    assert!(result.is_ok());
}
