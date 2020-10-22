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
use crate::routing::remote::addresses::RemoteRoutingAddresses;
use crate::routing::remote::config::ConnectionConfig;
use crate::routing::remote::net::{ExternalConnections, Listener};
use crate::routing::remote::pending::PendingRequests;
use crate::routing::remote::table::{HostAndPort, RoutingTable};
use crate::routing::remote::task::TaskFactory;
use crate::routing::remote::{ConnectionDropped, ResolutionRequest, RoutingRequest, SocketAddrIt};
use crate::routing::ws::WsConnections;
use crate::routing::{RoutingAddr, ServerRouterFactory};
use futures::future::{BoxFuture, Fuse};
use futures::StreamExt;
use futures::{select_biased, FutureExt};
use futures_util::stream::TakeUntil;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::trigger;
use utilities::task::Spawner;

pub struct RemoteConnections<'a, External, Ws, Sp, RouterFac>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket>,
{
    websockets: &'a Ws,
    spawner: Sp,
    listener: <External::ListenerType as Listener>::AcceptStream,
    external: External,
    requests: TakeUntil<mpsc::Receiver<RoutingRequest>, trigger::Receiver>,
    table: RoutingTable,
    pending: PendingRequests,
    addresses: RemoteRoutingAddresses,
    tasks: TaskFactory<RouterFac>,
    deferred: OpenEndedFutures<BoxFuture<'a, DeferredResult<Ws::StreamSink>>>,
    state: State,
    external_stop: Fuse<trigger::Receiver>,
    internal_stop: Option<trigger::Sender>,
}

impl<'a, External, Ws, Sp, RouterFac> RemoteConnections<'a, External, Ws, Sp, RouterFac>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket> + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Unpin,
    RouterFac: ServerRouterFactory + 'static,
{
    pub fn new(
        websockets: &'a Ws,
        configuration: ConnectionConfig,
        spawner: Sp,
        external: External,
        listener: External::ListenerType,
        stop_trigger: trigger::Receiver,
        delegate_router: RouterFac,
    ) -> Self {
        let (stop_tx, stop_rx) = trigger::trigger();
        let (req_tx, req_rx) = mpsc::channel(configuration.router_buffer_size.get());
        let tasks = TaskFactory::new(req_tx, stop_rx.clone(), configuration, delegate_router);
        RemoteConnections {
            websockets,
            listener: listener.into_stream(),
            external,
            spawner,
            requests: req_rx.take_until(stop_rx),
            table: RoutingTable::default(),
            pending: PendingRequests::default(),
            addresses: RemoteRoutingAddresses::default(),
            tasks,
            deferred: OpenEndedFutures::new(),
            state: State::Running,
            external_stop: stop_trigger.fuse(),
            internal_stop: Some(stop_tx),
        }
    }

    pub async fn select_next(&mut self) -> Option<Event<External::Socket, Ws::StreamSink>> {
        let RemoteConnections {
            spawner,
            listener,
            requests,
            deferred,
            state,
            ref mut external_stop,
            internal_stop,
            ..
        } = self;
        let mut external_stop = external_stop;
        loop {
            match state {
                State::Running => {
                    let result = select_biased! {
                        incoming = listener.next() => incoming.map(Event::Incoming),
                        request = requests.next() => request.map(Event::Request),
                        def_complete = deferred.next() => def_complete.map(Event::Deferred),
                        result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
                        _ = &mut external_stop => {
                            if let Some(stop_tx) = internal_stop.take() {
                                stop_tx.trigger();
                            }
                            None
                        }
                    };
                    if result.is_none() {
                        spawner.stop();
                        *state = State::ClosingConnections;
                    } else {
                        return result;
                    }
                }
                State::ClosingConnections => {
                    let result = select_biased! {
                        def_complete = deferred.next() => def_complete.map(Event::Deferred),
                        result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
                    };
                    if result.is_none() {
                        OpenEndedFutures::stop(deferred);
                        *state = State::ClearingDeferred;
                    } else {
                        return result;
                    }
                }
                State::ClearingDeferred => {
                    return deferred.next().await.map(Event::Deferred);
                }
            }
        }
    }

    pub fn stop(&mut self) {
        let RemoteConnections {
            spawner,
            state,
            internal_stop,
            ..
        } = self;
        if matches!(*state, State::Running) {
            if let Some(stop_tx) = internal_stop.take() {
                stop_tx.trigger();
            }
            spawner.stop();
            *state = State::ClosingConnections;
        }
    }

    pub fn defer<F>(&self, fut: F)
    where
        F: Future<Output = DeferredResult<Ws::StreamSink>> + Send + 'a,
    {
        self.deferred.push(fut.boxed());
    }

    pub fn next_address(&mut self) -> RoutingAddr {
        self.addresses.next().expect("Address counter overflow.")
    }

    pub fn spawn_task(
        &mut self,
        sock_addr: SocketAddr,
        ws_stream: Ws::StreamSink,
        host: Option<&HostAndPort>,
    ) {
        let addr = self.next_address();
        let RemoteConnections {
            tasks,
            spawner,
            table,
            pending,
            ..
        } = self;
        let msg_tx = tasks.spawn_connection_task(ws_stream, addr, spawner);
        table.insert(addr, None, sock_addr, msg_tx);
        if let Some(host) = host {
            pending.send_ok(host, addr);
        }
    }

    pub fn check_socket_addr(
        &mut self,
        host: HostAndPort,
        sock_addr: SocketAddr,
    ) -> Result<(), HostAndPort> {
        let RemoteConnections { table, pending, .. } = self;
        if let Some(addr) = table.get_resolved(&sock_addr) {
            pending.send_ok(&host, addr);
            table.add_host(host, sock_addr);
            Ok(())
        } else {
            Err(host)
        }
    }

    pub fn defer_handshake(&self, stream: External::Socket, peer_addr: SocketAddr) {
        let websockets = self.websockets;
        self.defer(async move {
            let result = do_handshake(true, stream, websockets).await;
            DeferredResult::incoming_handshake(result, peer_addr)
        });
    }

    pub fn defer_connect_and_handshake(
        &mut self,
        host: HostAndPort,
        sock_addr: SocketAddr,
        remaining: SocketAddrIt,
    ) {
        let websockets = self.websockets;
        if let Err(host) = self.check_socket_addr(host, sock_addr) {
            let external = self.external.clone();
            self.defer(async move {
                connect_and_handshake(external, sock_addr, remaining, host, websockets).await
            });
        }
    }

    pub fn defer_dns_lookup(&mut self, target: HostAndPort, request: ResolutionRequest) {
        let target_cpy = target.clone();
        let external = self.external.clone();
        self.defer(async move {
            let resolved = external
                .lookup(target_cpy.to_string())
                .await
                .map(|v| v.into_iter());
            DeferredResult::dns(resolved, target_cpy)
        });
        self.pending.add(target, request);
    }

    pub fn fail_connection(&mut self, host: &HostAndPort, error: ConnectionError) {
        self.pending.send_err(host, error);
    }

    pub fn table(&self) -> &RoutingTable {
        &self.table
    }

    pub fn table_mut(&mut self) -> &mut RoutingTable {
        &mut self.table
    }
}

pub enum DeferredResult<Snk> {
    ServerHandshake {
        result: Result<Snk, ConnectionError>,
        sock_addr: SocketAddr,
    },
    ClientHandshake {
        result: Result<(Snk, SocketAddr), ConnectionError>,
        host: HostAndPort,
    },
    FailedConnection {
        error: ConnectionError,
        remaining: SocketAddrIt,
        host: HostAndPort,
    },
    Dns {
        result: io::Result<SocketAddrIt>,
        host: HostAndPort,
    },
}

impl<Snk> DeferredResult<Snk> {
    fn incoming_handshake(result: Result<Snk, ConnectionError>, sock_addr: SocketAddr) -> Self {
        DeferredResult::ServerHandshake { result, sock_addr }
    }

    fn outgoing_handshake(
        result: Result<(Snk, SocketAddr), ConnectionError>,
        host: HostAndPort,
    ) -> Self {
        DeferredResult::ClientHandshake { result, host }
    }

    fn dns(result: io::Result<SocketAddrIt>, host: HostAndPort) -> Self {
        DeferredResult::Dns { result, host }
    }

    fn failed_connection(
        error: ConnectionError,
        remaining: std::vec::IntoIter<SocketAddr>,
        host: HostAndPort,
    ) -> Self {
        DeferredResult::FailedConnection {
            error,
            remaining,
            host,
        }
    }
}

enum State {
    Running,
    ClosingConnections,
    ClearingDeferred,
}

pub enum Event<Socket, Snk> {
    Incoming(io::Result<(Socket, SocketAddr)>),
    Request(RoutingRequest),
    Deferred(DeferredResult<Snk>),
    ConnectionClosed(RoutingAddr, ConnectionDropped),
}

async fn do_handshake<Socket, Ws>(
    server: bool,
    socket: Socket,
    websockets: &Ws,
) -> Result<Ws::StreamSink, ConnectionError>
where
    Socket: AsyncRead + AsyncWrite + Unpin,
    Ws: WsConnections<Socket>,
{
    if server {
        websockets.accept_connection(socket).await
    } else {
        websockets.open_connection(socket).await
    }
}

async fn connect_and_handshake<External: ExternalConnections, Ws>(
    external: External,
    sock_addr: SocketAddr,
    remaining: SocketAddrIt,
    host: HostAndPort,
    websockets: &Ws,
) -> DeferredResult<Ws::StreamSink>
where
    Ws: WsConnections<External::Socket>,
{
    match connect_and_handshake_single(external, sock_addr, websockets).await {
        Ok(str) => DeferredResult::outgoing_handshake(Ok((str, sock_addr)), host),
        Err(err) => DeferredResult::failed_connection(err, remaining, host),
    }
}

async fn connect_and_handshake_single<External: ExternalConnections, Ws>(
    external: External,
    addr: SocketAddr,
    websockets: &Ws,
) -> Result<Ws::StreamSink, ConnectionError>
where
    Ws: WsConnections<External::Socket>,
{
    websockets
        .open_connection(external.try_open(addr).await?)
        .await
}
