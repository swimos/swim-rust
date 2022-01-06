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

use crate::error::{ConnectionDropped, ConnectionError};
use crate::remote::addresses::RemoteRoutingAddresses;
use crate::remote::config::RemoteConnectionsConfig;
use crate::remote::pending::PendingRequest;
use crate::remote::pending::PendingRequests;
use crate::remote::router::{RemoteRoutingRequest, Router};
use crate::remote::table::{BidirectionalRegistrator, RoutingTable, SchemeHostPort};
use crate::remote::task::TaskFactory;
use crate::remote::{ExternalConnections, Listener, SchemeSocketAddr};
use crate::remote::{RawRoute, RemoteConnectionChannels, SchemeSocketAddrIt};
use crate::routing::CloseReceiver;
use crate::routing::RoutingAddr;
use crate::ws::WsConnections;
use futures::future::{BoxFuture, Fuse};
use futures::stream::TakeUntil;
use futures::StreamExt;
use futures::{select_biased, FutureExt};
use ratchet::{WebSocket, WebSocketStream};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use swim_model::path::Addressable;
use swim_utilities::future::open_ended::OpenEndedFutures;
use swim_utilities::future::task::Spawner;
use swim_utilities::trigger;
use swim_utilities::trigger::promise::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;

#[cfg(test)]
mod tests;

type DeferredConnections<'l, Sock, Ext> =
    OpenEndedFutures<BoxFuture<'l, DeferredResult<WebSocket<Sock, Ext>>>>;

/// Trait detailing the operations permissible on the state of the remote connections management
/// task. This is to allow the state to be decoupled from the state transition function so
/// the two can be tested separately.
pub trait RemoteTasksState {
    type Socket;
    type WebSocket;

    /// Explicitly move into the stopping state.
    fn stop(&mut self);

    /// Spawn a new connection task, attached to the provided web socket.
    fn spawn_task(
        &mut self,
        sock_addr: SchemeSocketAddr,
        ws_stream: Self::WebSocket,
        host: Option<SchemeHostPort>,
    );

    /// Check a pair of host/socket address, registering the hose with the address if a connection
    /// is already open to it and fulfilling any requests for that host.
    fn check_socket_addr(
        &mut self,
        host: SchemeHostPort,
        sock_addr: SchemeSocketAddr,
    ) -> Result<(), SchemeHostPort>;

    /// Add a deferred web socket handshake.
    fn defer_handshake(&self, stream: Self::Socket, peer_addr: SchemeSocketAddr);

    /// Add a deferred new connection followed by a websocket handshake.
    fn defer_connect_and_handshake(
        &mut self,
        host: SchemeHostPort,
        sock_addr: SchemeSocketAddr,
        remaining: SchemeSocketAddrIt,
    );

    /// Add a deferred DNS lookup for a host.
    fn defer_dns_lookup(&mut self, target: SchemeHostPort, resolution_request: PendingRequest);

    /// Flush out pending state for a failed connection.
    fn fail_connection(&mut self, host: &SchemeHostPort, error: ConnectionError);

    /// Resolve an entry in the routing table.
    fn table_resolve(&self, addr: RoutingAddr) -> Option<RawRoute>;

    /// Resolve a bidirectional route in the routing table.
    fn table_resolve_bidirectional(&self, addr: RoutingAddr) -> Option<BidirectionalRegistrator>;

    /// Try to resolve a host in the routing table.
    fn table_try_resolve(&self, target: &SchemeHostPort) -> Option<RoutingAddr>;

    /// Remote an entry from the routing table return the promise to use to indicate why the entry
    /// was removed.
    fn table_remove(&mut self, addr: RoutingAddr) -> Option<promise::Sender<ConnectionDropped>>;
}

/// The canonical implementation of [`RemoteTasksState`]. This is, in effect, a stream of events
/// where the next event is a function of the current state. It does not implement the [`Stream`]
/// trait to avoid boxing the future crated by the `select_next` function.
///
/// # Type Parameters
///
/// * `External` - Provides the ability to open sockets.
/// * `Ws` - Negotiates a web socket connection on top of the sockets provided by `External`.
/// * `Sp` - Spawner to run the tasks that manage the connections opened by this state machine.
/// * `Routerfac` - Creates router instances to be provided to the connection management tasks.
pub struct RemoteConnections<'a, External, Ws, Sp, Path>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket>,
{
    websockets: &'a Ws,
    spawner: Sp,
    listener: Option<<External::ListenerType as Listener>::AcceptStream>,
    external: External,
    requests: TakeUntil<ReceiverStream<RemoteRoutingRequest>, trigger::Receiver>,
    table: RoutingTable,
    pending: PendingRequests,
    addresses: RemoteRoutingAddresses,
    tasks: TaskFactory<Path>,
    deferred: DeferredConnections<'a, External::Socket, Ws::Ext>,
    state: State,
    external_stop: Fuse<CloseReceiver>,
    internal_stop: Option<trigger::Sender>,
}

impl<'a, External, Ws, Sp, Path> RemoteTasksState for RemoteConnections<'a, External, Ws, Sp, Path>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket> + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Unpin,
    Path: Addressable,
    External::Socket: WebSocketStream,
{
    type Socket = External::Socket;
    type WebSocket = WebSocket<Self::Socket, Ws::Ext>;

    fn stop(&mut self) {
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

    fn spawn_task(
        &mut self,
        sock_addr: SchemeSocketAddr,
        ws_stream: WebSocket<External::Socket, Ws::Ext>,
        host: Option<SchemeHostPort>,
    ) {
        let addr = self.next_address();
        let RemoteConnections {
            tasks,
            spawner,
            table,
            pending,
            ..
        } = self;

        let (msg_tx, bidirectional_request_tx) =
            tasks.spawn_connection_task(ws_stream, addr, spawner);

        let bidirectional_registrator = table.insert(
            addr,
            host.clone(),
            sock_addr,
            msg_tx,
            bidirectional_request_tx,
        );

        if let Some(host) = host {
            pending.send_ok(&host, addr, bidirectional_registrator);
        }
    }

    fn check_socket_addr(
        &mut self,
        host: SchemeHostPort,
        sock_addr: SchemeSocketAddr,
    ) -> Result<(), SchemeHostPort> {
        let RemoteConnections { table, pending, .. } = self;

        if let Some(addr) = table.get_resolved(&sock_addr) {
            if let Some(bidirectional_registrator) = table.resolve_bidirectional(addr) {
                pending.send_ok(&host, addr, bidirectional_registrator);
                table.add_host(host, sock_addr);
                Ok(())
            } else {
                Err(host)
            }
        } else {
            Err(host)
        }
    }

    fn defer_handshake(&self, stream: External::Socket, peer_addr: SchemeSocketAddr) {
        let websockets = self.websockets;
        self.defer(async move {
            let result = do_handshake(true, stream, websockets, peer_addr).await;
            DeferredResult::incoming_handshake(result, peer_addr)
        });
    }

    fn defer_connect_and_handshake(
        &mut self,
        host: SchemeHostPort,
        sock_addr: SchemeSocketAddr,
        remaining: SchemeSocketAddrIt,
    ) {
        let websockets = self.websockets;
        let external = self.external.clone();
        self.defer(async move {
            connect_and_handshake(external, sock_addr, remaining, host, websockets).await
        });
    }

    fn defer_dns_lookup(&mut self, target: SchemeHostPort, resolution_request: PendingRequest) {
        let target_cpy = target.clone();
        let external = self.external.clone();
        self.defer(async move {
            let resolved = external
                .lookup(target_cpy.clone())
                .await
                .map(|v| v.into_iter());
            DeferredResult::dns(resolved, target_cpy)
        });
        self.pending.add(target, resolution_request);
    }

    fn fail_connection(&mut self, host: &SchemeHostPort, error: ConnectionError) {
        self.pending.send_err(host, error);
    }

    fn table_resolve(&self, addr: RoutingAddr) -> Option<RawRoute> {
        self.table.resolve(addr)
    }

    fn table_resolve_bidirectional(&self, addr: RoutingAddr) -> Option<BidirectionalRegistrator> {
        self.table.resolve_bidirectional(addr)
    }

    fn table_try_resolve(&self, target: &SchemeHostPort) -> Option<RoutingAddr> {
        self.table.try_resolve(target)
    }

    fn table_remove(&mut self, addr: RoutingAddr) -> Option<Sender<ConnectionDropped>> {
        self.table.remove(addr)
    }
}

impl<'a, External, Ws, Sp, Path> RemoteConnections<'a, External, Ws, Sp, Path>
where
    External: ExternalConnections,
    Ws: WsConnections<External::Socket> + Send + Sync + 'static,
    Sp: Spawner<BoxFuture<'static, (RoutingAddr, ConnectionDropped)>> + Unpin,
    Path: Addressable,
{
    /// Create a new, empty state.
    ///
    /// # Arguments
    ///
    /// * `webcockets` - Negotiations web socket connections on top of the sockets produced by
    /// `external.
    /// * `configuration` - Configuration parameters for the state machine.
    /// * `spawner` - [`Spawner`] implementation to spawn the tasks that manage the connections.
    /// * `external` - Provider of remote sockets.
    /// * `listener` - Server to listen for incoming connections.
    /// * `stop_trigger`- Trigger to cause the state machine to stop externally.
    /// * `delegate_router` - Router than handles local routing requests.
    /// * `req_channel` - Transmitter and receiver for routing requests.
    pub fn new(
        websockets: &'a Ws,
        configuration: RemoteConnectionsConfig,
        spawner: Sp,
        external: External,
        listener: Option<External::ListenerType>,
        router: Router<Path>,
        channels: RemoteConnectionChannels,
    ) -> Self {
        let RemoteConnectionChannels {
            request_rx,
            stop_trigger,
            ..
        } = channels;

        let (stop_tx, stop_rx) = trigger::trigger();
        let tasks = TaskFactory::new(stop_rx.clone(), configuration, router);
        RemoteConnections {
            websockets,
            listener: listener.map(Listener::into_stream),
            external,
            spawner,
            requests: ReceiverStream::new(request_rx).take_until(stop_rx),
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

    fn next_address(&mut self) -> RoutingAddr {
        self.addresses.next().expect("Address counter overflow.")
    }

    /// Select the next event based on the current state (or none if we have reached the terminal
    /// state).
    pub async fn select_next(
        &mut self,
    ) -> Option<Event<External::Socket, WebSocket<External::Socket, Ws::Ext>>> {
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
                    let result = if listener.is_some() {
                        select_biased! {
                        _ = &mut external_stop => {
                            if let Some(stop_tx) = internal_stop.take() {
                                stop_tx.trigger();
                            }
                            None
                        },
                        incoming = listener.as_mut().unwrap().next() => incoming.map(Event::Incoming),
                        request = requests.next() => request.map(Event::Request),
                        def_complete = deferred.next() => def_complete.map(Event::Deferred),
                        result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
                        }
                    } else {
                        select_biased! {
                        _ = &mut external_stop => {
                            if let Some(stop_tx) = internal_stop.take() {
                                stop_tx.trigger();
                            }
                            None
                        },
                        request = requests.next() => request.map(Event::Request),
                        def_complete = deferred.next() => def_complete.map(Event::Deferred),
                        result = spawner.next() => result.map(|(addr, reason)| Event::ConnectionClosed(addr, reason)),
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

    pub fn defer<F>(&self, fut: F)
    where
        F: Future<Output = DeferredResult<WebSocket<External::Socket, Ws::Ext>>> + Send + 'a,
    {
        self.deferred.push(fut.boxed());
    }
}

/// The connection manager can defer long running tasks to avoid blocking its main event loop. When
/// these tasks complete an instance of this type will occur in the event stream.
#[derive(Debug)]
pub enum DeferredResult<Snk> {
    ServerHandshake {
        result: Result<Snk, ConnectionError>,
        sock_addr: SchemeSocketAddr,
    },
    ClientHandshake {
        result: Result<(Snk, SchemeSocketAddr), ConnectionError>,
        host: SchemeHostPort,
    },
    FailedConnection {
        error: ConnectionError,
        remaining: SchemeSocketAddrIt,
        host: SchemeHostPort,
    },
    Dns {
        result: io::Result<SchemeSocketAddrIt>,
        host: SchemeHostPort,
    },
}

impl<Snk> DeferredResult<Snk> {
    fn incoming_handshake(
        result: Result<Snk, ConnectionError>,
        sock_addr: SchemeSocketAddr,
    ) -> Self {
        DeferredResult::ServerHandshake { result, sock_addr }
    }

    fn outgoing_handshake(
        result: Result<(Snk, SchemeSocketAddr), ConnectionError>,
        host: SchemeHostPort,
    ) -> Self {
        DeferredResult::ClientHandshake { result, host }
    }

    fn dns(result: io::Result<SchemeSocketAddrIt>, host: SchemeHostPort) -> Self {
        DeferredResult::Dns { result, host }
    }

    fn failed_connection(
        error: ConnectionError,
        remaining: std::vec::IntoIter<SchemeSocketAddr>,
        host: SchemeHostPort,
    ) -> Self {
        DeferredResult::FailedConnection {
            error,
            remaining,
            host,
        }
    }
}

/// The current execution state (used to manage clean shutdown).
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// The connection manager is running and all events may occur.
    Running,
    /// The connection manger is closing and only task termination event and deferred results will
    /// be handled.
    ClosingConnections,
    /// All tasks have now terminated and we are waiting for the remaining deferred results to
    /// complete before stopping.
    ClearingDeferred,
}

/// Type of events that can be generated by the connection manager.
#[derive(Debug)]
pub enum Event<Socket, Snk> {
    /// An incoming connection has been opened.
    Incoming(io::Result<(Socket, SchemeSocketAddr)>),
    /// A routing request has been received.
    Request(RemoteRoutingRequest),
    /// A task that the manager deferred has completed.
    Deferred(DeferredResult<Snk>),
    /// A connection task has terminated.
    ConnectionClosed(RoutingAddr, ConnectionDropped),
}

async fn do_handshake<Socket, Ws>(
    server: bool,
    socket: Socket,
    websockets: &Ws,
    peer_addr: SchemeSocketAddr,
) -> Result<WebSocket<Socket, Ws::Ext>, ConnectionError>
where
    Socket: Send + Sync + Unpin + 'static,
    Ws: WsConnections<Socket>,
{
    if server {
        websockets
            .accept_connection(socket)
            .await
            .map_err(Into::into)
    } else {
        websockets
            .open_connection(socket, peer_addr.to_string())
            .await
            .map_err(Into::into)
    }
}

async fn connect_and_handshake<External, Ws>(
    external: External,
    sock_addr: SchemeSocketAddr,
    remaining: SchemeSocketAddrIt,
    scheme_host_port: SchemeHostPort,
    websockets: &Ws,
) -> DeferredResult<WebSocket<External::Socket, Ws::Ext>>
where
    Ws: WsConnections<External::Socket>,
    External: ExternalConnections,
{
    match connect_and_handshake_single(
        external,
        sock_addr.addr,
        websockets,
        scheme_host_port.to_string(),
    )
    .await
    {
        Ok(str) => DeferredResult::outgoing_handshake(Ok((str, sock_addr)), scheme_host_port),
        Err(err) => DeferredResult::failed_connection(err, remaining, scheme_host_port),
    }
}

async fn connect_and_handshake_single<External, Ws>(
    external: External,
    addr: SocketAddr,
    websockets: &Ws,
    host: String,
) -> Result<WebSocket<External::Socket, Ws::Ext>, ConnectionError>
where
    Ws: WsConnections<External::Socket>,
    External: ExternalConnections,
{
    websockets
        .open_connection(external.try_open(addr).await?, host)
        .await
        .map_err(Into::into)
}
