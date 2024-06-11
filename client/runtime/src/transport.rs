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

use crate::error::{DownlinkErrorKind, DownlinkRuntimeError};
use crate::models::IdIssuer;
use fnv::FnvHashMap;
use futures::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use ratchet::{ExtensionProvider, SplittableExtension, WebSocket, WebSocketStream};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use swimos_messages::remote_protocol::AttachClient;
use swimos_remote::net::{ClientConnections, Scheme, SchemeHostPort};
use swimos_remote::websocket::WebsocketClient;
use swimos_remote::RemoteTask;
use swimos_utilities::trigger;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tracing::{debug, error};

type AttachCallback =
    oneshot::Sender<Result<(SocketAddr, mpsc::Sender<AttachClient>), DownlinkRuntimeError>>;

pub enum TransportRequest {
    Resolve(SchemeHostPort, oneshot::Sender<io::Result<Vec<SocketAddr>>>),
    ConnectionFor {
        host: String,
        scheme: Scheme,
        addrs: Vec<SocketAddr>,
        callback: AttachCallback,
    },
}

enum TransportEvent<Sock, Ext> {
    Request(TransportRequest),
    Open {
        host: String,
        callback: AttachCallback,
        socket: Sock,
        addr: SocketAddr,
    },
    HandshakeComplete {
        addr: SocketAddr,
        host: String,
        callback: AttachCallback,
        websocket: WebSocket<Sock, Ext>,
    },
    PeerStopped {
        addr: SocketAddr,
        host: String,
        result: Result<(), JoinError>,
    },
}

#[derive(Debug, Clone)]
pub struct TransportHandle {
    tx: mpsc::Sender<TransportRequest>,
}

impl TransportHandle {
    pub fn new(tx: mpsc::Sender<TransportRequest>) -> TransportHandle {
        TransportHandle { tx }
    }

    async fn exec<F, O, E>(&self, fun: F) -> Result<O, DownlinkRuntimeError>
    where
        F: FnOnce(oneshot::Sender<Result<O, E>>) -> TransportRequest,
        E: Into<DownlinkRuntimeError>,
    {
        let (tx, rx) = oneshot::channel();
        match self.tx.send(fun(tx)).await {
            Ok(()) => match rx.await {
                Ok(Ok(o)) => Ok(o),
                Ok(Err(e)) => Err(e.into()),
                Err(_) => Err(DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)),
            },
            Err(_) => Err(DownlinkRuntimeError::new(DownlinkErrorKind::Terminated)),
        }
    }

    pub async fn resolve(
        &self,
        shp: SchemeHostPort,
    ) -> Result<Vec<SocketAddr>, DownlinkRuntimeError> {
        impl From<io::Error> for DownlinkRuntimeError {
            fn from(e: io::Error) -> Self {
                DownlinkRuntimeError::with_cause(DownlinkErrorKind::Unresolvable, e)
            }
        }

        self.exec(|tx| TransportRequest::Resolve(shp, tx)).await
    }

    pub async fn connection_for(
        &self,
        scheme: Scheme,
        host: String,
        addrs: Vec<SocketAddr>,
    ) -> Result<(SocketAddr, mpsc::Sender<AttachClient>), DownlinkRuntimeError> {
        self.exec(|callback| TransportRequest::ConnectionFor {
            host,
            scheme,
            addrs,
            callback,
        })
        .await
    }
}

pub struct Transport<Net, Ws, Provider> {
    networking: Net,
    websockets: Ws,
    ext_provider: Provider,
    buffer_size: NonZeroUsize,
    close_timeout: Duration,
}

impl<Net, Ws, Provider> Transport<Net, Ws, Provider>
where
    Net: ClientConnections,
    Net::ClientSocket: WebSocketStream,
    Ws: WebsocketClient + Sync,
    Provider: ExtensionProvider + Send + Sync + 'static,
    Provider::Extension: SplittableExtension + Send + Sync,
{
    pub fn new(
        networking: Net,
        websockets: Ws,
        ext_provider: Provider,
        buffer_size: NonZeroUsize,
        close_timeout: Duration,
    ) -> Transport<Net, Ws, Provider> {
        Transport {
            networking,
            websockets,
            ext_provider,
            buffer_size,
            close_timeout,
        }
    }

    pub async fn run(self, mut requests: mpsc::Receiver<TransportRequest>) {
        let Transport {
            networking,
            websockets,
            ext_provider,
            buffer_size,
            close_timeout,
        } = self;

        let (stop_tx, stop_rx) = trigger::trigger();
        let mut peers: FnvHashMap<SocketAddr, mpsc::Sender<AttachClient>> = FnvHashMap::default();
        let mut events: FuturesUnordered<BoxFuture<Option<_>>> = FuturesUnordered::default();
        let mut remote_issuer = IdIssuer::new();

        debug!("Transport task started");

        loop {
            let event: TransportEvent<Net::ClientSocket, Provider::Extension> = select! {
                biased;
                // Bias towards encapsulated events in case there is a closing connection.
                Some(Some(event)) = events.next(), if !events.is_empty() => event,
                request = requests.recv() => {
                    match request {
                        Some(request) => TransportEvent::Request(request),
                        None => {
                            stop_tx.trigger();
                            break
                        },
                    }
                }
            };

            match event {
                TransportEvent::Request(TransportRequest::Resolve(shp, callback)) => {
                    let shared_networking = &networking;
                    let resolve_fut = async move {
                        let result = shared_networking
                            .lookup(shp.host().clone(), shp.port())
                            .await;
                        let _r = callback.send(result);
                        None
                    };
                    events.push(resolve_fut.boxed());
                }
                TransportEvent::Request(TransportRequest::ConnectionFor {
                    host,
                    scheme,
                    addrs,
                    callback,
                }) => {
                    let peer = addrs
                        .iter()
                        .find_map(|sock| peers.get(sock).map(|tx| (sock, tx)));
                    match peer {
                        Some((sock, peer)) => {
                            let _r = callback.send(Ok((*sock, peer.clone())));
                        }
                        None => {
                            let shared_networking = &networking;
                            events.push(
                                async move {
                                    for addr in addrs {
                                        if let Ok(socket) = shared_networking
                                            .try_open(scheme, Some(host.as_str()), addr)
                                            .await
                                        {
                                            return Some(TransportEvent::Open {
                                                host,
                                                callback,
                                                socket,
                                                addr,
                                            });
                                        }
                                    }

                                    let _r = callback.send(Err(DownlinkRuntimeError::new(
                                        DownlinkErrorKind::Unresolvable,
                                    )));
                                    None
                                }
                                .boxed(),
                            );
                        }
                    }
                }
                TransportEvent::Open {
                    callback,
                    host,
                    socket,
                    addr,
                } => {
                    let shared_ws = &websockets;
                    let provider = &ext_provider;
                    let handshake_fut = async move {
                        match shared_ws
                            .open_connection(socket, provider, host.clone())
                            .await
                        {
                            Ok(websocket) => Some(TransportEvent::HandshakeComplete {
                                addr,
                                host,
                                callback,
                                websocket,
                            }),
                            Err(e) => {
                                let _r = callback.send(Err(DownlinkRuntimeError::with_cause(
                                    DownlinkErrorKind::WebsocketNegotiationFailed,
                                    e,
                                )));
                                None
                            }
                        }
                    };
                    events.push(handshake_fut.boxed())
                }
                TransportEvent::HandshakeComplete {
                    addr,
                    host,
                    callback,
                    websocket,
                } => {
                    let (attach_tx, attach_rx) = mpsc::channel(buffer_size.get());
                    let remote = RemoteTask::new(
                        remote_issuer.next_id(),
                        stop_rx.clone(),
                        websocket,
                        attach_rx,
                        None,
                        buffer_size,
                        close_timeout,
                    );
                    events.push(
                        async move {
                            Some(TransportEvent::PeerStopped {
                                addr,
                                result: tokio::spawn(remote.run()).await,
                                host,
                            })
                        }
                        .boxed(),
                    );
                    peers.insert(addr, attach_tx.clone());
                    let _r = callback.send(Ok((addr, attach_tx)));
                }
                TransportEvent::PeerStopped { addr, host, result } => {
                    // We don't need to propagate the closure of the peer as any runtime and
                    // downlink tasks will be immediately notified due to their streams closing.
                    // Following this, the peer will be removed from the collection in the IO task.
                    if let Err(ref e) = result {
                        error!(
                            host = %host,
                            error = ?e,
                            "Connection task failure"
                        );
                    }
                    debug_assert!(
                        peers.remove(&addr).is_some(),
                        "Attempted to remove a peer that didn't exist"
                    );
                }
            }
        }

        debug!("Transport task completed");
    }
}
