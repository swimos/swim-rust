// Copyright 2015-2021 SWIM.AI inc.
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

use std::collections::HashMap;
use std::ops::Deref;

use crate::configuration::router::RouterParams;
use crate::connections::{ConnectionPool, ConnectionRequest, ConnectionSender};
use either::Either;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::join;
use futures::select;
use futures::stream::FuturesUnordered;
use futures::{select_biased, Future, FutureExt, SinkExt, StreamExt};
use std::collections::hash_map::Entry;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use swim_common::request::request_future::RequestError;
use swim_common::request::Request;
use swim_common::routing::error::{ResolutionError, RouterError, RoutingError, Unresolvable};
use swim_common::routing::remote::config::ConnectionConfig;
use swim_common::routing::remote::net::dns::Resolver;
use swim_common::routing::remote::net::plain::TokioPlainTextNetworking;
use swim_common::routing::remote::{
    RawRoute, RemoteConnectionChannels, RemoteConnectionsTask, RoutingRequest, SchemeSocketAddr,
};
use swim_common::routing::ws::tungstenite::TungsteniteWsConnections;
use swim_common::routing::ConnectionDropped;
use swim_common::routing::{
    Route, Router, RouterFactory, RoutingAddr, TaggedEnvelope, TaggedSender,
};
use swim_common::warp::envelope::{Envelope, IncomingHeader, IncomingLinkMessage, LinkMessage};
use swim_common::warp::path::{AbsolutePath, RelativePath};
use swim_runtime::task::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::trace_span;
use tracing::{debug, error, span, trace, warn, Level};
use tracing_futures::Instrument;
use url::Url;
use utilities::errors::Recoverable;
use utilities::future::open_ended::OpenEndedFutures;
use utilities::sync::{promise, trigger};
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub(crate) struct ClientRouterFactory {
    request_sender: mpsc::Sender<ClientRequest>,
}

impl ClientRouterFactory {
    pub(crate) fn new(request_sender: mpsc::Sender<ClientRequest>) -> Self {
        ClientRouterFactory { request_sender }
    }
}

impl RouterFactory for ClientRouterFactory {
    type Router = ClientRouter;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        ClientRouter {
            tag: addr,
            request_sender: self.request_sender.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClientRouter {
    tag: RoutingAddr,
    request_sender: mpsc::Sender<ClientRequest>,
}

impl Router for ClientRouter {
    fn resolve_sender(
        &mut self,
        _addr: RoutingAddr,
        origin: Option<SchemeSocketAddr>,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        async move {
            let ClientRouter {
                tag,
                request_sender,
            } = self;
            let (tx, rx) = oneshot::channel();
            if request_sender
                .send(ClientRequest::Connect {
                    request: Request::new(tx),
                    origin: origin.unwrap(),
                })
                .await
                .is_err()
            {
                Err(ResolutionError::router_dropped())
            } else {
                match rx.await {
                    Ok(Ok(RawRoute { sender, on_drop })) => {
                        Ok(Route::new(TaggedSender::new(*tag, sender), on_drop))
                    }
                    Ok(Err(err)) => Err(ResolutionError::unresolvable(err.to_string())),
                    Err(_) => Err(ResolutionError::router_dropped()),
                }
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        _host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move { Ok(RoutingAddr::local(0)) }.boxed()
    }
}

#[derive(Debug)]
pub(crate) enum ClientRequest {
    /// Obtain a connection.
    Connect {
        request: Request<Result<RawRoute, Unresolvable>>,
        origin: SchemeSocketAddr,
    },
    /// Subscribe to a connection.
    Subscribe {
        path: AbsolutePath,
        request: Request<mpsc::Receiver<TaggedEnvelope>>,
    },
}

pub(crate) type OutgoingManagerSender = mpsc::Sender<TaggedEnvelope>;

//Todo dm rename the channels to something that makes sense
pub(crate) async fn run_client_router(
    mut request_rx: mpsc::Receiver<ClientRequest>,
    mut conn_req_rx: mpsc::Receiver<ConnectionRequest>,
) {
    let mut outgoing_managers: HashMap<
        String,
        (
            OutgoingManagerSender,
            mpsc::Sender<(RelativePath, Request<mpsc::Receiver<TaggedEnvelope>>)>,
        ),
    > = HashMap::new();

    let mut request_rx = ReceiverStream::new(request_rx).fuse();
    let mut local_rx = ReceiverStream::new(local_rx).fuse();
    let futures = FuturesUnordered::new();
    let mut close_txs = Vec::new();

    loop {
        let next: Option<ClientRequest> = select_biased! {
            request = request_rx.next() => request,
            sub = local_rx.next() => sub,
        };

        match next {
            Some(ClientRequest::Connect { request, origin }) => {
                eprintln!("addr = {:#?}", origin.to_string());
                let (sender, _) = outgoing_managers
                    .entry(origin.to_string())
                    .or_insert_with(|| {
                        let (manager, sender, sub_tx) = IncomingManager::new();

                        let handle = spawn(manager.run());
                        futures.push(handle);
                        (sender, sub_tx)
                    })
                    .clone();

                let (on_drop_tx, on_drop_rx) = promise::promise();
                close_txs.push(on_drop_tx);

                request.send(Ok(RawRoute::new(sender, on_drop_rx))).unwrap();
            }
            Some(ClientRequest::Subscribe {
                path: sub_addr,
                request: sub_req,
            }) => {
                eprintln!("host = {:#?}", sub_addr.host.to_string());
                let (_, sub_sender) = outgoing_managers
                    .entry(sub_addr.host.to_string())
                    .or_insert_with(|| {
                        let (manager, sender, sub_tx) = IncomingManager::new();

                        let handle = spawn(manager.run());
                        futures.push(handle);
                        (sender, sub_tx)
                    });

                sub_sender
                    .send((sub_addr.relative_path(), sub_req))
                    .await
                    .unwrap();
            }
            _ => {
                close_txs.into_iter().for_each(|trigger| {
                    if let Err(err) = trigger.provide(ConnectionDropped::Closed) {
                        tracing::error!("{:?}", err);
                    }
                });

                for result in futures.collect::<Vec<_>>().await {
                    match result {
                        Ok(res) => {
                            if let Err(err) = res {
                                tracing::error!("{:?}", err);
                            }
                        }
                        Err(err) => {
                            tracing::error!("{:?}", err);
                        }
                    }
                }

                break;
            }
        }
    }
}

pub(crate) struct IncomingManager {
    envelope_rx: mpsc::Receiver<TaggedEnvelope>,
    sub_rx: mpsc::Receiver<(RelativePath, Request<mpsc::Receiver<TaggedEnvelope>>)>,
}

impl IncomingManager {
    pub(crate) fn new() -> (
        IncomingManager,
        OutgoingManagerSender,
        mpsc::Sender<(RelativePath, Request<mpsc::Receiver<TaggedEnvelope>>)>,
    ) {
        let (envelope_tx, envelope_rx) = mpsc::channel(8);
        let (sub_tx, sub_rx) = mpsc::channel(8);
        (
            IncomingManager {
                envelope_rx,
                sub_rx,
            },
            envelope_tx,
            sub_tx,
        )
    }

    pub(crate) async fn run(mut self) -> Result<(), RoutingError> {
        let IncomingManager {
            mut envelope_rx,
            mut sub_rx,
        } = self;

        let mut envelope_rx = ReceiverStream::new(envelope_rx).fuse();
        let mut sub_rx = ReceiverStream::new(sub_rx).fuse();

        let mut subs: HashMap<RelativePath, Vec<mpsc::Sender<TaggedEnvelope>>> = HashMap::new();

        loop {
            let next: Either<
                Option<TaggedEnvelope>,
                Option<(RelativePath, Request<mpsc::Receiver<TaggedEnvelope>>)>,
            > = select_biased! {
                envelope = envelope_rx.next() => Either::Left(envelope),
                sub = sub_rx.next() => Either::Right(sub),
            };

            match next {
                Either::Left(Some(envelope)) => match envelope.1.clone().into_incoming() {
                    Ok(env) => {
                        broadcast_destination(&mut subs, env.path, envelope).await?;
                    }
                    Err(env) => {
                        warn!("Unsupported message: {:?}", env);
                    }
                },
                Either::Right(Some((rel_path, sub_request))) => {
                    let (sub_tx, sub_rx) = mpsc::channel(8);

                    match subs.entry(rel_path) {
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().push(sub_tx);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(vec![sub_tx]);
                        }
                    }
                    sub_request.send(sub_rx).unwrap();
                }
                _ => break Ok(()),
            }
        }
    }
}

/// Broadcasts an event to all subscribers of the task that are subscribed to a given path.
/// The path is the combination of the node and lane.
///
/// # Arguments
///
/// * `subscribers`             - A map of all subscribers.
/// * `destination`             - The node and lane.
/// * `event`                   - An event to be broadcasted.
async fn broadcast_destination(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<TaggedEnvelope>>>,
    destination: RelativePath,
    envelope: TaggedEnvelope,
) -> Result<(), RoutingError> {
    if subscribers.contains_key(&destination) {
        let destination_subs = subscribers
            .get_mut(&destination)
            .ok_or(RoutingError::ConnectionError)?;

        if destination_subs.len() == 1 {
            let result = destination_subs
                .get_mut(0)
                .ok_or(RoutingError::ConnectionError)?
                .send(envelope)
                .await;

            if result.is_err() {
                destination_subs.remove(0);
            }
        } else {
            let futures: FuturesUnordered<_> = destination_subs
                .iter_mut()
                .enumerate()
                .map(|(index, sender)| index_sender(sender, envelope.clone(), index))
                .collect();

            for index in futures.filter_map(ready).collect::<Vec<_>>().await {
                destination_subs.remove(index);
            }
        }
    } else {
        trace!("No downlink interested in event: {:?}", envelope);
    };

    Ok(())
}

async fn index_sender(
    sender: &mut mpsc::Sender<TaggedEnvelope>,
    envelope: TaggedEnvelope,
    index: usize,
) -> Option<usize> {
    if sender.send(envelope).await.is_err() {
        Some(index)
    } else {
        None
    }
}
