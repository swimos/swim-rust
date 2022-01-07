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

pub mod connections;

use crate::error::{
    ConnectionError, HttpError, HttpErrorKind, ResolutionError, ResolutionErrorKind,
};
use crate::remote::router::{
    DownlinkRoutingRequest, PlaneRoutingRequest, RemoteRoutingRequest, ResolutionErrorReplacement,
    Router, RoutingError,
};
use crate::remote::{ConnectionDropped, RawRoute};
use crate::routing::{RoutingAddr, TaggedEnvelope};
use futures_util::StreamExt;
use http::StatusCode;
use std::collections::{hash_map, HashMap};
use swim_model::path::Path;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

enum Event {
    Plane(PlaneRoutingRequest),
    Remote(RemoteRoutingRequest),
    Client(DownlinkRoutingRequest<Path>),
}

#[derive(Debug)]
pub struct RouteTable {
    owner: RoutingAddr,
    routes: HashMap<RoutingAddr, Entry>,
    uri_mappings: HashMap<RelativeUri, (RoutingAddr, u8)>,
    counter: u32,
}

impl RouteTable {
    pub fn new(owner: RoutingAddr) -> RouteTable {
        RouteTable {
            owner,
            routes: Default::default(),
            uri_mappings: Default::default(),
            counter: 0,
        }
    }

    pub fn addr(&self) -> RoutingAddr {
        self.owner
    }

    pub fn add(&mut self, uri: RelativeUri) -> mpsc::Receiver<TaggedEnvelope> {
        self.add_with_countdown(uri, 0)
    }

    pub fn add_with_countdown(
        &mut self,
        uri: RelativeUri,
        countdown: u8,
    ) -> mpsc::Receiver<TaggedEnvelope> {
        let (tx, rx) = mpsc::channel(8);
        self.add_sender_with_countdown(uri, tx, countdown);
        rx
    }

    fn add_sender_with_countdown(
        &mut self,
        uri: RelativeUri,
        tx: mpsc::Sender<TaggedEnvelope>,
        countdown: u8,
    ) {
        let RouteTable {
            routes,
            uri_mappings,
            counter,
            ..
        } = self;

        let entry = uri_mappings.entry(uri);

        match entry {
            hash_map::Entry::Occupied(_) => {
                panic!("Duplicate registration.")
            }
            hash_map::Entry::Vacant(vacant) => {
                let id = RoutingAddr::plane(*counter);
                *counter += 1;
                vacant.insert((id, countdown));
                let (drop_tx, drop_rx) = promise::promise();
                let route = RawRoute::new(tx, drop_rx);
                routes.insert(
                    id,
                    Entry {
                        route,
                        _on_drop: drop_tx,
                        countdown,
                    },
                );
            }
        }
    }
}

pub struct LocalRoutes {
    routes: HashMap<RoutingAddr, Entry>,
    uri_mappings: HashMap<RelativeUri, (RoutingAddr, u8)>,
    plane_rx: mpsc::Receiver<PlaneRoutingRequest>,
    remote_rx: mpsc::Receiver<RemoteRoutingRequest>,
    client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
}

impl LocalRoutes {
    pub fn from_table(table: RouteTable) -> (Router<Path>, JoinHandle<()>) {
        let RouteTable {
            routes,
            uri_mappings,
            ..
        } = table;

        let (plane_tx, plane_rx) = mpsc::channel(8);
        let (remote_tx, remote_rx) = mpsc::channel(8);
        let (client_tx, client_rx) = mpsc::channel(8);

        let router = Router::server(client_tx, plane_tx, remote_tx);
        let local_routes = LocalRoutes {
            routes,
            plane_rx,
            remote_rx,
            client_rx,
            uri_mappings,
        };

        (router, tokio::spawn(local_routes.run()))
    }

    async fn run(self) {
        let LocalRoutes {
            mut routes,
            mut uri_mappings,
            plane_rx,
            remote_rx,
            client_rx,
        } = self;

        let mut plane_stream = ReceiverStream::new(plane_rx).fuse();
        let mut remote_stream = ReceiverStream::new(remote_rx).fuse();
        let mut client_stream = ReceiverStream::new(client_rx).fuse();

        loop {
            let item: Option<Event> = select! {
                it = plane_stream.next() => it.map(Event::Plane),
                it = remote_stream.next() => it.map(Event::Remote),
                it = client_stream.next() => it.map(Event::Client),
            };
            match item {
                Some(Event::Plane(request)) => {
                    println!("Local routes received plane request: {:?}", request);

                    match request {
                        PlaneRoutingRequest::Agent { .. } => {}
                        PlaneRoutingRequest::Endpoint { addr, request } => {
                            let result = if let Some(Entry {
                                route, countdown, ..
                            }) = routes.get(&addr)
                            {
                                if *countdown == 0 {
                                    Ok(route.clone())
                                } else {
                                    Err(RoutingError::Connection(ConnectionError::Resolution(
                                        ResolutionError::new(
                                            ResolutionErrorKind::Unresolvable,
                                            None,
                                        ),
                                    )))
                                }
                            } else {
                                Err(RoutingError::Connection(ConnectionError::Resolution(
                                    ResolutionError::new(ResolutionErrorKind::Unresolvable, None),
                                )))
                            };

                            let _ = request.send(result);
                        }
                        PlaneRoutingRequest::Resolve {
                            host,
                            route,
                            request,
                        } => {
                            let result = if host.is_some() {
                                Err(RoutingError::Connection(ConnectionError::Resolution(
                                    ResolutionError::new(ResolutionErrorKind::Unresolvable, None),
                                )))
                            } else if let Some((addr, countdown)) = uri_mappings.get_mut(&route) {
                                if *countdown == 0 {
                                    Ok(*addr)
                                } else {
                                    *countdown -= 1;
                                    let addr = *addr;
                                    if let Some(Entry { countdown, .. }) = routes.get_mut(&addr) {
                                        *countdown -= 1;
                                    }
                                    // A non-fatal error that will allow a retry.
                                    Err(RoutingError::Connection(ConnectionError::Http(
                                        HttpError::new(
                                            HttpErrorKind::StatusCode(Some(StatusCode::CONTINUE)),
                                            None,
                                        ),
                                    )))
                                }
                            } else {
                                Err(RoutingError::Resolution(
                                    ResolutionErrorReplacement::NoAgentAtRoute(route),
                                ))
                            };

                            let _ = request.send(result);
                        }
                        PlaneRoutingRequest::Routes(_) => {}
                    }
                }
                Some(Event::Remote(request)) => {
                    println!("Local routes received remote request: {:?}", request);
                }
                Some(Event::Client(request)) => {
                    println!("Local routes received client request: {:?}", request);
                }
                None => break,
            }
        }
    }
}

#[derive(Debug)]
struct Entry {
    route: RawRoute,
    _on_drop: promise::Sender<ConnectionDropped>,
    countdown: u8,
}
