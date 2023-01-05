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
use crate::error::RouterError;
use crate::error::{ConnectionError, HttpError, HttpErrorKind, ResolutionError};
use crate::remote::ConnectionDropped;
use crate::routing::{Route, Router, RouterFactory, RoutingAddr, TaggedEnvelope, TaggedSender};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use http::StatusCode;
use parking_lot::Mutex;
use std::collections::{hash_map, HashMap};
use std::sync::Arc;
use swim_utilities::routing::route_uri::RouteUri;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use url::Url;

#[derive(Debug)]
struct Entry {
    route_sender: TaggedSender,
    on_dropped: promise::Receiver<ConnectionDropped>,
    on_drop: promise::Sender<ConnectionDropped>,
    countdown: u8,
}

#[derive(Debug, Default)]
pub struct LocalRoutesInner {
    routes: HashMap<RoutingAddr, Entry>,
    uri_mappings: HashMap<RouteUri, (RoutingAddr, u8)>,
    counter: u32,
}

#[derive(Debug, Clone)]
pub struct LocalRoutes(RoutingAddr, Arc<Mutex<LocalRoutesInner>>);

impl LocalRoutes {
    pub(crate) fn new(owner_addr: RoutingAddr) -> Self {
        LocalRoutes(owner_addr, Default::default())
    }
}

impl Router for LocalRoutes {
    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route, ResolutionError>> {
        let lock = self.1.lock();
        let result = if let Some(Entry {
            route_sender,
            on_dropped,
            countdown,
            ..
        }) = lock.routes.get(&addr)
        {
            if *countdown == 0 {
                Ok(Route::new(route_sender.clone(), on_dropped.clone()))
            } else {
                Err(ResolutionError::unresolvable(addr))
            }
        } else {
            Err(ResolutionError::unresolvable(addr))
        };
        ready(result).boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RouteUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        let mut lock = self.1.lock();
        let result = if let Some(host_name) = host {
            Err(RouterError::ConnectionFailure(ConnectionError::Resolution(
                host_name.to_string(),
            )))
        } else if let Some((addr, countdown)) = lock.uri_mappings.get_mut(&route) {
            if *countdown == 0 {
                Ok(*addr)
            } else {
                *countdown -= 1;
                let addr = *addr;
                if let Some(Entry { countdown, .. }) = lock.routes.get_mut(&addr) {
                    *countdown -= 1;
                }
                // A non-fatal error that will allow a retry.
                Err(RouterError::ConnectionFailure(ConnectionError::Http(
                    HttpError::new(HttpErrorKind::StatusCode(Some(StatusCode::CONTINUE)), None),
                )))
            }
        } else {
            Err(RouterError::NoAgentAtRoute(route))
        };
        ready(result).boxed()
    }
}

impl LocalRoutes {
    pub fn add_with_countdown(
        &self,
        uri: RouteUri,
        countdown: u8,
    ) -> mpsc::Receiver<TaggedEnvelope> {
        let (tx, rx) = mpsc::channel(8);
        self.add_sender_with_countdown(uri, tx, countdown);
        rx
    }

    pub fn add_sender(&self, uri: RouteUri, tx: mpsc::Sender<TaggedEnvelope>) {
        self.add_sender_with_countdown(uri, tx, 0);
    }

    fn add_sender_with_countdown(
        &self,
        uri: RouteUri,
        tx: mpsc::Sender<TaggedEnvelope>,
        countdown: u8,
    ) {
        let LocalRoutes(owner_addr, inner) = self;
        let LocalRoutesInner {
            routes,
            uri_mappings,
            counter,
        } = &mut *inner.lock();
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
                routes.insert(
                    id,
                    Entry {
                        route_sender: TaggedSender::new(*owner_addr, tx),
                        on_dropped: drop_rx,
                        on_drop: drop_tx,
                        countdown,
                    },
                );
            }
        }
    }

    pub fn add(&self, uri: RouteUri) -> mpsc::Receiver<TaggedEnvelope> {
        self.add_with_countdown(uri, 0)
    }

    pub fn remove(&self, uri: RouteUri) -> promise::Sender<ConnectionDropped> {
        let LocalRoutesInner {
            routes,
            uri_mappings,
            ..
        } = &mut *self.1.lock();
        let Entry { on_drop, .. } = uri_mappings
            .remove(&uri)
            .and_then(|(id, _)| routes.remove(&id))
            .unwrap();
        on_drop
    }
}

impl RouterFactory for LocalRoutes {
    type Router = LocalRoutes;

    fn create_for(&self, addr: RoutingAddr) -> Self::Router {
        let LocalRoutes(_, inner) = self;
        LocalRoutes(addr, inner.clone())
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RouteUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        Router::lookup(self, host, route)
    }
}
