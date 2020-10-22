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

use crate::routing::error::{ConnectionError, ResolutionError, RouterError, Unresolvable};
use crate::routing::remote::ConnectionDropped;
use crate::routing::{Route, RoutingAddr, ServerRouter, TaggedEnvelope, TaggedSender};
use futures::future::{ready, BoxFuture};
use futures::FutureExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

#[derive(Debug)]
pub struct LocalRoutesInner {
    routes: HashMap<RoutingAddr, (Route<TaggedSender>, promise::Sender<ConnectionDropped>)>,
    uri_mappings: HashMap<RelativeUri, RoutingAddr>,
    counter: u32,
}

#[derive(Debug, Clone)]
pub struct LocalRoutes(Arc<Mutex<LocalRoutesInner>>);

impl ServerRouter for LocalRoutes {
    type Sender = TaggedSender;

    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, ResolutionError>> {
        let lock = self.0.lock();
        let result = if let Some((route, _)) = lock.routes.get(&addr) {
            Ok(route.clone())
        } else {
            Err(ResolutionError::Unresolvable(Unresolvable(addr)))
        };
        ready(result).boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        let lock = self.0.lock();
        let result = if host.is_some() {
            Err(RouterError::ConnectionFailure(ConnectionError::Resolution))
        } else {
            if let Some(addr) = lock.uri_mappings.get(&route) {
                Ok(*addr)
            } else {
                Err(RouterError::NoAgentAtRoute(route))
            }
        };
        ready(result).boxed()
    }
}

impl LocalRoutes {
    pub fn add(&self, uri: RelativeUri) -> mpsc::Receiver<TaggedEnvelope> {
        let LocalRoutesInner {
            routes,
            uri_mappings,
            counter,
        } = &mut *self.0.lock();
        if uri_mappings.contains_key(&uri) {
            panic!("Duplicate registration.");
        } else {
            let id = RoutingAddr::local(*counter);
            *counter += 1;
            uri_mappings.insert(uri, id);
            let (tx, rx) = mpsc::channel(8);
            let (drop_tx, drop_rx) = promise::promise();
            let route = Route::new(TaggedSender::new(id, tx), drop_rx);
            routes.insert(id, (route, drop_tx));
            rx
        }
    }

    pub fn remove(&self, uri: RelativeUri) -> promise::Sender<ConnectionDropped> {
        let LocalRoutesInner {
            routes,
            uri_mappings,
            ..
        } = &mut *self.0.lock();
        let (_, drop_tx) = uri_mappings
            .remove(&uri)
            .and_then(|id| routes.remove(&id))
            .unwrap();
        drop_tx
    }
}
