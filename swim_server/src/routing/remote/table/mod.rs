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

use crate::routing::remote::ConnectionDropped;
use crate::routing::{Route, RoutingAddr, TaggedEnvelope};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use utilities::sync::promise;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAndPort(String, u16);

impl HostAndPort {
    pub fn new(host: String, port: u16) -> Self {
        HostAndPort(host, port)
    }
}

impl Display for HostAndPort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let HostAndPort(host, port) = self;
        write!(f, "{}:{}", host, port)
    }
}

#[derive(Debug, Default)]
pub struct RoutingTable {
    open_sockets: HashMap<SocketAddr, RoutingAddr>,
    resolved_forward: HashMap<HostAndPort, RoutingAddr>,
    endpoints: HashMap<RoutingAddr, Handle>,
}

impl RoutingTable {
    pub fn try_resolve(&self, target: &HostAndPort) -> Option<RoutingAddr> {
        self.resolved_forward.get(target).copied()
    }

    pub fn get_resolved(&self, target: &SocketAddr) -> Option<RoutingAddr> {
        self.open_sockets.get(target).copied()
    }

    pub fn resolve(&self, addr: RoutingAddr) -> Option<Route<mpsc::Sender<TaggedEnvelope>>> {
        self.endpoints
            .get(&addr)
            .map(|h| Route::new(h.tx.clone(), h.drop_rx.clone()))
    }

    pub fn insert(
        &mut self,
        addr: RoutingAddr,
        host: Option<HostAndPort>,
        sock_addr: SocketAddr,
        tx: mpsc::Sender<TaggedEnvelope>,
    ) {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
        } = self;
        debug_assert!(!open_sockets.contains_key(&sock_addr));

        open_sockets.insert(sock_addr, addr);
        let mut hosts = HashSet::new();
        if let Some(host) = host {
            resolved_forward.insert(host.clone(), addr);
            hosts.insert(host);
        }

        endpoints.insert(addr, Handle::new(tx, sock_addr, hosts));
    }

    pub fn add_host(&mut self, host: HostAndPort, sock_addr: SocketAddr) -> Option<RoutingAddr> {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
            ..
        } = self;

        if let Some(addr) = open_sockets.get(&sock_addr) {
            debug_assert!(!resolved_forward.contains_key(&host));
            resolved_forward.insert(host.clone(), *addr);
            let handle = endpoints.get_mut(&addr).expect("Inconsistent table.");
            handle.bindings.insert(host);
            Some(*addr)
        } else {
            None
        }
    }

    pub fn remove(&mut self, addr: RoutingAddr) -> Option<promise::Sender<ConnectionDropped>> {
        let RoutingTable {
            open_sockets,
            resolved_forward,
            endpoints,
            ..
        } = self;
        if let Some(Handle {
            peer,
            bindings,
            drop_tx,
            ..
        }) = endpoints.remove(&addr)
        {
            open_sockets.remove(&peer);
            bindings.iter().for_each(move |h| {
                resolved_forward.remove(h);
            });
            Some(drop_tx)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct Handle {
    tx: mpsc::Sender<TaggedEnvelope>,
    drop_tx: promise::Sender<ConnectionDropped>,
    drop_rx: promise::Receiver<ConnectionDropped>,
    peer: SocketAddr,
    bindings: HashSet<HostAndPort>,
}

impl Handle {
    fn new(
        tx: mpsc::Sender<TaggedEnvelope>,
        peer: SocketAddr,
        bindings: HashSet<HostAndPort>,
    ) -> Self {
        let (drop_tx, drop_rx) = promise::promise();
        Handle {
            tx,
            drop_tx,
            drop_rx,
            peer,
            bindings,
        }
    }
}
