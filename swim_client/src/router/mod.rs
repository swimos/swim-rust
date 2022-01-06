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

use crate::connections::ConnectionRegistrator;
use std::collections::HashMap;

use std::convert::TryFrom;

use swim_model::path::Addressable;
use swim_runtime::remote::table::SchemeHostPort;
use swim_runtime::remote::BadUrl;
use swim_runtime::routing::RoutingAddr;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) type Node = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RoutingPath {
    Remote(SchemeHostPort),
    Local(Node),
}

pub struct AddressableWrapper<T: Addressable>(pub T);

impl<T: Addressable> TryFrom<AddressableWrapper<T>> for RoutingPath {
    type Error = BadUrl;

    fn try_from(path: AddressableWrapper<T>) -> Result<Self, Self::Error> {
        match path.0.host() {
            Some(host) => Ok(RoutingPath::Remote(SchemeHostPort::try_from(host)?)),
            None => Ok(RoutingPath::Local(path.0.node().to_string())),
        }
    }
}

pub(crate) struct RoutingTable<Path: Addressable> {
    addresses: HashMap<RoutingPath, RoutingAddr>,
    endpoints: HashMap<RoutingAddr, ConnectionRegistrator<Path>>,
}

impl<Path: Addressable> RoutingTable<Path> {
    pub(crate) fn new() -> Self {
        RoutingTable {
            addresses: HashMap::new(),
            endpoints: HashMap::new(),
        }
    }

    pub(crate) fn try_resolve_addr(
        &self,
        routing_path: &RoutingPath,
    ) -> Option<(RoutingAddr, ConnectionRegistrator<Path>)> {
        let routing_addr = self.addresses.get(routing_path).copied()?;
        let endpoint = self.try_resolve_endpoint(&routing_addr)?;
        Some((routing_addr, endpoint))
    }

    pub(crate) fn try_resolve_endpoint(
        &self,
        routing_addr: &RoutingAddr,
    ) -> Option<ConnectionRegistrator<Path>> {
        self.endpoints.get(routing_addr).cloned()
    }

    pub(crate) fn add_registrator(
        &mut self,
        routing_path: RoutingPath,
        routing_addr: RoutingAddr,
        connection_registrator: ConnectionRegistrator<Path>,
    ) {
        self.addresses.insert(routing_path, routing_addr);
        self.endpoints.insert(routing_addr, connection_registrator);
    }
}
