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

use crate::routing::remote::table::HostAndPort;
use crate::routing::remote::{ResolutionRequest, REQUEST_DROPPED};
use crate::routing::RoutingAddr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use swim_common::routing::ConnectionError;
use swim_tracing::request::TryRequestExt;

#[cfg(test)]
mod tests;

/// Keeps track of pending routing requests to ensure that two requests for the same point are not
/// started simultaneously.
#[derive(Debug, Default)]
pub struct PendingRequests(HashMap<HostAndPort, Vec<ResolutionRequest>>);

impl PendingRequests {
    /// Add a new pending request for a specific host/port combination.
    pub fn add(&mut self, host: HostAndPort, request: ResolutionRequest) {
        let PendingRequests(map) = self;
        match map.entry(host) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(request);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![request]);
            }
        }
    }

    /// Complete all requests for a given host/port combination with a successful result.
    pub fn send_ok(&mut self, host: &HostAndPort, addr: RoutingAddr) {
        let PendingRequests(map) = self;
        if let Some(requests) = map.remove(host) {
            for request in requests.into_iter() {
                request.send_ok_debug(addr, REQUEST_DROPPED);
            }
        }
    }

    /// Complete all requests for a given host/port combination with an error.
    pub fn send_err(&mut self, host: &HostAndPort, err: ConnectionError) {
        let PendingRequests(map) = self;
        if let Some(mut requests) = map.remove(host) {
            let first = requests.pop();
            for request in requests.into_iter() {
                request.send_err_debug(err.clone(), REQUEST_DROPPED);
            }
            if let Some(first) = first {
                first.send_err_debug(err, REQUEST_DROPPED);
            }
        }
    }
}
