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

use crate::error::{ConnectionError, ResolutionError};
use crate::remote::table::SchemeHostPort;
use crate::remote::task::AttachClientRouted;
use crate::remote::{RawOutRoute, REQUEST_DROPPED};
use crate::routing::RoutingAddr;
use crate::routing::{AttachClientRequest, ResolutionRequest};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use swim_tracing::request::TryRequestExt;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum PendingRequest {
    Resolution(ResolutionRequest),
}

pub struct PendingClient {
    tx: mpsc::Sender<AttachClientRouted>,
    route: RawOutRoute,
    request: AttachClientRequest,
}

impl PendingClient {
    pub fn new(
        tx: mpsc::Sender<AttachClientRouted>,
        route: RawOutRoute,
        request: AttachClientRequest,
    ) -> Self {
        PendingClient { tx, route, request }
    }

    pub async fn send_attach_requests(self) {
        let PendingClient { tx, route, request } = self;
        let routed_request = AttachClientRouted::new(route.clone(), request);
        if let Err(e) = tx.send(routed_request).await {
            let AttachClientRouted {
                request: AttachClientRequest { addr, request, .. },
                ..
            } = e.0;
            request.send_err_debug(ResolutionError::Addr(addr), REQUEST_DROPPED);
        }
    }
}

/// Keeps track of pending routing requests to ensure that two requests for the same point are not
/// started simultaneously.
#[derive(Debug, Default)]
pub struct PendingRequests(HashMap<SchemeHostPort, Vec<PendingRequest>>);

impl PendingRequests {
    /// Add a new pending request for a specific host/port combination.
    pub fn add(&mut self, host: SchemeHostPort, request: PendingRequest) {
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
    pub fn send_ok(&mut self, host: &SchemeHostPort, addr: RoutingAddr) {
        let PendingRequests(map) = self;

        if let Some(requests) = map.remove(host) {
            for request in requests.into_iter() {
                match request {
                    PendingRequest::Resolution(request) => {
                        request.send_ok_debug(addr, REQUEST_DROPPED)
                    }
                }
            }
        }
    }

    /// Complete all requests for a given host/port combination with an error.
    pub fn send_err(&mut self, host: &SchemeHostPort, err: ConnectionError) {
        let PendingRequests(map) = self;
        if let Some(mut requests) = map.remove(host) {
            let first = requests.pop();
            for request in requests.into_iter() {
                match request {
                    PendingRequest::Resolution(request) => {
                        request.send_err_debug(err.clone(), REQUEST_DROPPED)
                    }
                }
            }
            if let Some(first) = first {
                match first {
                    PendingRequest::Resolution(request) => {
                        request.send_err_debug(err, REQUEST_DROPPED)
                    }
                }
            }
        }
    }
}
