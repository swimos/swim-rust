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

use crate::error::ConnectionError;
use crate::remote::router::{BidirectionalRequest, ResolutionRequest};
use crate::remote::table::{BidirectionalRegistrator, SchemeHostPort};
use crate::remote::REQUEST_DROPPED;
use crate::routing::RoutingAddr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use swim_tracing::request::TryRequestExt;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum PendingRequest {
    Resolution(ResolutionRequest),
    Bidirectional(BidirectionalRequest),
}

impl PendingRequest {
    pub fn send_ok_debug<M: tracing::Value + Debug>(
        self,
        addr: RoutingAddr,
        bidirectional_registrator: &BidirectionalRegistrator,
        message: M,
    ) {
        match self {
            PendingRequest::Resolution(request) => request.send_ok_debug(addr, message),
            PendingRequest::Bidirectional(request) => {
                request.send_ok_debug(bidirectional_registrator.clone(), message)
            }
        }
    }

    pub fn send_err_debug<M: tracing::Value + Debug>(self, err: ConnectionError, message: M) {
        match self {
            PendingRequest::Resolution(request) => request.send_err_debug(err, message),
            PendingRequest::Bidirectional(request) => request.send_err_debug(err, message),
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
    pub fn send_ok(
        &mut self,
        host: &SchemeHostPort,
        addr: RoutingAddr,
        bidirectional_registrator: BidirectionalRegistrator,
    ) {
        let PendingRequests(map) = self;
        if let Some(requests) = map.remove(host) {
            for request in requests.into_iter() {
                request.send_ok_debug(addr, &bidirectional_registrator, REQUEST_DROPPED)
            }
        }
    }

    /// Complete all requests for a given host/port combination with an error.
    pub fn send_err(&mut self, host: &SchemeHostPort, err: ConnectionError) {
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
