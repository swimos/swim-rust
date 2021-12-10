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

use crate::byte_routing::routing::router::{PlaneRoutingRequest, RouterError, RouterErrorKind};
use crate::byte_routing::routing::RawRoute;
use crate::routing::RoutingAddr;
use std::collections::HashMap;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc;

pub struct MockPlaneRouter {
    rx: mpsc::Receiver<PlaneRoutingRequest>,
    lut: HashMap<RelativeUri, RoutingAddr>,
    resolver: HashMap<RoutingAddr, Option<RawRoute>>,
}

impl MockPlaneRouter {
    pub fn new(
        rx: mpsc::Receiver<PlaneRoutingRequest>,
        lut: HashMap<RelativeUri, RoutingAddr>,
        resolver: HashMap<RoutingAddr, Option<RawRoute>>,
    ) -> MockPlaneRouter {
        MockPlaneRouter { rx, lut, resolver }
    }

    pub async fn run(self) {
        let MockPlaneRouter {
            mut rx,
            lut,
            mut resolver,
        } = self;

        while let Some(request) = rx.recv().await {
            match request {
                PlaneRoutingRequest::Endpoint { addr, request } => {
                    let response = match resolver.get_mut(&addr) {
                        Some(addr) => Ok(addr.take().expect("Route already taken")),
                        None => Err(RouterError::new(RouterErrorKind::Resolution)),
                    };
                    let _ = request.send(response);
                }
                PlaneRoutingRequest::Resolve { request, uri, .. } => {
                    let response = match lut.get(&uri) {
                        Some(addr) => Ok(*addr),
                        None => Err(RouterError::new(RouterErrorKind::Resolution)),
                    };
                    let _ = request.send(response);
                }
                PlaneRoutingRequest::Agent { .. } => {
                    panic!("Unexpected agent request")
                }
                PlaneRoutingRequest::Routes(_) => {
                    panic!("Unexpected routes request")
                }
            }
        }

        println!("Fixture router stopped");
    }
}
