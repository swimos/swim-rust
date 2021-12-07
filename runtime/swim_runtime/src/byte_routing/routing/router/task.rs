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

use crate::byte_routing::routing::router::{Address, RoutingRequest};
use crate::byte_routing::routing::RawRoute;
use crate::routing::RoutingAddr;
use std::collections::HashMap;
use swim_utilities::routing::uri::RelativeUri;
use tokio::sync::mpsc;

pub struct RouterTask {
    lut: HashMap<RelativeUri, RoutingAddr>,
    resolver: HashMap<RoutingAddr, Option<RawRoute>>,
    requests: mpsc::Receiver<RoutingRequest>,
}

impl RouterTask {
    pub fn new(requests: mpsc::Receiver<RoutingRequest>) -> RouterTask {
        RouterTask {
            lut: HashMap::default(),
            resolver: HashMap::default(),
            requests,
        }
    }

    pub async fn run(self) {
        let RouterTask { mut requests, .. } = self;

        while let Some(request) = requests.recv().await {
            match request {
                RoutingRequest::ResolveSender { .. } => {}
                RoutingRequest::Lookup {
                    address,
                    callback: _,
                } => match address {
                    Address::Local(_uri) => {}
                    Address::Remote(_url, _uri) => {}
                },
            }
        }
    }
}
