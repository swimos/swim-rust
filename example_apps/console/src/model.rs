// Copyright 2015-2023 Swim Inc.
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

use std::fmt::Display;

use swim::{model::Value, route::RouteUri};

use crate::oneshot;

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Host {
    pub host_name: String,
    pub port: u16,
}

impl Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", &self.host_name, self.port)
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Endpoint {
    pub remote: Host,
    pub node: RouteUri,
    pub lane: String,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum EndpointOrId {
    Endpoint(Endpoint),
    Id(usize),
}

#[derive(PartialEq, Eq, Debug, Hash)]
pub struct Target {
    pub remote: Option<Host>,
    pub node: Option<RouteUri>,
    pub lane: Option<String>,
}

#[derive(PartialEq, Eq, Debug, Hash)]
pub enum LinkRef {
    ById(usize),
    ByName(String),
}

#[derive(PartialEq, Eq, Debug, Hash)]
pub enum TargetRef {
    Link(LinkRef),
    Direct(Target),
}

#[derive(PartialEq, Eq, Debug, Hash)]
pub enum AppCommand {
    WithHost(Host),
    WithNode(RouteUri),
    WithLane(String),
    ShowWith,
    ClearWith,
    Command {
        target: TargetRef,
        body: Value,
    },
    ListLinks,
    Link {
        name: Option<String>,
        target: Target,
        sync: bool,
    },
    Sync(LinkRef),
    Unlink(LinkRef),
}

#[derive(Debug)]
pub enum RuntimeCommand {
    Link {
        endpoint: Endpoint,
        response: oneshot::Sender<Result<usize, ratchet::Error>>,
    },
    Sync(usize),
    Command(usize, Value),
    AdHocCommand(Endpoint, Value),
    Unlink(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DisplayResponse {
    pub id: usize,
    pub body: String,
}

impl DisplayResponse {
    pub fn new(id: usize, body: String) -> Self {
        DisplayResponse { id, body }
    }
}
