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

use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
    str::FromStr,
    time::Duration,
};

use http::Uri;
use swim_model::Value;
use swim_utilities::routing::route_uri::RouteUri;

use crate::{data::DataKind, oneshot};

mod parse;

pub use parse::parse_app_command;

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Host {
    pub scheme: String,
    pub host_name: String,
    pub port: u16,
}

impl Host {
    pub fn new(host_name: String, port: u16) -> Self {
        Host {
            scheme: "ws".to_string(),
            host_name,
            port,
        }
    }

    pub fn with_scheme(scheme: String, host_name: String, port: u16) -> Self {
        Host {
            scheme,
            host_name,
            port,
        }
    }

    pub fn host_only(&self) -> String {
        let Host {
            host_name, port, ..
        } = self;
        format!("{}:{}", host_name, port)
    }
}

impl Display for Host {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", &self.scheme, &self.host_name, self.port)
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Endpoint {
    pub remote: Host,
    pub node: RouteUri,
    pub lane: String,
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Endpoint { remote, node, lane } = self;
        write!(
            f,
            "{{ host => '{}', node => '{}', lane => '{}' }}",
            remote, node, lane
        )
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum EndpointOrId {
    Endpoint(Endpoint),
    Id(usize),
}

#[derive(PartialEq, Eq, Debug, Default, Hash)]
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
    CommandTarget(String),
    Direct(Target),
}

#[derive(PartialEq, Eq, Debug, Hash)]
pub enum ControllerCommand {
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
        kind: LinkKind,
        sync: bool,
    },
    Periodically {
        target: String,
        delay: Duration,
        limit: Option<usize>,
        kind: DataKind,
    },
    Sync(LinkRef),
    Target {
        name: String,
        target: Target,
    },
    Unlink(LinkRef),
    UnlinkAll,
    Query(LinkRef),
    Meta(MetaCommand),
}

#[allow(dead_code)]
#[derive(PartialEq, Eq, Debug, Hash)]
pub enum MetaCommand {
    Lanes(LinkRef),
    NodePulse(LinkRef),
    LanePulse(LinkRef),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LinkKind {
    Event,
    Map,
}

pub enum AppCommand {
    Quit,
    Clear,
    Help { command_name: Option<String> },
    Controller(ControllerCommand),
}

#[derive(Debug)]
pub enum RuntimeCommand {
    Link {
        endpoint: Endpoint,
        response: oneshot::Sender<Result<usize, ratchet::Error>>,
        kind: LinkKind,
        immediate_sync: bool,
    },
    Sync(usize),
    Command(usize, Value),
    AdHocCommand(Endpoint, Value),
    Unlink(usize),
    UnlinkAll,
    Query(usize),
    Periodically {
        endpoint: Endpoint,
        delay: Duration,
        limit: Option<usize>,
        kind: DataKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogMessageKind {
    Report,
    Data,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum UIUpdate {
    LinkDisplay(DisplayResponse),
    LogMessage(LogMessageKind, String),
}

impl UIUpdate {
    pub fn log_error(msg: String) -> Self {
        UIUpdate::LogMessage(LogMessageKind::Error, msg)
    }

    pub fn log_report(msg: String) -> Self {
        UIUpdate::LogMessage(LogMessageKind::Report, msg)
    }

    pub fn log_data(msg: String) -> Self {
        UIUpdate::LogMessage(LogMessageKind::Data, msg)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DisplayResponse {
    pub id: usize,
    pub body: DisplayResponseBody,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DisplayResponseBody {
    Linked,
    Synced,
    Event(String),
    Unlinked(Option<String>),
}

impl DisplayResponse {
    pub fn linked(id: usize) -> Self {
        DisplayResponse {
            id,
            body: DisplayResponseBody::Linked,
        }
    }

    pub fn synced(id: usize) -> Self {
        DisplayResponse {
            id,
            body: DisplayResponseBody::Synced,
        }
    }

    pub fn unlinked(id: usize, body: Option<String>) -> Self {
        DisplayResponse {
            id,
            body: DisplayResponseBody::Unlinked(body),
        }
    }

    pub fn event(id: usize, body: String) -> Self {
        DisplayResponse {
            id,
            body: DisplayResponseBody::Event(body),
        }
    }
}

impl Display for DisplayResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DisplayResponse { id, body } = self;
        match body {
            DisplayResponseBody::Linked => write!(f, "{}: LINKED", id),
            DisplayResponseBody::Synced => write!(f, "{}: SYNCED", id),
            DisplayResponseBody::Event(b) => write!(f, "{}: EVENT => {}", id, b),
            DisplayResponseBody::Unlinked(None) => write!(f, "{}: UNLINKED", id),
            DisplayResponseBody::Unlinked(Some(b)) => write!(f, "{}: UNLINKED => {}", id, b),
        }
    }
}

const DEFAULT_PORT: u16 = 8080;

impl FromStr for Host {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<Uri>() {
            Ok(uri) => {
                if let Some(auth) = uri.authority() {
                    let host_name = auth.host().to_string();
                    let port = auth.port_u16().unwrap_or(DEFAULT_PORT);
                    if let Some(scheme) = uri.scheme_str() {
                        Ok(Host::with_scheme(scheme.to_string(), host_name, port))
                    } else {
                        Ok(Host::new(host_name, port))
                    }
                } else {
                    Err(Cow::Borrowed("Missing authority."))
                }
            }
            Err(err) => Err(Cow::Owned(format!("{}", err))),
        }
    }
}

impl FromStr for LinkRef {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(Cow::Borrowed("Invalid link reference."))
        } else if let Ok(id) = s.parse() {
            Ok(LinkRef::ById(id))
        } else {
            Ok(LinkRef::ByName(s.to_string()))
        }
    }
}
