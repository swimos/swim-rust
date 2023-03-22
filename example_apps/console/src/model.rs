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
};

use swim::{model::Value, route::RouteUri};
use swim_recon::parser::parse_value;

use crate::oneshot;

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Host {
    pub host_name: String,
    pub port: u16,
}

impl Display for Host {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
pub enum UIUpdate {
    LinkDisplay(DisplayResponse),
    LogMessage(String),
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
    Unlinked,
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

    pub fn unlinked(id: usize) -> Self {
        DisplayResponse {
            id,
            body: DisplayResponseBody::Unlinked,
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
            DisplayResponseBody::Unlinked => write!(f, "{}: UNLINKED", id),
        }
    }
}

const DEFAULT_PORT: u16 = 8080;

impl FromStr for Host {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_terminator(':');
        let host_name = if let Some(name) = parts.next() {
            if name.is_empty() {
                return Err(Cow::Borrowed("Empty host name."));
            } else {
                name.to_string()
            }
        } else {
            return Err(Cow::Borrowed("Empty host name."));
        };
        let port = if let Some(p) = parts.next() {
            if let Ok(port) = p.parse::<u16>() {
                port
            } else {
                return Err(Cow::Owned(format!("Invalid port: {}", p)));
            }
        } else {
            DEFAULT_PORT
        };

        if parts.next().is_none() {
            Ok(Host { host_name, port })
        } else {
            Err(Cow::Owned(format!("Invalid remote host: {}", s)))
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

pub fn parse_app_command(parts: &[&str]) -> Result<AppCommand, Cow<'static, str>> {
    match parts {
        ["with-host", host] => {
            let h = host.parse()?;
            Ok(AppCommand::WithHost(h))
        }
        ["with-node", node] => {
            let n = node
                .parse()
                .map_err(|_| Cow::Borrowed("Invalid route URI."))?;
            Ok(AppCommand::WithNode(n))
        }
        ["with-lane", lane] => Ok(AppCommand::WithLane(lane.to_string())),
        ["show-with"] => Ok(AppCommand::ShowWith),
        ["clear-with"] => Ok(AppCommand::ClearWith),
        ["command", tail @ ..] => {
            let (consumed, target) = parse_target_ref(tail)?;
            match &tail[consumed..] {
                [body] => {
                    if let Ok(value) = parse_value(body, false) {
                        Ok(AppCommand::Command {
                            target,
                            body: value,
                        })
                    } else {
                        Err(Cow::Owned(format!("'{}' is not value recon.", body)))
                    }
                }
                [] => Ok(AppCommand::Command {
                    target,
                    body: Value::Extant,
                }),
                _ => Err(Cow::Borrowed("Too many parameters for command.")),
            }
        }
        ["list"] => Ok(AppCommand::ListLinks),
        ["link", tail @ ..] => {
            let (consumed, target) = parse_target(tail)?;
            match &tail[consumed..] {
                [name] => Ok(AppCommand::Link {
                    name: Some(name.to_string()),
                    target,
                }),
                [] => Ok(AppCommand::Link { name: None, target }),
                _ => Err(Cow::Borrowed("Too many parameters for link.")),
            }
        }
        ["sync", target] => {
            let r = target.parse()?;
            Ok(AppCommand::Sync(r))
        }
        ["unlink", target] => {
            let r = target.parse()?;
            Ok(AppCommand::Unlink(r))
        }
        _ => Err(Cow::Borrowed("Unknown command.")),
    }
}

pub fn parse_target_ref(parts: &[&str]) -> Result<(usize, TargetRef), Cow<'static, str>> {
    match parts {
        [] => Ok((0, TargetRef::Direct(Target::default()))),
        [first, ..] if first.starts_with('-') => {
            let (consumed, target) = parse_target(parts)?;
            Ok((consumed, TargetRef::Direct(target)))
        }
        [arg, ..] => {
            let r = if let Ok(id) = arg.parse() {
                TargetRef::Link(LinkRef::ById(id))
            } else {
                TargetRef::Link(LinkRef::ByName(arg.to_string()))
            };
            Ok((1, r))
        }
    }
}

enum TargetPart {
    Host,
    Node,
    Lane,
}

pub fn parse_target(parts: &[&str]) -> Result<(usize, Target), Cow<'static, str>> {
    let mut expected = None;
    let mut target = Target::default();
    let it = parts.iter().enumerate();
    for (i, part) in it {
        match expected {
            Some(TargetPart::Host) => {
                target.remote = Some(part.parse()?);
                expected = None;
            }
            Some(TargetPart::Node) => {
                let n = part
                    .parse()
                    .map_err(|_| Cow::Borrowed("Invalid route URI."))?;
                target.node = Some(n);
                expected = None;
            }
            Some(TargetPart::Lane) => {
                target.lane = Some(part.to_string());
                expected = None;
            }
            _ => match *part {
                "--host" | "-h" => {
                    expected = Some(TargetPart::Host);
                }
                "--node" | "-n" => {
                    expected = Some(TargetPart::Node);
                }
                "--lane" | "-l" => {
                    expected = Some(TargetPart::Lane);
                }
                _ => {
                    return Ok((i, target));
                }
            },
        }
    }
    Ok((parts.len(), target))
}
