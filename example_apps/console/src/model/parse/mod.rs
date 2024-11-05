// Copyright 2015-2024 Swim Inc.
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

use std::num::ParseIntError;
use std::{borrow::Cow, time::Duration};

use std::collections::HashMap;
use swimos_agent_protocol::MapMessage;
use swimos_model::Value;
use swimos_recon::parser::parse_recognize;

use crate::data::DataKind;

use super::{AppCommand, ControllerCommand, LinkKind, LinkRef, Target, TargetRef};

#[cfg(test)]
mod tests;

pub struct Tokenizer<'a> {
    input: &'a str,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Tokenizer { input }
    }
}

enum TokState {
    Before,
    InNormal,
    QuoteStart,
    InQuoted,
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let Tokenizer { input } = self;
        let mut state = TokState::Before;
        let mut start = None;

        let mut indices = input.char_indices();
        let (end, rem_start) = loop {
            if let Some((index, c)) = indices.next() {
                match state {
                    TokState::Before => {
                        if c == '`' {
                            state = TokState::QuoteStart;
                        } else if !c.is_whitespace() {
                            state = TokState::InNormal;
                            start = Some(index);
                        }
                    }
                    TokState::InNormal => {
                        if c.is_whitespace() {
                            break (index, index);
                        }
                    }
                    TokState::QuoteStart => {
                        start = Some(index);
                        if c == '`' {
                            break (index, index + c.len_utf8());
                        } else {
                            state = TokState::InQuoted;
                        }
                    }
                    TokState::InQuoted => {
                        if c == '`' {
                            break (index, index + c.len_utf8());
                        }
                    }
                }
            } else {
                break (input.len(), input.len());
            }
        };
        let result = start.map(|s| &input[s..end]);
        *input = &input[rem_start..];
        result
    }
}

pub fn parse_app_command(command: &str) -> Result<AppCommand, Cow<'static, str>> {
    let command_parts = Tokenizer::new(command).collect::<Vec<_>>();

    match command_parts.as_slice() {
        ["help"] => Ok(AppCommand::Help { command_name: None }),
        ["help", cmd_name] => Ok(AppCommand::Help {
            command_name: Some(cmd_name.to_string()),
        }),
        ["quit"] => Ok(AppCommand::Quit),
        ["clear"] => Ok(AppCommand::Clear),
        _ => Ok(AppCommand::Controller(Box::new(parse_controller_command(
            command_parts.as_slice(),
        )?))),
    }
}

pub fn parse_controller_command(parts: &[&str]) -> Result<ControllerCommand, Cow<'static, str>> {
    match parts {
        ["with-host", host] => {
            let h = host.parse()?;
            Ok(ControllerCommand::WithHost(h))
        }
        ["with-node", node] => {
            let n = node
                .parse()
                .map_err(|_| Cow::Borrowed("Invalid route URI."))?;
            Ok(ControllerCommand::WithNode(n))
        }
        ["with-lane", lane] => Ok(ControllerCommand::WithLane(lane.to_string())),
        ["show-with"] => Ok(ControllerCommand::ShowWith),
        ["clear-with"] => Ok(ControllerCommand::ClearWith),
        ["command", tail @ ..] => {
            let (target, tail) = parse_target_ref(tail)?;
            match tail {
                [body] => {
                    if let Ok(value) = parse_recognize(*body, false) {
                        Ok(ControllerCommand::Command {
                            target,
                            body: value,
                        })
                    } else {
                        Err(Cow::Owned(format!("'{}' is not valid recon.", body)))
                    }
                }
                [] => Ok(ControllerCommand::Command {
                    target,
                    body: Value::Extant,
                }),
                _ => Err(Cow::Borrowed(
                    "Incorrect parameters for command. Type 'help command' for correct usage.",
                )),
            }
        }
        ["map-command", tail @ ..] => {
            let (target, tail) = parse_target_ref(tail)?;
            match tail {
                ["update", key, value] => {
                    let result = parse_recognize(*key, false).and_then(|k| {
                        parse_recognize(*value, false)
                            .map(move |v| MapMessage::Update { key: k, value: v })
                    });
                    if let Ok(message) = result {
                        Ok(ControllerCommand::MapCommand {
                            target,
                            body: message,
                        })
                    } else {
                        Err(Cow::Owned(format!(
                            "One of '{}', '{}' is not valid recon.",
                            key, value
                        )))
                    }
                }
                ["remove", key] => {
                    let result =
                        parse_recognize(*key, false).map(|k| MapMessage::Remove { key: k });
                    if let Ok(message) = result {
                        Ok(ControllerCommand::MapCommand {
                            target,
                            body: message,
                        })
                    } else {
                        Err(Cow::Owned(format!("'{}' is not valid recon.", key)))
                    }
                }
                ["clear"] => Ok(ControllerCommand::MapCommand {
                    target,
                    body: MapMessage::Clear,
                }),
                _ => Err(Cow::Borrowed(
                    "Incorrect parameters for command. Type 'help command' for correct usage.",
                )),
            }
        }
        ["list"] => Ok(ControllerCommand::ListLinks),
        ["link", tail @ ..] => {
            let (mut options, tail) = parse_options(tail);
            let target = options.target()?.unwrap_or_default();
            let name = options
                .take("name", None)
                .flatten()
                .map(ToString::to_string);
            let kind = if matches!(options.take("map", Some('m')), Some(None)) {
                LinkKind::Map
            } else {
                LinkKind::Event
            };
            let sync = matches!(options.take("sync", None), Some(None));
            if !tail.is_empty() || !options.is_empty() {
                Err(Cow::Borrowed(
                    "Incorrect parameters to link. Type 'help link' for correct usage.",
                ))
            } else {
                Ok(ControllerCommand::Link {
                    name,
                    target,
                    kind,
                    sync,
                })
            }
        }
        ["periodically", tail @ ..] => {
            let (mut options, tail) = parse_options(tail);
            let delay = parse_duration(options.take("delay", Some('d')).flatten().unwrap_or("1s"))?;
            let kind = parse_kind(options.take("kind", Some('k')).flatten().unwrap_or("words"))?;
            let limit = if let Some(l) = options.take("limit", None).flatten() {
                if let Ok(lim) = l.parse::<usize>() {
                    Some(lim)
                } else {
                    return Err(Cow::Borrowed("Limit must be a non-zero integer."));
                }
            } else {
                None
            };
            match tail {
                [target] if options.is_empty() => {
                    if let Some(target_name) = target.strip_prefix('$') {
                        Ok(ControllerCommand::Periodically { target: target_name.to_string(), delay, limit, kind })
                    } else {
                        Err(Cow::Owned(format!("{} is not a valid target.", target)))
                    }
                },
                _ => Err(Cow::Borrowed("Incorrect parameters to periodically. Type 'help periodically; for correct usage."))
            }
        }
        ["sync", target] => {
            let r = target.parse()?;
            Ok(ControllerCommand::Sync(r))
        }
        ["target", tail @ ..] => {
            let (mut options, tail) = parse_options(tail);
            let target = options.target()?.unwrap_or_default();
            match tail {
                [name] if options.is_empty() => Ok(ControllerCommand::Target {
                    name: name.to_string(),
                    target,
                }),
                _ => Err(Cow::Borrowed(
                    "Incorrect parameters to target. Type 'help target' for correct usage.",
                )),
            }
        }
        ["unlink", "--all"] => Ok(ControllerCommand::UnlinkAll),
        ["unlink", target] => {
            let r = target.parse()?;
            Ok(ControllerCommand::Unlink(r))
        }
        ["query", target] => {
            let r = target.parse()?;
            Ok(ControllerCommand::Query(r))
        }
        _ => Err(Cow::Borrowed(
            "Unknown command. Type 'help' to list valid commands.",
        )),
    }
}

pub fn parse_target_ref<'a, 'b>(
    parts: &'a [&'b str],
) -> Result<(TargetRef, &'a [&'b str]), Cow<'static, str>> {
    let (mut options, tail) = parse_options(parts);
    if let Some(target) = options.target()? {
        Ok((TargetRef::Direct(target), tail))
    } else {
        match tail.split_first() {
            Some((arg, tail)) if options.is_empty() => {
                let r = if let Some(cmd_target) = arg.strip_prefix('$') {
                    TargetRef::CommandTarget(cmd_target.to_string())
                } else if let Ok(id) = arg.parse() {
                    TargetRef::Link(LinkRef::ById(id))
                } else {
                    TargetRef::Link(LinkRef::ByName(arg.to_string()))
                };
                Ok((r, tail))
            }
            None => Ok((TargetRef::Direct(Target::default()), &[])),
            _ => Err(Cow::Borrowed(
                "Incorrect parameters for command. Type 'help command' for correct usage.",
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum OptionName<'a> {
    Short(char),
    Long(&'a str),
}

#[derive(Debug)]
struct Options<'a> {
    opts: HashMap<OptionName<'a>, Option<&'a str>>,
}

impl<'a> Options<'a> {
    fn take(&mut self, long: &'static str, short: Option<char>) -> Option<Option<&'a str>> {
        let Options { opts } = self;
        opts.remove(&OptionName::Long(long))
            .or_else(|| short.and_then(|s| opts.remove(&OptionName::Short(s))))
    }

    fn target(&mut self) -> Result<Option<Target>, Cow<'static, str>> {
        let mut target = Target::default();
        if let Some(host) = self.take("host", Some('h')).flatten() {
            target.remote = Some(host.parse()?);
        }
        if let Some(node) = self.take("node", Some('n')).flatten() {
            target.node = Some(
                node.parse()
                    .map_err(|_| Cow::Borrowed("Invalid route URI."))?,
            );
        }
        if let Some(lane) = self.take("lane", Some('l')).flatten() {
            target.lane = Some(lane.to_string());
        }

        if target.remote.is_none() && target.node.is_none() && target.lane.is_none() {
            Ok(None)
        } else {
            Ok(Some(target))
        }
    }

    fn is_empty(&self) -> bool {
        self.opts.is_empty()
    }
}

fn parse_options<'a, 'b>(parts: &'a [&'b str]) -> (Options<'b>, &'a [&'b str]) {
    let mut opts: HashMap<OptionName<'b>, Option<&'b str>> = HashMap::new();
    let mut current = None;

    let mut it = parts.iter().enumerate();
    let end = loop {
        if let Some((i, part)) = it.next() {
            if let Some(long_name) = part.strip_prefix("--") {
                if let Some(name) = current.take() {
                    opts.insert(name, None);
                }
                current = Some(OptionName::Long(long_name));
            } else if let Some(chars) = part.strip_prefix('-') {
                for c in chars.chars() {
                    if let Some(name) = current.take() {
                        opts.insert(name, None);
                    }
                    current = Some(OptionName::Short(c));
                }
            } else if let Some(name) = current.take() {
                opts.insert(name, Some(*part));
            } else {
                break Some(i);
            }
        } else {
            if let Some(name) = current.take() {
                opts.insert(name, None);
            }
            break None;
        }
    };
    if let Some(i) = end {
        (Options { opts }, &parts[i..])
    } else {
        (Options { opts }, &[])
    }
}

fn parse_duration(dur_str: &str) -> Result<Duration, Cow<'static, str>> {
    duration_str::parse(dur_str)
        .map_err(|_| Cow::Owned(format!("{} is not a valid duration.", dur_str)))
}

fn parse_kind(kind_str: &str) -> Result<DataKind, Cow<'static, str>> {
    let parts: Vec<_> = kind_str.split("..").collect();
    match parts.as_slice() {
        ["", ""] => Ok(DataKind::I32(None)),
        [start, end] => parse_range(start, end).map_err(|_| Cow::Borrowed("Invalid data kind.")),
        ["words"] => Ok(DataKind::Words),
        _ => Err(Cow::Borrowed("Invalid data kind.")),
    }
}

fn parse_range(start: &str, end: &str) -> Result<DataKind, ParseIntError> {
    let s = start.parse::<i64>()?;
    let e = end.parse::<i64>()?;
    Ok(match (i32::try_from(s), i32::try_from(e)) {
        (Ok(s), Ok(e)) => DataKind::I32(Some(s..e)),
        _ => DataKind::I64(Some(s..e)),
    })
}
