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

use std::borrow::Cow;

use swim::model::Value;
use swim_recon::parser::parse_value;

use super::{AppCommand, ControllerCommand, LinkRef, Target, TargetRef};

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
        _ => Ok(AppCommand::Controller(parse_controller_command(
            command_parts.as_slice(),
        )?)),
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
            let (consumed, target) = parse_target_ref(tail)?;
            match &tail[consumed..] {
                [body] => {
                    if let Ok(value) = parse_value(body, false) {
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
                    "In correct parameters for command. Type 'help command' for correct usage.",
                )),
            }
        }
        ["list"] => Ok(ControllerCommand::ListLinks),
        ["link", tail @ ..] => {
            let (consumed, target) = parse_target(tail)?;
            match &tail[consumed..] {
                [name] => Ok(ControllerCommand::Link {
                    name: Some(name.to_string()),
                    target,
                }),
                [] => Ok(ControllerCommand::Link { name: None, target }),
                _ => Err(Cow::Borrowed(
                    "Incorrect parameters for link. Type 'help link' for correct usage.",
                )),
            }
        }
        ["sync", target] => {
            let r = target.parse()?;
            Ok(ControllerCommand::Sync(r))
        }
        ["unlink", target] => {
            let r = target.parse()?;
            Ok(ControllerCommand::Unlink(r))
        }
        _ => Err(Cow::Borrowed(
            "Unknown command. Type 'help' to list valid commands.",
        )),
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
