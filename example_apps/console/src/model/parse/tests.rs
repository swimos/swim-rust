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

use swim_model::Value;
use swim_recon::parser::parse_value;
use swim_utilities::routing::route_uri::RouteUri;

use crate::model::{AppCommand, ControllerCommand, Host, LinkKind, LinkRef, Target, TargetRef};

use super::Tokenizer;

#[test]
fn empty_string() {
    let tok = Tokenizer::new("");
    let parts = tok.collect::<Vec<_>>();
    assert!(parts.is_empty());
}

#[test]
fn single_token() {
    let tok = Tokenizer::new("one");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one"]);
}

#[test]
fn single_token_padded() {
    let tok = Tokenizer::new("  one ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one"]);
}

#[test]
fn two_tokens() {
    let tok = Tokenizer::new("one two");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two"]);
}

#[test]
fn two_token_padded() {
    let tok = Tokenizer::new("   one    two ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two"]);
}

#[test]
fn three_tokens() {
    let tok = Tokenizer::new("one two three");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "two", "three"]);
}

#[test]
fn quoted_token() {
    let tok = Tokenizer::new("`multiple words`");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["multiple words"]);
}

#[test]
fn quoted_token_padded() {
    let tok = Tokenizer::new("  `  multiple words ` ");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["  multiple words "]);
}

#[test]
fn mixed_tokens1() {
    let tok = Tokenizer::new("one `multiple words`");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "multiple words"]);
}

#[test]
fn mixed_tokens2() {
    let tok = Tokenizer::new("`multiple words`one");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["multiple words", "one"]);
}

#[test]
fn mixed_tokens3() {
    let tok = Tokenizer::new("one `multiple words` two");
    let parts = tok.map(ToOwned::to_owned).collect::<Vec<_>>();
    assert_eq!(parts, vec!["one", "multiple words", "two"]);
}

fn to_controller(cmd: AppCommand) -> ControllerCommand {
    if let AppCommand::Controller(c) = cmd {
        c
    } else {
        panic!("Unexpected command kind.");
    }
}

#[test]
fn parse_link() {
    let host: Host = "localhost:8080".parse().unwrap();
    let node: RouteUri = "/node".parse().unwrap();

    let cmd = to_controller(super::parse_app_command("link").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: None,
            target: Target::default(),
            kind: LinkKind::Event,
            sync: false,
        }
    );

    let cmd = to_controller(
        super::parse_app_command("link --host localhost:8080").expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: None,
            target: Target {
                remote: Some(host.clone()),
                node: None,
                lane: None
            },
            kind: LinkKind::Event,
            sync: false,
        }
    );

    let cmd = to_controller(
        super::parse_app_command("link --host localhost:8080 -n /node -l lane")
            .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: None,
            target: Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("lane".to_string())
            },
            kind: LinkKind::Event,
            sync: false,
        }
    );

    let cmd =
        to_controller(super::parse_app_command("link --name my_link").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: Some("my_link".to_string()),
            target: Target::default(),
            kind: LinkKind::Event,
            sync: false,
        }
    );

    let cmd = to_controller(super::parse_app_command("link --map").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: None,
            target: Target::default(),
            kind: LinkKind::Map,
            sync: false,
        }
    );

    let cmd = to_controller(
        super::parse_app_command(
            "link --host localhost:8080 -n /node -l lane --map --name my_link",
        )
        .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: Some("my_link".to_string()),
            target: Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("lane".to_string())
            },
            kind: LinkKind::Map,
            sync: false,
        }
    );

    let cmd = to_controller(
        super::parse_app_command(
            "link --host localhost:8080 -n /node -l lane --map --sync --name my_link",
        )
        .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Link {
            name: Some("my_link".to_string()),
            target: Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("lane".to_string())
            },
            kind: LinkKind::Map,
            sync: true,
        }
    );
}

#[test]
fn parse_command() {
    let host: Host = "localhost:8080".parse().unwrap();
    let node: RouteUri = "/node".parse().unwrap();

    let cmd = to_controller(super::parse_app_command("command").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Direct(Target {
                remote: None,
                node: None,
                lane: None
            }),
            body: Value::Extant,
        }
    );

    let cmd = to_controller(super::parse_app_command("command 1").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Link(LinkRef::ById(1)),
            body: Value::Extant,
        }
    );

    let cmd = to_controller(super::parse_app_command("command my_link").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Link(LinkRef::ByName("my_link".to_string())),
            body: Value::Extant,
        }
    );

    let cmd =
        to_controller(super::parse_app_command("command $my_target").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::CommandTarget("my_target".to_string()),
            body: Value::Extant,
        }
    );

    let cmd = to_controller(super::parse_app_command("command 1 42").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Link(LinkRef::ById(1)),
            body: Value::Int32Value(42),
        }
    );

    let cmd =
        to_controller(super::parse_app_command("command my_link Hello").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Link(LinkRef::ByName("my_link".to_string())),
            body: Value::text("Hello"),
        }
    );

    let cmd = to_controller(
        super::parse_app_command("command $my_target true").expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::CommandTarget("my_target".to_string()),
            body: Value::BooleanValue(true),
        }
    );

    let cmd = to_controller(
        super::parse_app_command("command --host localhost:8080 -n /node -l lane")
            .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Direct(Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("lane".to_string())
            }),
            body: Value::Extant
        }
    );

    let expected_value = parse_value("@complex {1, 2, 3}", false).unwrap();

    let cmd = to_controller(
        super::parse_app_command(
            "command --host localhost:8080 -n /node -l lane `@complex {1, 2, 3}`",
        )
        .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Command {
            target: TargetRef::Direct(Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("lane".to_string())
            }),
            body: expected_value
        }
    );
}

#[test]
fn parse_target() {
    let host: Host = "localhost:8080".parse().unwrap();
    let node: RouteUri = "/node".parse().unwrap();

    let cmd = to_controller(super::parse_app_command("target name").expect("Should succeed."));
    assert_eq!(
        cmd,
        ControllerCommand::Target {
            name: "name".to_string(),
            target: Target::default()
        }
    );

    let cmd = to_controller(
        super::parse_app_command("target --host localhost:8080 name").expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Target {
            name: "name".to_string(),
            target: Target {
                remote: Some(host.clone()),
                node: None,
                lane: None
            }
        }
    );

    let cmd = to_controller(
        super::parse_app_command("target --node /node name").expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Target {
            name: "name".to_string(),
            target: Target {
                remote: None,
                node: Some(node.clone()),
                lane: None
            }
        }
    );

    let cmd = to_controller(
        super::parse_app_command("target --lane my_lane name").expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Target {
            name: "name".to_string(),
            target: Target {
                remote: None,
                node: None,
                lane: Some("my_lane".to_string())
            }
        }
    );

    let cmd = to_controller(
        super::parse_app_command("target --lane my_lane --node /node --host localhost:8080 name")
            .expect("Should succeed."),
    );
    assert_eq!(
        cmd,
        ControllerCommand::Target {
            name: "name".to_string(),
            target: Target {
                remote: Some(host.clone()),
                node: Some(node.clone()),
                lane: Some("my_lane".to_string())
            }
        }
    );
}
