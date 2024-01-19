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

use anyhow::{anyhow, Result};
use cargo_generate::{generate, GenerateArgs, TemplatePath};
use clap::Parser;
use include_dir::{include_dir, Dir};
use tempdir::TempDir;

use crate::config::add_member;
use crate::ui::print_success;
use crate::workspace::{agent_dir, config_file_path, load_workspace_config, write_config};

static GUEST_AGENT_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../../wasm/control/cli/templates/agent");

#[derive(Debug, Parser)]
pub struct NewAgentCommand {
    #[arg(long)]
    name: String,
    #[arg(long)]
    uri: String,
}

impl NewAgentCommand {
    pub fn execute(self) -> Result<()> {
        let NewAgentCommand { name, uri } = self;
        let mut config = load_workspace_config()?;

        if !config.add_agent(&name, uri) {
            return Err(anyhow!("Workspace already contains agent: {name}"));
        }

        let temp_dir = TempDir::new("wasm")?;
        let path = temp_dir.path().to_str().map(|s| s.to_string());
        GUEST_AGENT_TEMPLATE.extract(&temp_dir)?;

        let template_path = TemplatePath {
            path,
            ..Default::default()
        };
        let args = GenerateArgs {
            name: Some(name.clone()),
            force: true,
            template_path,
            destination: Some(agent_dir()?),
            ..Default::default()
        };

        generate(args)?;
        write_config(config_file_path()?, &config)?;
        print_success(format!("Created new agent: {name}"));

        add_member(&format!("agents/{name}"))?;

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub enum AgentCommand {
    New(NewAgentCommand),
}

impl AgentCommand {
    pub async fn execute(self) -> Result<()> {
        match self {
            AgentCommand::New(command) => command.execute(),
        }
    }
}
