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

use std::fs;

use anyhow::{anyhow, Result};
use cargo_generate::{generate, GenerateArgs, TemplatePath};
use clap::Parser;
use include_dir::{include_dir, Dir};
use tempdir::TempDir;
use url::Url;

use control_ir::{ConnectorDef, ConnectorProperties, KafkaConnectorDef};

use crate::config::add_member;
use crate::ui::{print_success, print_warn};
use crate::workspace::{
    config_file_path, connectors_dir, load_workspace_config, modules_dir, write_config,
};

static GUEST_CONNECTOR_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../../wasm/control/cli/templates/connector");

#[derive(Debug, Parser)]
pub enum NewConnectorCommand {
    Kafka(NewKafkaConnectorCommand),
}

impl NewConnectorCommand {
    pub async fn execute(self) -> Result<()> {
        match self {
            NewConnectorCommand::Kafka(command) => command.execute().await,
        }
    }
}

#[derive(Debug, Parser)]
pub struct NewKafkaConnectorCommand {
    #[arg(long)]
    name: String,
    #[arg(long)]
    broker: Url,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    group: String,
    #[arg(long)]
    module: String,
}

impl NewKafkaConnectorCommand {
    pub async fn execute(self) -> Result<()> {
        let NewKafkaConnectorCommand {
            name,
            broker,
            topic,
            group,
            module,
        } = self;

        let mut config = load_workspace_config()?;

        if !config.add_connector(&name) {
            return Err(anyhow!("Workspace already contains connector: {name}"));
        } else if config.add_module(&module) {
            print_success(format!("Creating connector module: {module}"));

            let temp_dir = TempDir::new("wasm")?;
            let path = temp_dir.path().to_str().map(|s| s.to_string());
            GUEST_CONNECTOR_TEMPLATE.extract(&temp_dir)?;

            let template_path = TemplatePath {
                path,
                ..Default::default()
            };
            let args = GenerateArgs {
                name: Some(module.clone()),
                force: true,
                template_path,
                destination: Some(modules_dir()?),
                ..Default::default()
            };
            generate(args)?;

            add_member(&format!("connectors/{name}"))?;
            print_success(format!("Created new connector module: {module}"));
        } else {
            print_warn(format!(
                "Connector module '{module}' already exists, linking"
            ));
        }

        write_config(config_file_path()?, &config)?;
        print_success(format!("Created new connector: {name}"));

        let mut connector = connectors_dir()?;
        connector.push(format!("{name}.yml"));

        let spec = ConnectorDef::Kafka(KafkaConnectorDef {
            broker,
            topic,
            group,
            module,
            properties: ConnectorProperties::default(),
        });
        let spec_str = serde_yaml::to_string(&spec)?;

        fs::write(connector, spec_str)?;

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub enum ConnectorCommand {
    #[command(subcommand)]
    New(NewConnectorCommand),
}

impl ConnectorCommand {
    pub async fn execute(self) -> Result<()> {
        match self {
            ConnectorCommand::New(command) => command.execute().await,
        }
    }
}
