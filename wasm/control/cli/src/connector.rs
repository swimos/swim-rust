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
use clap::Parser;
use url::Url;

use control_ir::{ConnectorSpec, KafkaConnectorSpec, Pipe};

use crate::ui::print_success;
use crate::workspace::{config_file_path, connectors_dir, load_workspace_config, write_config};

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
    node: String,
    #[arg(long)]
    lane: String,
}

impl NewKafkaConnectorCommand {
    pub async fn execute(self) -> Result<()> {
        let NewKafkaConnectorCommand {
            name,
            broker,
            topic,
            group,
            node,
            lane,
        } = self;

        let mut config = load_workspace_config()?;

        if !config.add_connector(&name) {
            return Err(anyhow!("Workspace already contains connector: {name}"));
        }

        write_config(config_file_path()?, &config)?;
        print_success(format!("Created new connector: {name}"));

        let mut connector = connectors_dir()?;
        connector.push(format!("{name}.yml"));

        let spec = ConnectorSpec::Kafka(KafkaConnectorSpec {
            broker,
            topic,
            group,
            pipe: Pipe { node, lane },
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
