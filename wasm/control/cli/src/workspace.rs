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

use std::collections::{HashMap, HashSet};
use std::env::current_dir;
use std::fs;
use std::fs::read;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::connector::link_module;
use crate::ui::{print_error, print_success};
use control_ir::{AgentSpec, ConnectorDef, DeploySpec};
use wasm_compiler::{install_wasm_target, ReleaseMode};

const AGENTS_DIR_NAME: &str = "agents";
const CONNECTORS_DIR_NAME: &str = "connectors";
const MODULES_DIR_NAME: &str = "modules";
pub const CARGO_FILE_NAME: &str = "Cargo.toml";
const CONFIG_FILE_NAME: &str = "Swim.toml";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    remote: Url,
    deploy_port: usize,
    connectors: HashSet<String>,
    modules: HashSet<String>,
    // name -> uri
    agents: HashMap<String, String>,
}

impl WorkspaceConfig {
    pub fn add_agent(&mut self, name: &String, uri: String) -> bool {
        if !self.agents.contains_key(name) {
            self.agents.insert(name.clone(), uri);
            true
        } else {
            false
        }
    }

    pub fn add_connector(&mut self, name: &String) -> bool {
        if !self.connectors.contains(name) {
            self.connectors.insert(name.clone());
            true
        } else {
            false
        }
    }

    pub fn add_module(&mut self, name: &String) -> bool {
        if !self.modules.contains(name) {
            self.modules.insert(name.clone());
            true
        } else {
            false
        }
    }

    pub fn new(remote: Url, deploy_port: usize) -> WorkspaceConfig {
        WorkspaceConfig {
            remote,
            deploy_port,
            connectors: HashSet::default(),
            modules: Default::default(),
            agents: HashMap::default(),
        }
    }
}

pub fn load_workspace_config() -> Result<WorkspaceConfig> {
    match load_config() {
        Ok(config) => Ok(config),
        Err(e) => Err(anyhow!("Failed to load workspace: {e}")),
    }
}

pub fn config_file_path() -> Result<PathBuf> {
    let mut path = current_dir()?;
    path.push(CONFIG_FILE_NAME);
    Ok(path)
}

pub fn agent_dir() -> Result<PathBuf> {
    let mut path = current_dir()?;
    path.push(AGENTS_DIR_NAME);
    Ok(path)
}

pub fn modules_dir() -> Result<PathBuf> {
    let mut path = current_dir()?;
    path.push(MODULES_DIR_NAME);
    Ok(path)
}

pub fn connectors_dir() -> Result<PathBuf> {
    let mut path = current_dir()?;
    path.push(CONNECTORS_DIR_NAME);
    Ok(path)
}

fn load_config() -> Result<WorkspaceConfig> {
    let config = fs::read_to_string(config_file_path()?)?;
    let config = toml::from_str(config.as_str())?;

    Ok(config)
}

pub fn write_config(workspace_file: PathBuf, config: &WorkspaceConfig) -> Result<()> {
    let config_str = toml::to_string(config).expect("Failed to serialize config");
    fs::write(workspace_file, config_str)
        .with_context(|| "Failed to write config file. The workspace may be corrupted")?;
    Ok(())
}

#[derive(Debug, Args)]
pub struct NewWorkspaceCommand {
    /// The name of the workspace. A folder with this name must not exist in the current directory.
    #[arg(long)]
    name: String,
    /// The port that this workspace will be deployed on at the remote server.
    #[arg(long)]
    port: usize,
    /// The remote server's URL.
    #[arg(long)]
    remote: Url,
}

impl NewWorkspaceCommand {
    pub fn execute(self) -> Result<()> {
        let NewWorkspaceCommand { name, port, remote } = self;
        let mut workspace = current_dir()?;
        workspace.push(&name);

        if workspace.exists() {
            return Err(anyhow!("Directory already exists"));
        }

        let mut connectors = workspace.clone();
        connectors.push(CONNECTORS_DIR_NAME);

        let mut agents = workspace.clone();
        agents.push(AGENTS_DIR_NAME);

        let mut modules = workspace.clone();
        modules.push(MODULES_DIR_NAME);

        fs::create_dir_all(connectors)?;
        fs::create_dir_all(agents)?;
        fs::create_dir_all(modules)?;

        let mut config_path = workspace.clone();
        config_path.push(CONFIG_FILE_NAME);

        let config = WorkspaceConfig::new(remote, port);
        write_config(config_path, &config)?;

        let mut cargo_file_path = workspace.clone();
        cargo_file_path.push(CARGO_FILE_NAME);

        let cargo_contents = r#"[workspace]
resolver = "2"
members = []

[workspace.dependencies]
swim_wasm_guest = { path = "../../guest/swim_wasm_guest" }
swim_wasm_connector = { path = "../../guest/swim_wasm_connector" }
swim_utilities = { path = "../../../swim_utilities" }
bincode = "1.3.3""#;
        fs::write(cargo_file_path, cargo_contents)?;

        print_success(format!("Created new workspace: {name}"));

        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct DeployCommand;

impl DeployCommand {
    async fn execute(self) -> Result<()> {
        let WorkspaceConfig {
            remote,
            deploy_port,
            connectors,
            agents,
            modules,
        } = load_workspace_config()?;

        install_wasm_target().await?;

        if agents.is_empty() {
            return Err(anyhow!("Workspace contains no agents"));
        }

        let dir = current_dir()?;

        print_success("Building workspace...");
        print_success("Compiling agents...");

        let mut target_dir = dir.clone();
        target_dir.push("target/wasm32-unknown-unknown/release");

        let mut linked_agents = HashMap::new();

        for (name, uri) in agents {
            let mut agent_dir = dir.clone();
            agent_dir.push(format!("agents/{name}"));

            print_success(format!("\tCompiling agent: {name}"));

            let project = wasm_compiler::Project::new(agent_dir, vec![], ReleaseMode::Release);
            wasm_compiler::compile(project).await?;

            let mut agent_file = target_dir.clone();
            agent_file.push(format!("{name}.wasm"));

            print_success(format!("\tCompiled agent: {name}"));

            let module = read(agent_file)?;
            linked_agents.insert(name, AgentSpec { route: uri, module });
        }

        for module in &modules {
            print_success(format!("\tCompiling module: {module}"));

            let mut module_dir = dir.clone();
            module_dir.push(format!("modules/{module}"));

            let project = wasm_compiler::Project::new(module_dir, vec![], ReleaseMode::Release);
            wasm_compiler::compile(project).await?;

            print_success(format!("\tCompiled module: {module}"));
        }

        print_success("Compiled agents");

        let mut linked_connectors = HashMap::new();
        let connectors_dir = connectors_dir()?;

        print_success("Linking connectors...");

        for connector in connectors {
            print_success(format!("\tLinking connector: {connector}"));

            let mut connector_file = connectors_dir.clone();
            connector_file.push(format!("{connector}.yml"));

            let connector_str = fs::read_to_string(connector_file)?;
            let def = serde_yaml::from_str::<ConnectorDef>(&connector_str)?;
            let spec = link_module(&modules, def, target_dir.clone())?;

            print_success(format!("\tLinked connector: {connector}"));

            linked_connectors.insert(connector, spec);
        }

        print_success("Linked connectors");

        let command = DeploySpec {
            name: dir.iter().last().unwrap().to_string_lossy().to_string(),
            port: deploy_port,
            agents: linked_agents,
            connectors: linked_connectors,
        };

        let spec_str = serde_json::to_string(&command)?;
        let client = reqwest::Client::new();

        print_success(format!("Deploying workspace"));

        let deploy_url = remote.join(control_ir::endpoints::DEPLOY_WORKSPACE)?;
        let response = client.post(deploy_url).body(spec_str).send().await?;

        if response.status().is_success() {
            print_success("Successfully deployed workspace");
        } else {
            print_error(format!("Failed to deploy workspace: {}", response.status()));
        }

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub enum WorkspaceCommand {
    /// Create a new workspace.
    New(NewWorkspaceCommand),
    /// Deploy the current workspace.
    Deploy(DeployCommand),
}

impl WorkspaceCommand {
    pub async fn execute(self) -> Result<()> {
        match self {
            WorkspaceCommand::New(command) => command.execute(),
            WorkspaceCommand::Deploy(command) => command.execute().await,
        }
    }
}
