use anyhow::Result;
use clap::Parser;

use crate::agent::AgentCommand;
use crate::connector::ConnectorCommand;
use crate::workspace::WorkspaceCommand;

mod agent;
mod config;
mod connector;
mod ui;
mod workspace;

#[tokio::main]
async fn main() -> Result<()> {
    let command = Command::parse();
    command.execute().await
}

#[derive(Debug, Parser)]
enum Command {
    /// Execute a command at the workspace level.
    #[command(subcommand)]
    Workspace(WorkspaceCommand),
    /// Execute a command at a agent level.
    #[command(subcommand)]
    Agent(AgentCommand),
    /// Execute a command at a connector level.
    #[command(subcommand)]
    Connector(ConnectorCommand),
}

impl Command {
    async fn execute(self) -> Result<()> {
        match self {
            Command::Workspace(command) => command.execute().await,
            Command::Agent(command) => command.execute().await,
            Command::Connector(command) => command.execute().await,
        }
    }
}
