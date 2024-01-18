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
    #[command(subcommand)]
    Workspace(WorkspaceCommand),
    #[command(subcommand)]
    Agent(AgentCommand),
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
