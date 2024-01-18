mod connectors;
pub mod endpoints;

pub use connectors::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[derive(Serialize, Deserialize)]
pub struct AgentSpec {
    pub route: String,
    pub module: Vec<u8>,
}

impl Debug for AgentSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let AgentSpec { route, .. } = self;
        f.debug_struct("AgentSpec")
            .field("route", route)
            .field("module", &"..")
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeploySpec {
    pub name: String,
    pub port: usize,
    pub agents: HashMap<String, AgentSpec>,
    pub connectors: HashMap<String, ConnectorSpec>,
}
