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

use tokio::sync::mpsc;
use wasm_ir::connector::ConnectorMessage;
use wasmtime::MemoryAccessError;

pub mod wasm;

pub trait WasmConnectorFactory: Send + Sync + Clone + 'static {
    type Connector: WasmConnector;

    async fn new_connector(
        &self,
        bytes: Vec<u8>,
        channel: mpsc::Sender<ConnectorMessage>,
    ) -> Result<Self::Connector, WasmError>;
}

pub trait WasmConnector: Send + 'static {
    async fn dispatch(&mut self, data: &[u8]) -> Result<(), WasmError>;
}

#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("Wasmtime error: {0}")]
    Runtime(#[from] wasmtime::Error),
    #[error("Module missing export: {0}")]
    MissingExport(String),
    #[error("Memory access error: {0}")]
    Memory(#[from] MemoryAccessError),
    #[error("Malformatted intermediate representation: {0}")]
    Ir(#[from] bincode::Error),
}
