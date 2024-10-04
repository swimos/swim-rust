// Copyright 2015-2024 Swim Inc.
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

mod ingress;

use fluvio::dataplane::link::ErrorCode;
use fluvio::FluvioError;
use swimos_connector::{LoadError, SelectorError};

/// Errors that can be produced by the Fluvio connector.
#[derive(thiserror::Error, Debug)]
pub enum FluvioConnectorError {
    /// Fluvio Library Error.
    #[error("Fluvio client error: {0}")]
    Native(FluvioError),
    /// Fluvio error code.
    #[error("Fluvio dataplane error: {0}")]
    Code(ErrorCode),
    /// Failed to load the deserializers required to interpret the Fluvio messages.
    #[error("Failed to load deserializer: {0}")]
    Configuration(#[from] LoadError),
    /// Attempting to select the required components of a Fluvio message failed.
    #[error("Failed to select from a message: {0}")]
    Lane(#[from] SelectorError),
    /// String error message.
    #[error("{0}")]
    Message(String),
}
