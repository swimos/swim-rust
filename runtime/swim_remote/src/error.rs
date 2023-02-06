// Copyright 2015-2021 Swim Inc.
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

use swim_model::Text;
use thiserror::Error;

/// Error type produced when attempting to resolve a lane on an agent that does
/// not exist (the lane name is kept for producing the response envelope).
#[derive(Debug, Error)]
#[error("Agent '{node}' does not exist.")]
pub struct NoSuchAgent {
    pub node: Text,
    pub lane: Text,
}

/// Error type produced when the resolution of an agent fails.
#[derive(Debug, Error)]
pub enum AgentResolutionError {
    #[error(transparent)]
    NotFound(#[from] NoSuchAgent),
    #[error("The plane is stopping.")]
    PlaneStopping,
}
