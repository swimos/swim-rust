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

use std::{num::NonZeroUsize, time::Duration};

use swim_api::agent::AgentConfig;
use swim_runtime::agent::AgentRuntimeConfig;

#[derive(Debug, Clone, Copy)]
pub struct SwimServerConfig {
    pub remote: RemoteConnectionsConfig,
    pub agent: AgentConfig,
    pub agent_runtime: AgentRuntimeConfig,
    pub client_attachment_buffer_size: NonZeroUsize,
    pub find_route_buffer_size: NonZeroUsize,
    pub agent_runtime_buffer_size: NonZeroUsize,
    pub attachment_timeout: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteConnectionsConfig {
    pub registration_buffer_size: NonZeroUsize,
}
