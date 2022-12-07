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

use ratchet::WebSocketConfig;
use swim_api::agent::AgentConfig;
use swim_runtime::agent::AgentRuntimeConfig;
use swim_utilities::algebra::non_zero_usize;

/// Configuration parameters for a Swim server.
#[derive(Debug, Clone, Copy)]
pub struct SwimServerConfig {
    /// Parameters for remote sockets.
    pub remote: RemoteConnectionsConfig,
    /// Parameters to be passed to agents.
    pub agent: AgentConfig,
    /// Parameters for the agent runtime component.
    pub agent_runtime: AgentRuntimeConfig,
    /// Size of the MPSC channel for requesting new downlinks.
    pub client_attachment_buffer_size: NonZeroUsize,
    /// Size of the MPSC channel for resolving agents.
    pub find_route_buffer_size: NonZeroUsize,
    /// The buffer size for communication between remote sockets and agents.
    pub agent_runtime_buffer_size: NonZeroUsize,
    /// Timeout on attempting to connect a remote socket to an agent.
    pub attachment_timeout: Duration,
    /// Configuration for websocket connections.
    pub websockets: WebSocketConfig,
}

/// Configuration for remote socket management.
#[derive(Debug, Clone, Copy)]
pub struct RemoteConnectionsConfig {
    /// Size of the MPSC channel used to register agents with the socket.
    pub registration_buffer_size: NonZeroUsize,
}

const DEFAULT_CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(16);
const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

impl Default for RemoteConnectionsConfig {
    fn default() -> Self {
        Self {
            registration_buffer_size: DEFAULT_CHANNEL_SIZE,
        }
    }
}

impl Default for SwimServerConfig {
    fn default() -> Self {
        Self {
            remote: Default::default(),
            agent: Default::default(),
            agent_runtime: AgentRuntimeConfig {
                default_lane_config: Default::default(),
                attachment_queue_size: DEFAULT_CHANNEL_SIZE,
                inactive_timeout: DEFAULT_TIMEOUT,
                prune_remote_delay: DEFAULT_TIMEOUT,
                shutdown_timeout: DEFAULT_TIMEOUT,
            },
            client_attachment_buffer_size: DEFAULT_CHANNEL_SIZE,
            find_route_buffer_size: DEFAULT_CHANNEL_SIZE,
            agent_runtime_buffer_size: DEFAULT_BUFFER_SIZE,
            attachment_timeout: DEFAULT_TIMEOUT,
            websockets: Default::default(),
        }
    }
}
