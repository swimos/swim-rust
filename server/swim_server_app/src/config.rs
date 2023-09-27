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

use std::{num::NonZeroUsize, time::Duration};

use ratchet::WebSocketConfig;
use swim_api::agent::AgentConfig;
use swim_runtime::{agent::AgentRuntimeConfig, downlink::DownlinkRuntimeConfig};
use swim_utilities::non_zero_usize;

/// Configuration parameters for a Swim server.
#[derive(Debug, Clone, Copy)]
pub struct SwimServerConfig {
    /// Parameters for remote sockets.
    pub remote: RemoteConnectionsConfig,
    /// Parameters to be passed to agents.
    pub agent: AgentConfig,
    /// Parameters for the agent runtime component.
    pub agent_runtime: AgentRuntimeConfig,
    /// Size of the MPSC channel for requesting new downlinks from a remote.
    pub client_attachment_buffer_size: NonZeroUsize,
    /// Size of the MPSC channel for requesting new downlinks to the server.
    pub client_request_channel_size: NonZeroUsize,
    /// Size of the MPSC channel for resolving agents.
    pub find_route_channel_size: NonZeroUsize,
    /// Size of the MPSC channel for opening new downlinks.
    pub open_downlink_channel_size: NonZeroUsize,
    /// The buffer size for communication between remote sockets and agents.
    pub agent_runtime_buffer_size: NonZeroUsize,
    /// Timeout on attempting to connect a remote socket to an agent.
    pub attachment_timeout: Duration,
    /// HTTP server parameters.
    pub http: HttpConfig,
    /// Parameters for the downlink runtime component.
    pub downlink_runtime: DownlinkRuntimeConfig,
    /// Budget for byte stream futures (causes streams with constantly available data to periodically yield).
    pub channel_coop_budget: Option<NonZeroUsize>,
}

#[derive(Debug, Clone, Copy)]
pub struct HttpConfig {
    /// Configuration for websocket connections.
    pub websockets: WebSocketConfig,
    /// Maximum number of concurrent HTTP requests to serve.
    pub max_http_requests: NonZeroUsize,
    /// HTTP request timeout.
    pub http_request_timeout: Duration,
    /// Period of inactivity after which the HTTP server will drop channels to agents.
    pub resolver_timeout: Duration,
}

/// Configuration for remote socket management.
#[derive(Debug, Clone, Copy)]
pub struct RemoteConnectionsConfig {
    /// Size of the MPSC channel used to register agents with the socket.
    pub registration_buffer_size: NonZeroUsize,
    /// Time to wait for a websocket to close before giving up.
    pub close_timeout: Duration,
}

const DEFAULT_CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(16);
const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HTTP_RESOLVER_TIMEOUT: Duration = Duration::from_secs(60 * 5);
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_HTTP: NonZeroUsize = non_zero_usize!(1024);

impl Default for RemoteConnectionsConfig {
    fn default() -> Self {
        Self {
            registration_buffer_size: DEFAULT_CHANNEL_SIZE,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
        }
    }
}

impl Default for SwimServerConfig {
    fn default() -> Self {
        Self {
            remote: Default::default(),
            agent: Default::default(),
            agent_runtime: AgentRuntimeConfig::default(),
            client_attachment_buffer_size: DEFAULT_CHANNEL_SIZE,
            find_route_channel_size: DEFAULT_CHANNEL_SIZE,
            open_downlink_channel_size: DEFAULT_CHANNEL_SIZE,
            agent_runtime_buffer_size: DEFAULT_BUFFER_SIZE,
            attachment_timeout: DEFAULT_TIMEOUT,
            http: HttpConfig::default(),
            downlink_runtime: DownlinkRuntimeConfig {
                empty_timeout: DEFAULT_TIMEOUT,
                attachment_queue_size: DEFAULT_CHANNEL_SIZE,
                abort_on_bad_frames: true,
                remote_buffer_size: DEFAULT_BUFFER_SIZE,
                downlink_buffer_size: DEFAULT_BUFFER_SIZE,
            },
            client_request_channel_size: DEFAULT_CHANNEL_SIZE,
            channel_coop_budget: None,
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            websockets: Default::default(),
            max_http_requests: DEFAULT_MAX_HTTP,
            http_request_timeout: DEFAULT_HTTP_TIMEOUT,
            resolver_timeout: DEFAULT_HTTP_RESOLVER_TIMEOUT,
        }
    }
}
