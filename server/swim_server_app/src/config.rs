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

use std::{
    ffi::OsStr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

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
    /// Configuration for websocket connections.
    pub websockets: WebSocketConfig,
    /// Parameters for the downlink runtime component.
    pub downlink_runtime: DownlinkRuntimeConfig,
    /// Budget for byte stream futures (causes streams with constantly available data to periodically yield).
    pub channel_coop_budget: Option<NonZeroUsize>,
}

pub enum TlsIdentityBody {
    InMemory(Vec<u8>),
    FromFile(PathBuf),
}

pub enum TlsIdentityKind {
    DER,
    PEM,
}

pub struct TlsConfig {
    pub identity_kind: TlsIdentityKind,
    pub body: TlsIdentityBody,
    pub key: Vec<u8>,
}

impl TlsConfig {
    pub fn der(body: Vec<u8>, password: &str) -> Self {
        TlsConfig {
            identity_kind: TlsIdentityKind::DER,
            body: TlsIdentityBody::InMemory(body),
            key: password.as_bytes().to_vec(),
        }
    }

    pub fn der_from_file<N: AsRef<OsStr>>(path: N, password: &str) -> Self {
        TlsConfig {
            identity_kind: TlsIdentityKind::DER,
            body: TlsIdentityBody::FromFile(Path::new(&path).to_owned()),
            key: password.as_bytes().to_vec(),
        }
    }

    pub fn pem(body: Vec<u8>, key: &[u8]) -> Self {
        TlsConfig {
            identity_kind: TlsIdentityKind::PEM,
            body: TlsIdentityBody::InMemory(body),
            key: key.to_vec(),
        }
    }

    pub fn pem_from_file<N: AsRef<OsStr>>(path: N, key: &[u8]) -> Self {
        TlsConfig {
            identity_kind: TlsIdentityKind::DER,
            body: TlsIdentityBody::FromFile(Path::new(&path).to_owned()),
            key: key.to_vec(),
        }
    }
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
const DEFAULT_INIT_TIMEOUT: Duration = Duration::from_secs(1);

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
                attachment_queue_size: DEFAULT_CHANNEL_SIZE,
                inactive_timeout: DEFAULT_TIMEOUT,
                prune_remote_delay: DEFAULT_TIMEOUT,
                shutdown_timeout: DEFAULT_TIMEOUT,
                lane_init_timeout: DEFAULT_INIT_TIMEOUT,
            },
            client_attachment_buffer_size: DEFAULT_CHANNEL_SIZE,
            find_route_channel_size: DEFAULT_CHANNEL_SIZE,
            open_downlink_channel_size: DEFAULT_CHANNEL_SIZE,
            agent_runtime_buffer_size: DEFAULT_BUFFER_SIZE,
            attachment_timeout: DEFAULT_TIMEOUT,
            websockets: Default::default(),
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
