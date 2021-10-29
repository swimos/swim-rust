// Copyright 2015-2021 SWIM.AI inc.
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
use std::collections::HashMap;
use std::num::NonZeroUsize;
use swim_form::structural::read::ReadError;
use swim_utilities::future::retryable::strategy::RetryStrategy;
use swim_runtime::configuration::{DownlinkConnectionsConfig, DownlinkConfig, DownlinksConfig};
use swim_runtime::remote::config::RemoteConnectionsConfig;
use swim_model::path::AbsolutePath;
use thiserror::Error;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;
use swim_recon::parser::ParseError;
use swim_model::path::Addressable;

mod recognizers;
mod tags;
#[cfg(test)]
mod tests;
mod writers;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60000);
const DEFAULT_DOWNLINK_BUFFER_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(32) };
const DEFAULT_YIELD_AFTER: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(256) };
const DEFAULT_BUFFER_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(128) };
const DEFAULT_DL_REQUEST_BUFFER_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(8) };
const DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE: NonZeroUsize =
    unsafe { NonZeroUsize::new_unchecked(32) };
const DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE: NonZeroUsize =
    unsafe { NonZeroUsize::new_unchecked(16) };
const DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS: NonZeroUsize =
    unsafe { NonZeroUsize::new_unchecked(16) };
const DEFAULT_BACK_PRESSURE_YIELD_AFTER: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(256) };

/// Configuration for the swim client.
///
/// * `downlink_connections_config` - Configuration parameters for the downlink connections.
/// * `remote_connections_config` - Configuration parameters the remote connections.
/// * `websocket_config` - Configuration parameters the WebSocket connections.
/// * `downlinks_config` - Configuration for the behaviour of downlinks.
#[derive(Clone, Debug, Default)]
pub struct SwimClientConfig {
    pub downlink_connections_config: DownlinkConnectionsConfig,
    pub remote_connections_config: RemoteConnectionsConfig,
    pub websocket_config: WebSocketConfig,
    pub downlinks_config: ClientDownlinksConfig,
}

impl PartialEq<Self> for SwimClientConfig {
    fn eq(&self, other: &Self) -> bool {
        self.downlink_connections_config == other.downlink_connections_config
            && self.remote_connections_config == other.remote_connections_config
            && self.websocket_config.max_send_queue == other.websocket_config.max_send_queue
            && self.websocket_config.max_message_size == other.websocket_config.max_message_size
            && self.websocket_config.max_frame_size == other.websocket_config.max_frame_size
            && self.websocket_config.accept_unmasked_frames
                == other.websocket_config.accept_unmasked_frames
            && match (
                self.websocket_config.compression,
                other.websocket_config.compression,
            ) {
                (WsCompression::None(self_val), WsCompression::None(other_val)) => {
                    self_val == other_val
                }
                (WsCompression::Deflate(self_deflate), WsCompression::Deflate(other_deflate)) => {
                    self_deflate == other_deflate
                }
                _ => false,
            }
            && self.downlinks_config == other.downlinks_config
    }
}

impl SwimClientConfig {
    pub fn new(
        downlink_connections_config: DownlinkConnectionsConfig,
        remote_connections_config: RemoteConnectionsConfig,
        websocket_config: WebSocketConfig,
        downlinks_config: ClientDownlinksConfig,
    ) -> SwimClientConfig {
        SwimClientConfig {
            downlink_connections_config,
            remote_connections_config,
            websocket_config,
            downlinks_config,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ClientDownlinksConfig {
    default: DownlinkConfig,
    by_host: HashMap<Url, DownlinkConfig>,
    by_lane: HashMap<AbsolutePath, DownlinkConfig>,
}

impl ClientDownlinksConfig {
    pub fn new(default: DownlinkConfig) -> ClientDownlinksConfig {
        ClientDownlinksConfig {
            default,
            by_host: HashMap::new(),
            by_lane: HashMap::new(),
        }
    }
}

impl DownlinksConfig for ClientDownlinksConfig {
    type PathType = AbsolutePath;

    fn config_for(&self, path: &Self::PathType) -> DownlinkConfig {
        let ClientDownlinksConfig {
            default,
            by_host,
            by_lane,
            ..
        } = self;
        match by_lane.get(path) {
            Some(config) => *config,
            _ => {
                let maybe_host = path.host();

                match maybe_host {
                    Some(host) => match by_host.get(&host) {
                        Some(config) => *config,
                        _ => *default,
                    },
                    None => *default,
                }
            }
        }
    }

    fn for_host(&mut self, host: Url, params: DownlinkConfig) {
        self.by_host.insert(host, params);
    }

    fn for_lane(&mut self, lane: &AbsolutePath, params: DownlinkConfig) {
        self.by_lane.insert(lane.clone(), params);
    }
}

#[derive(Debug, Error)]
#[error("Could not process client configuration: {0}")]
pub enum ConfigError {
    File(std::io::Error),
    Parse(ParseError),
    Recognizer(ReadError),
}
