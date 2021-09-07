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
use swim_common::model::text::Text;
use swim_common::routing::remote::config::RemoteConnectionsConfig;
use swim_common::warp::path::{AbsolutePath, Addressable};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;
use utilities::future::retryable::strategy::RetryStrategy;

//Todo dm this needs to be changed after config from file is done.
// #[cfg(test)]
// mod tests;

const BAD_BUFFER_SIZE: &str = "Buffer sizes must be positive.";
const BAD_YIELD_AFTER: &str = "Yield after count must be positive..";
const BAD_TIMEOUT: &str = "Timeout must be positive.";

const DEFAULT_BACK_PRESSURE: BackpressureMode = BackpressureMode::Propagate;
const DEFAULT_IDLE_TIMEOUT: u64 = 60000;
const DEFAULT_DOWNLINK_BUFFER_SIZE: usize = 5;
const DEFAULT_ON_INVALID: OnInvalidMessage = OnInvalidMessage::Terminate;
const DEFAULT_YIELD_AFTER: usize = 256;
const DEFAULT_BUFFER_SIZE: usize = 100;
const DEFAULT_DL_REQUEST_BUFFER_SIZE: usize = 8;

#[derive(Clone, Debug)]
pub struct SwimClientConfig {
    /// Configuration parameters for the downlink connections.
    pub downlink_connections_config: DownlinkConnectionsConfig,
    /// Configuration parameters the remote connections.
    pub remote_connections_config: RemoteConnectionsConfig,
    /// Configuration parameters the WebSocket connections.
    pub websocket_config: WebSocketConfig,
    /// Configuration for the behaviour of downlinks.
    pub downlinks_config: ClientDownlinksConfig,
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

impl Default for SwimClientConfig {
    fn default() -> Self {
        SwimClientConfig::new(
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }
}

/// Configuration parameters for the router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DownlinkConnectionsConfig {
    /// Buffer size for servicing requests for new downlinks.
    pub dl_req_buffer_size: NonZeroUsize,
    /// Size of the internal buffers of the router.
    pub buffer_size: NonZeroUsize,
    /// Number of values to process before yielding to the runtime.
    pub yield_after: NonZeroUsize,
    /// The retry strategy that will be used when attempting to make a request to a Web Agent.
    pub retry_strategy: RetryStrategy,
}

impl DownlinkConnectionsConfig {
    pub fn new(
        dl_req_buffer_size: NonZeroUsize,
        buffer_size: NonZeroUsize,
        yield_after: NonZeroUsize,
        retry_strategy: RetryStrategy,
    ) -> DownlinkConnectionsConfig {
        DownlinkConnectionsConfig {
            dl_req_buffer_size,
            buffer_size,
            yield_after,
            retry_strategy,
        }
    }
}

impl Default for DownlinkConnectionsConfig {
    fn default() -> Self {
        DownlinkConnectionsConfig {
            dl_req_buffer_size: NonZeroUsize::new(DEFAULT_DL_REQUEST_BUFFER_SIZE).unwrap(),
            retry_strategy: RetryStrategy::default(),
            buffer_size: NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap(),
            yield_after: NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientDownlinksConfig {
    default: DownlinkConfig,
    by_host: HashMap<Url, DownlinkConfig>,
    by_lane: HashMap<Text, DownlinkConfig>,
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
        match by_lane.get(path.lane().as_str()) {
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

    fn for_lane(&mut self, lane: &Text, params: DownlinkConfig) {
        self.by_lane.insert(lane.clone(), params);
    }
}

impl Default for ClientDownlinksConfig {
    fn default() -> Self {
        ClientDownlinksConfig::new(Default::default())
    }
}

/// Configuration parameters for a single downlink.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DownlinkConfig {
    /// Whether the downlink propagates back-pressure.
    pub back_pressure: BackpressureMode,
    /// Timeout after which an idle downlink will be closed (not yet implemented).
    pub idle_timeout: Duration,
    /// Buffer size for local actions performed on the downlink.
    pub buffer_size: NonZeroUsize,
    /// What do do on receipt of an invalid message.
    pub on_invalid: OnInvalidMessage,
    /// Number of operations after which a downlink will yield to the runtime.
    pub yield_after: NonZeroUsize,
}

impl DownlinkConfig {
    pub fn new(
        back_pressure: BackpressureMode,
        idle_timeout: Duration,
        buffer_size: usize,
        on_invalid: OnInvalidMessage,
        yield_after: usize,
    ) -> Result<DownlinkConfig, String> {
        if idle_timeout == Duration::from_millis(0) {
            Err(BAD_TIMEOUT.to_string())
        } else {
            match (
                NonZeroUsize::new(buffer_size),
                NonZeroUsize::new(yield_after),
            ) {
                (Some(nz), Some(ya)) => Ok(DownlinkConfig {
                    back_pressure,
                    idle_timeout,
                    buffer_size: nz,
                    on_invalid,
                    yield_after: ya,
                }),
                (None, _) => Err(BAD_BUFFER_SIZE.to_string()),
                _ => Err(BAD_YIELD_AFTER.to_string()),
            }
        }
    }
}

impl From<&DownlinkConfig> for DownlinkConfig {
    fn from(conf: &DownlinkConfig) -> Self {
        DownlinkConfig {
            back_pressure: conf.back_pressure,
            idle_timeout: conf.idle_timeout,
            buffer_size: conf.buffer_size,
            yield_after: conf.yield_after,
            on_invalid: conf.on_invalid,
        }
    }
}

impl Default for DownlinkConfig {
    fn default() -> Self {
        DownlinkConfig::new(
            DEFAULT_BACK_PRESSURE,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT),
            DEFAULT_DOWNLINK_BUFFER_SIZE,
            DEFAULT_ON_INVALID,
            DEFAULT_YIELD_AFTER,
        )
        .unwrap()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BackpressureMode {
    /// Propagate back-pressure through the downlink.
    Propagate,
    /// Attempt to relieve back-pressure through the downlink as much as possible.
    Release {
        /// Input queue size for the back-pressure relief component.
        input_buffer_size: NonZeroUsize,
        /// Queue size for control messages between different components of the pressure
        /// relief component. This only applies to map downlinks.
        bridge_buffer_size: NonZeroUsize,
        /// Maximum number of active keys in the pressure relief component for map downlinks.
        max_active_keys: NonZeroUsize,
        /// Number of values to process before yielding to the runtime.
        yield_after: NonZeroUsize,
    },
}

/// Instruction on how to respond when an invalid message is received for a downlink.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnInvalidMessage {
    /// Disregard the message and continue.
    Ignore,
    /// Terminate the downlink.
    Terminate,
}

/// Configuration for the creation and management of downlinks for a Warp client.
pub trait DownlinksConfig: Send + Sync {
    type PathType: Addressable;

    /// Get the downlink configuration for a downlink for a specific path.
    fn config_for(&self, path: &Self::PathType) -> DownlinkConfig;

    /// Add specific configuration for a host.
    fn for_host(&mut self, host: Url, params: DownlinkConfig);

    /// Add specific configuration for an absolute path (this will override host level
    /// configuration).
    fn for_lane(&mut self, lane: &Text, params: DownlinkConfig);
}

impl<'a, Path: Addressable> DownlinksConfig for Box<dyn DownlinksConfig<PathType = Path> + 'a> {
    type PathType = Path;

    fn config_for(&self, path: &Self::PathType) -> DownlinkConfig {
        (**self).config_for(path)
    }

    fn for_host(&mut self, host: Url, params: DownlinkConfig) {
        (**self).for_host(host, params)
    }

    fn for_lane(&mut self, lane: &Text, params: DownlinkConfig) {
        (**self).for_lane(lane, params)
    }
}
