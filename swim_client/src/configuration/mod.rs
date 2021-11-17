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
use std::collections::HashMap;

use swim_form::structural::read::ReadError;
use swim_model::path::AbsolutePath;
use swim_model::path::Addressable;
use swim_recon::parser::ParseError;
use swim_runtime::configuration::{
    DownlinkConfig, DownlinkConnectionsConfig, DownlinksConfig, WebSocketConfig,
};
use swim_runtime::remote::config::RemoteConnectionsConfig;

use thiserror::Error;
use url::Url;

mod recognizers;
mod tags;
#[cfg(test)]
mod tests;
mod writers;

/// Configuration for the swim client.
///
/// * `downlink_connections_config` - Configuration parameters for the downlink connections.
/// * `remote_connections_config` - Configuration parameters the remote connections.
/// * `websocket_config` - Configuration parameters the WebSocket connections.
/// * `downlinks_config` - Configuration for the behaviour of downlinks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SwimClientConfig {
    pub downlink_connections_config: DownlinkConnectionsConfig,
    pub remote_connections_config: RemoteConnectionsConfig,
    pub websocket_config: WebSocketConfig,
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
