// Copyright 2015-2020 SWIM.AI inc.
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

pub mod router;

pub mod downlink {
    use crate::configuration::router::RouterParams;
    use common::warp::path::AbsolutePath;
    use std::collections::HashMap;
    use std::fmt::{Display, Formatter};
    use std::num::NonZeroUsize;
    use tokio::time::Duration;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum DownlinkKind {
        Value,
        Map,
    }

    impl Display for DownlinkKind {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                DownlinkKind::Value => write!(f, "Value"),
                DownlinkKind::Map => write!(f, "Map"),
            }
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum BackpressureMode {
        /// Propagate back-pressure through the downlink.
        Propagate,
        /// Attempt to relieve back-pressure through the downlink as much as possible.
        Release {
            /// Input queue size for the back-pressure relief component.
            input_buffer_size: usize,
            /// Queue size for control messages between different components of the pressure
            /// relief component. This only applies to map downlinks.
            bridge_buffer_size: usize,
            /// Maximum number of active keys in the pressure relief component for map downlinks.
            max_active_keys: usize,
        },
    }

    /// Configuration for the creation and management of downlinks for a Warp client.
    pub trait Config: Send + Sync {
        /// Get the downlink configuration for a downlink a specific path.
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams;

        /// Get the global parameters for any downlink.
        fn client_params(&self) -> ClientParams;
    }

    /// Multiplexing strategy for the topic of events produced by a downlink.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum MuxMode {
        /// Each consumer has an intermediate queues. If any one of these queues fills the
        /// downlink will block.
        Queue(NonZeroUsize),
        /// Each subscriber to the downlink will see only the most recent event each time it polls.
        /// Subscribers could miss a large proportion of messages.
        Dropping,
        /// All consumers read from a single intermediate queue. If this queue fills the oldest
        /// values will be discarded. Lagging consumers could miss messages.
        Buffered(NonZeroUsize),
    }

    /// Instruction on how to repsond when an invalid message is received for a downlink.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum OnInvalidMessage {
        /// Disregard the message and continue.
        Ignore,

        /// Terminate the downlink.
        Terminate,
    }

    /// Configuration parameters for a single downlink.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct DownlinkParams {
        /// Whether the downlink propagates back-pressure.
        pub back_pressure: BackpressureMode,

        /// Multiplexing mode for the downlink.
        pub mux_mode: MuxMode,

        /// Timeout after which an idle downlink will be closed (not yet implemented).
        pub idle_timeout: Duration,

        /// Buffer size for local actions performed on the downlink.
        pub buffer_size: NonZeroUsize,

        /// What do do on receipt of an invalid message.
        pub on_invalid: OnInvalidMessage,
    }

    impl DownlinkParams {
        pub fn new(
            back_pressure: BackpressureMode,
            mux_mode: MuxMode,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
        ) -> Result<DownlinkParams, String> {
            if idle_timeout == Duration::from_millis(0) {
                Err(BAD_TIMEOUT.to_string())
            } else {
                match NonZeroUsize::new(buffer_size) {
                    Some(nz) => Ok(DownlinkParams {
                        back_pressure,
                        mux_mode,
                        idle_timeout,
                        buffer_size: nz,
                        on_invalid,
                    }),
                    _ => Err(BAD_BUFFER_SIZE.to_string()),
                }
            }
        }

        pub fn new_queue(
            back_pressure: BackpressureMode,
            queue_size: usize,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
        ) -> Result<DownlinkParams, String> {
            match NonZeroUsize::new(queue_size) {
                Some(nz) => Self::new(
                    back_pressure,
                    MuxMode::Queue(nz),
                    idle_timeout,
                    buffer_size,
                    on_invalid,
                ),
                _ => Err(BAD_BUFFER_SIZE.to_string()),
            }
        }

        pub fn new_dropping(
            back_pressure: BackpressureMode,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
        ) -> Result<DownlinkParams, String> {
            Self::new(
                back_pressure,
                MuxMode::Dropping,
                idle_timeout,
                buffer_size,
                on_invalid,
            )
        }

        pub fn new_buffered(
            back_pressure: BackpressureMode,
            queue_size: usize,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
        ) -> Result<DownlinkParams, String> {
            match NonZeroUsize::new(queue_size) {
                Some(nz) => Self::new(
                    back_pressure,
                    MuxMode::Buffered(nz),
                    idle_timeout,
                    buffer_size,
                    on_invalid,
                ),
                _ => Err(BAD_BUFFER_SIZE.to_string()),
            }
        }
    }

    /// Configuration parameters for all downlinks.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct ClientParams {
        /// Buffer size for servicing requests for new downlinks.
        pub dl_req_buffer_size: NonZeroUsize,

        pub router_params: RouterParams,
    }

    const BAD_BUFFER_SIZE: &str = "Buffer sizes must be positive.";
    const BAD_TIMEOUT: &str = "Timeout must be positive.";

    impl ClientParams {
        pub fn new(
            dl_req_buffer_size: usize,
            router_params: RouterParams,
        ) -> Result<ClientParams, String> {
            match NonZeroUsize::new(dl_req_buffer_size) {
                Some(nz) => Ok(ClientParams {
                    dl_req_buffer_size: nz,
                    router_params,
                }),
                _ => Err(BAD_BUFFER_SIZE.to_string()),
            }
        }
    }

    /// Basic [`Config`] implementation which allows for configuration to be specified by absolute
    /// path or host and provides a default fallback.
    #[derive(Clone, Debug)]
    pub struct ConfigHierarchy {
        client_params: ClientParams,
        default: DownlinkParams,
        by_host: HashMap<url::Url, DownlinkParams>,
        by_lane: HashMap<AbsolutePath, DownlinkParams>,
    }

    impl ConfigHierarchy {
        /// Create a new configuration store with just a default.
        pub fn new(client_params: ClientParams, default: DownlinkParams) -> ConfigHierarchy {
            ConfigHierarchy {
                client_params,
                default,
                by_host: HashMap::new(),
                by_lane: HashMap::new(),
            }
        }

        /// Add specific configuration for a host.
        pub fn for_host(&mut self, host: url::Url, params: DownlinkParams) {
            self.by_host.insert(host, params);
        }

        /// Add specific configuration for an absolute path (this will override host level
        /// configuration).
        pub fn for_lane(&mut self, lane: &AbsolutePath, params: DownlinkParams) {
            self.by_lane.insert(lane.clone(), params);
        }
    }

    impl Config for ConfigHierarchy {
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams {
            let ConfigHierarchy {
                default,
                by_host,
                by_lane,
                ..
            } = self;
            match by_lane.get(path) {
                Some(params) => *params,
                _ => match by_host.get(&path.host) {
                    Some(params) => *params,
                    _ => *default,
                },
            }
        }

        fn client_params(&self) -> ClientParams {
            self.client_params
        }
    }

    impl<'a> Config for Box<dyn Config + 'a> {
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams {
            (**self).config_for(path)
        }

        fn client_params(&self) -> ClientParams {
            (**self).client_params()
        }
    }
}
