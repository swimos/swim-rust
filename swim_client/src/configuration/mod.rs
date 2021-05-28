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

//Todo dm this needs to be changed after the new client configuration is finalised.
#![allow(warnings, unused)]

pub mod router;

// Todo dm
// #[cfg(test)]
// mod tests;

pub mod downlink {
    use crate::configuration::downlink::ConfigParseError::UnexpectedValue;
    use crate::configuration::router::{ConnectionPoolParams, RouterParams};
    use std::collections::HashMap;
    use std::error::Error;
    use std::fmt::{Display, Formatter};
    use std::num::NonZeroUsize;
    use swim_common::form::Form;
    use swim_common::model::parser::ParseFailure;
    use swim_common::model::{Attr, Item, Value};
    use swim_common::routing::remote::config::ConnectionConfig;
    use swim_common::warp::path::{AbsolutePath, Addressable};
    use tokio::time::Duration;
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    use url::Url;

    const CONFIG_TAG: &str = "config";
    const CLIENT_TAG: &str = "client";
    const DOWNLINKS_TAG: &str = "downlinks";
    const PATH_TAG: &str = "path";
    const HOST_TAG: &str = "host";
    const LANE_TAG: &str = "lane";
    const NODE_TAG: &str = "node";
    const BUFFER_SIZE_TAG: &str = "buffer_size";
    pub const ROUTER_TAG: &str = "router";

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum DownlinkKind {
        Value,
        Map,
        Command,
    }

    impl Display for DownlinkKind {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                DownlinkKind::Value => write!(f, "Value"),
                DownlinkKind::Map => write!(f, "Map"),
                DownlinkKind::Command => write!(f, "Command"),
            }
        }
    }

    const PROPAGATE_TAG: &str = "propagate";
    const RELEASE_TAG: &str = "release";

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

    impl BackpressureMode {
        fn try_from_value(value: Value, use_defaults: bool) -> Result<Self, ConfigParseError> {
            match value {
                Value::Record(mut attrs, items) if attrs.len() <= 1 => {
                    if let Some(Attr { name, value }) = attrs.pop() {
                        match name.as_str() {
                            PROPAGATE_TAG => {
                                if let Value::Extant = value {
                                    Ok(BackpressureMode::Propagate)
                                } else {
                                    Err(UnexpectedValue(value, Some(PROPAGATE_TAG)))
                                }
                            }
                            RELEASE_TAG => {
                                if let Value::Record(_, items) = value {
                                    try_release_mode_from_items(items, use_defaults)
                                } else {
                                    Err(ConfigParseError::UnexpectedValue(value, Some(RELEASE_TAG)))
                                }
                            }
                            _ => Err(ConfigParseError::UnexpectedAttribute(
                                name.to_string(),
                                Some(BACK_PRESSURE_TAG),
                            )),
                        }
                    } else {
                        Err(ConfigParseError::UnnamedRecord(
                            Value::Record(attrs, items),
                            Some(BACK_PRESSURE_TAG),
                        ))
                    }
                }
                _ => Err(ConfigParseError::InvalidValue(value, BACK_PRESSURE_TAG)),
            }
        }
    }

    const INPUT_BUFFER_SIZE_TAG: &str = "input_buffer_size";
    const BRIDGE_BUFFER_SIZE_TAG: &str = "bridge_buffer_size";
    const MAX_ACTIVE_KEYS_TAG: &str = "max_active_keys";

    const DEFAULT_INPUT_BUFFER_SIZE: usize = 5;
    const DEFAULT_BRIDGE_BUFFER_SIZE: usize = 5;
    const DEFAULT_MAX_ACTIVE_KEYS: usize = 20;

    fn try_release_mode_from_items(
        items: Vec<Item>,
        use_defaults: bool,
    ) -> Result<BackpressureMode, ConfigParseError> {
        let mut input_buffer_size: Option<NonZeroUsize> = None;
        let mut bridge_buffer_size: Option<NonZeroUsize> = None;
        let mut max_active_keys: Option<NonZeroUsize> = None;
        let mut yield_after: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    INPUT_BUFFER_SIZE_TAG => {
                        let size = usize::try_from_value(&value).map_err(|_| {
                            ConfigParseError::InvalidValue(value, INPUT_BUFFER_SIZE_TAG)
                        })?;
                        input_buffer_size = Some(NonZeroUsize::new(size).unwrap());
                    }

                    BRIDGE_BUFFER_SIZE_TAG => {
                        let size = usize::try_from_value(&value).map_err(|_| {
                            ConfigParseError::InvalidValue(value, BRIDGE_BUFFER_SIZE_TAG)
                        })?;
                        bridge_buffer_size = Some(NonZeroUsize::new(size).unwrap());
                    }

                    MAX_ACTIVE_KEYS_TAG => {
                        let size = usize::try_from_value(&value).map_err(|_| {
                            ConfigParseError::InvalidValue(value, MAX_ACTIVE_KEYS_TAG)
                        })?;
                        max_active_keys = Some(NonZeroUsize::new(size).unwrap());
                    }

                    YIELD_AFTER_TAG => {
                        let size = usize::try_from_value(&value)
                            .map_err(|_| ConfigParseError::InvalidValue(value, YIELD_AFTER_TAG))?;
                        yield_after = Some(NonZeroUsize::new(size).unwrap());
                    }

                    _ => {
                        return Err(ConfigParseError::UnexpectedKey(
                            name.to_string(),
                            RELEASE_TAG,
                        ))
                    }
                },

                Item::Slot(value, _) => {
                    return Err(ConfigParseError::UnexpectedValue(value, Some(RELEASE_TAG)))
                }
                Item::ValueItem(value) => {
                    return Err(ConfigParseError::UnexpectedValue(value, Some(RELEASE_TAG)))
                }
            };
        }

        if use_defaults {
            input_buffer_size = input_buffer_size
                .or_else(|| Some(NonZeroUsize::new(DEFAULT_INPUT_BUFFER_SIZE).unwrap()));
            bridge_buffer_size = bridge_buffer_size
                .or_else(|| Some(NonZeroUsize::new(DEFAULT_BRIDGE_BUFFER_SIZE).unwrap()));
            max_active_keys = max_active_keys
                .or_else(|| Some(NonZeroUsize::new(DEFAULT_MAX_ACTIVE_KEYS).unwrap()));
            yield_after =
                yield_after.or_else(|| Some(NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap()));
        }

        Ok(BackpressureMode::Release {
            input_buffer_size: input_buffer_size.ok_or(ConfigParseError::MissingKey(
                INPUT_BUFFER_SIZE_TAG,
                RELEASE_TAG,
            ))?,
            bridge_buffer_size: bridge_buffer_size.ok_or(ConfigParseError::MissingKey(
                BRIDGE_BUFFER_SIZE_TAG,
                RELEASE_TAG,
            ))?,
            max_active_keys: max_active_keys.ok_or(ConfigParseError::MissingKey(
                MAX_ACTIVE_KEYS_TAG,
                RELEASE_TAG,
            ))?,
            yield_after: yield_after
                .ok_or(ConfigParseError::MissingKey(YIELD_AFTER_TAG, RELEASE_TAG))?,
        })
    }

    /// Configuration for the creation and management of downlinks for a Warp client.
    pub trait Config: Send + Sync {
        type PathType: Addressable;

        /// Get the downlink configuration for a downlink a specific path.
        fn config_for(&self, path: &Self::PathType) -> DownlinkParams;

        /// Get the global parameters for any downlink.
        fn client_params(&self) -> ClientParams;
    }

    /// Instruction on how to respond when an invalid message is received for a downlink.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum OnInvalidMessage {
        /// Disregard the message and continue.
        Ignore,

        /// Terminate the downlink.
        Terminate,
    }

    const BACK_PRESSURE_TAG: &str = "back_pressure";
    const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
    const ON_INVALID_TAG: &str = "on_invalid";
    const YIELD_AFTER_TAG: &str = "yield_after";
    const TERMINATE_TAG: &str = "terminate";
    const IGNORE_TAG: &str = "ignore";

    const DEFAULT_BACK_PRESSURE: BackpressureMode = BackpressureMode::Propagate;
    const DEFAULT_IDLE_TIMEOUT: u64 = 60000;
    const DEFAULT_DOWNLINK_BUFFER_SIZE: usize = 5;
    const DEFAULT_ON_INVALID: OnInvalidMessage = OnInvalidMessage::Terminate;
    const DEFAULT_YIELD_AFTER: usize = 256;

    /// Configuration parameters for a single downlink.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct DownlinkParams {
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

    impl DownlinkParams {
        pub fn new(
            back_pressure: BackpressureMode,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
            yield_after: usize,
        ) -> Result<DownlinkParams, String> {
            if idle_timeout == Duration::from_millis(0) {
                Err(BAD_TIMEOUT.to_string())
            } else {
                match (
                    NonZeroUsize::new(buffer_size),
                    NonZeroUsize::new(yield_after),
                ) {
                    (Some(nz), Some(ya)) => Ok(DownlinkParams {
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

        //Todo dm this needs to be changed after the new client configuration is finalised.
        //     fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
        //         let mut back_pressure: Option<BackpressureMode> = None;
        //         let mut idle_timeout: Option<Duration> = None;
        //         let mut buffer_size: Option<usize> = None;
        //         let mut on_invalid: Option<OnInvalidMessage> = None;
        //         let mut yield_after: Option<usize> = None;
        //
        //         for item in items {
        //             match item {
        //                 Item::Slot(Value::Text(name), value) => match name.as_str() {
        //                     BACK_PRESSURE_TAG => {
        //                         back_pressure =
        //                             Some(BackpressureMode::try_from_value(value, use_defaults)?)
        //                     }
        //                     IDLE_TIMEOUT_TAG => {
        //                         let timeout = u64::try_from_value(&value).map_err(|_| {
        //                             ConfigParseError::InvalidValue(value, IDLE_TIMEOUT_TAG)
        //                         })?;
        //                         idle_timeout = Some(Duration::from_secs(timeout))
        //                     }
        //                     BUFFER_SIZE_TAG => {
        //                         let size = usize::try_from_value(&value).map_err(|_| {
        //                             ConfigParseError::InvalidValue(value, BUFFER_SIZE_TAG)
        //                         })?;
        //                         buffer_size = Some(size);
        //                     }
        //                     ON_INVALID_TAG => {
        //                         on_invalid = Some(try_on_invalid_from_value(value)?);
        //                     }
        //                     YIELD_AFTER_TAG => {
        //                         let size = usize::try_from_value(&value).map_err(|_| {
        //                             ConfigParseError::InvalidValue(value, YIELD_AFTER_TAG)
        //                         })?;
        //                         yield_after = Some(size);
        //                     }
        //
        //                     _ => {
        //                         return Err(ConfigParseError::UnexpectedKey(
        //                             name.to_string(),
        //                             DOWNLINKS_TAG,
        //                         ))
        //                     }
        //                 },
        //                 Item::Slot(value, _) => {
        //                     return Err(ConfigParseError::UnexpectedValue(
        //                         value,
        //                         Some(DOWNLINKS_TAG),
        //                     ))
        //                 }
        //                 Item::ValueItem(value) => {
        //                     return Err(ConfigParseError::UnexpectedValue(
        //                         value,
        //                         Some(DOWNLINKS_TAG),
        //                     ))
        //                 }
        //             }
        //         }
        //
        //         if use_defaults {
        //             back_pressure = back_pressure.or(Some(DEFAULT_BACK_PRESSURE));
        //             idle_timeout =
        //                 idle_timeout.or_else(|| Some(Duration::from_secs(DEFAULT_IDLE_TIMEOUT)));
        //             buffer_size = buffer_size.or(Some(DEFAULT_DOWNLINK_BUFFER_SIZE));
        //             on_invalid = on_invalid.or(Some(DEFAULT_ON_INVALID));
        //             yield_after = yield_after.or(Some(DEFAULT_YIELD_AFTER));
        //         }
        //
        //         DownlinkParams::new(
        //             back_pressure.ok_or(ConfigParseError::MissingKey(
        //                 BACK_PRESSURE_TAG,
        //                 DOWNLINKS_TAG,
        //             ))?,
        //             idle_timeout.ok_or(ConfigParseError::MissingKey(
        //                 IDLE_TIMEOUT_TAG,
        //                 DOWNLINKS_TAG,
        //             ))?,
        //             buffer_size.ok_or(ConfigParseError::MissingKey(BUFFER_SIZE_TAG, DOWNLINKS_TAG))?,
        //             on_invalid.ok_or(ConfigParseError::MissingKey(ON_INVALID_TAG, DOWNLINKS_TAG))?,
        //             yield_after.ok_or(ConfigParseError::MissingKey(YIELD_AFTER_TAG, DOWNLINKS_TAG))?,
        //         )
        //         .map_err(ConfigParseError::DownlinkError)
        //     }
    }

    //Todo dm this needs to be changed after the new client configuration is finalised.
    // fn try_on_invalid_from_value(value: Value) -> Result<OnInvalidMessage, ConfigParseError> {
    //     let on_invalid_str = String::try_from_value(&value)
    //         .map_err(|_| ConfigParseError::InvalidValue(value, ON_INVALID_TAG))?;
    //
    //     match on_invalid_str.as_str() {
    //         IGNORE_TAG => Ok(OnInvalidMessage::Ignore),
    //         TERMINATE_TAG => Ok(OnInvalidMessage::Terminate),
    //         _ => Err(ConfigParseError::InvalidValue(
    //             Value::text(on_invalid_str),
    //             ON_INVALID_TAG,
    //         )),
    //     }
    // }

    impl Default for DownlinkParams {
        fn default() -> Self {
            DownlinkParams::new(
                DEFAULT_BACK_PRESSURE,
                Duration::from_secs(DEFAULT_IDLE_TIMEOUT),
                DEFAULT_DOWNLINK_BUFFER_SIZE,
                DEFAULT_ON_INVALID,
                DEFAULT_YIELD_AFTER,
            )
            .unwrap()
        }
    }

    /// Configuration parameters for all downlinks.
    #[derive(Clone, Copy, Debug)]
    pub struct ClientParams {
        /// Buffer size for servicing requests for new downlinks.
        pub dl_req_buffer_size: NonZeroUsize,
        /// Configuration parameters for the router.
        pub router_params: RouterParams,
        /// Configuration parameters the remote connections.
        pub connections_params: ConnectionConfig,
        /// Configuration parameters the WebSocket connections.
        pub websocket_params: WebSocketConfig,
        /// Configuration parameter for the connection pool.
        pub conn_pool_params: ConnectionPoolParams,
    }

    const DEFAULT_CLIENT_BUFFER_SIZE: usize = 2;

    const BAD_BUFFER_SIZE: &str = "Buffer sizes must be positive.";
    const BAD_YIELD_AFTER: &str = "Yield after count must be positive..";
    const BAD_TIMEOUT: &str = "Timeout must be positive.";

    impl ClientParams {
        pub fn new(
            dl_req_buffer_size: NonZeroUsize,
            router_params: RouterParams,
            connections_params: ConnectionConfig,
            websocket_params: WebSocketConfig,
            conn_pool_params: ConnectionPoolParams,
        ) -> ClientParams {
            ClientParams {
                dl_req_buffer_size,
                router_params,
                connections_params,
                websocket_params,
                conn_pool_params,
            }
        }

        //Todo dm this needs to be changed after the new client configuration is finalised.
        //     fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
        //         let mut buffer_size: Option<NonZeroUsize> = None;
        //         let mut router_params: Option<RouterParams> = None;
        //
        //         for item in items {
        //             match item {
        //                 Item::Slot(Value::Text(name), value) => match name.as_str() {
        //                     BUFFER_SIZE_TAG => {
        //                         let size = usize::try_from_value(&value).map_err(|_| {
        //                             ConfigParseError::InvalidValue(value, BUFFER_SIZE_TAG)
        //                         })?;
        //                         buffer_size = Some(NonZeroUsize::new(size).unwrap());
        //                     }
        //                     ROUTER_TAG => {
        //                         if let Value::Record(_, items) = value {
        //                             router_params =
        //                                 Some(RouterParams::try_from_items(items, use_defaults)?);
        //                         } else {
        //                             return Err(ConfigParseError::UnexpectedValue(
        //                                 value,
        //                                 Some(ROUTER_TAG),
        //                             ));
        //                         }
        //                     }
        //                     _ => {
        //                         return Err(ConfigParseError::UnexpectedKey(
        //                             name.to_string(),
        //                             CLIENT_TAG,
        //                         ))
        //                     }
        //                 },
        //                 Item::Slot(value, _) => {
        //                     return Err(ConfigParseError::UnexpectedValue(value, Some(CLIENT_TAG)))
        //                 }
        //                 Item::ValueItem(value) => {
        //                     return Err(ConfigParseError::UnexpectedValue(value, Some(CLIENT_TAG)))
        //                 }
        //             }
        //         }
        //
        //         if use_defaults {
        //             buffer_size = buffer_size
        //                 .or_else(|| Some(NonZeroUsize::new(DEFAULT_CLIENT_BUFFER_SIZE).unwrap()));
        //             router_params = router_params.or_else(|| Some(RouterParams::default()));
        //         }
        //
        //         Ok(ClientParams::new(
        //             buffer_size.ok_or(ConfigParseError::MissingKey(BUFFER_SIZE_TAG, CLIENT_TAG))?,
        //             router_params.ok_or(ConfigParseError::MissingKey(ROUTER_TAG, CLIENT_TAG))?,
        //         ))
        //     }
    }

    impl Default for ClientParams {
        fn default() -> Self {
            ClientParams::new(
                NonZeroUsize::new(DEFAULT_CLIENT_BUFFER_SIZE).unwrap(),
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )
        }
    }

    /// Basic [`Config`] implementation which allows for configuration to be specified by absolute
    /// path or host and provides a default fallback.
    #[derive(Clone, Debug)]
    pub struct ConfigHierarchy<Path: Addressable> {
        client_params: ClientParams,
        default: DownlinkParams,
        by_host: HashMap<Url, DownlinkParams>,
        by_lane: HashMap<Path, DownlinkParams>,
    }

    impl<Path: Addressable> ConfigHierarchy<Path> {
        /// Create a new configuration store with just a default.
        pub fn new(client_params: ClientParams, default: DownlinkParams) -> ConfigHierarchy<Path> {
            ConfigHierarchy {
                client_params,
                default,
                by_host: HashMap::new(),
                by_lane: HashMap::new(),
            }
        }

        /// Add specific configuration for a host.
        pub fn for_host(&mut self, host: Url, params: DownlinkParams) {
            self.by_host.insert(host, params);
        }

        /// Add specific configuration for an absolute path (this will override host level
        /// configuration).
        pub fn for_lane(&mut self, lane: &Path, params: DownlinkParams) {
            self.by_lane.insert(lane.clone(), params);
        }

        //Todo dm this needs to be changed after the new client configuration is finalised.
        //     pub fn try_from_value(value: Value, use_defaults: bool) -> Result<Self, ConfigParseError> {
        //         let (mut attrs, items) = match value {
        //             Value::Record(attrs, items) if attrs.len() <= 1 => (attrs, items),
        //             _ => return Err(ConfigParseError::UnexpectedValue(value, None)),
        //         };
        //
        //         if let Some(Attr { name, value: _ }) = attrs.pop() {
        //             if name == CONFIG_TAG {
        //                 ConfigHierarchy::try_from_items(items, use_defaults)
        //             } else {
        //                 Err(ConfigParseError::UnexpectedAttribute(
        //                     name.to_string(),
        //                     None,
        //                 ))
        //             }
        //         } else {
        //             Err(ConfigParseError::UnnamedRecord(
        //                 Value::Record(attrs, items),
        //                 None,
        //             ))
        //         }
        //     }
        //
        //     fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
        //         let mut client_params: Option<ClientParams> = None;
        //         let mut downlink_params: Option<DownlinkParams> = None;
        //         let mut host_params: HashMap<Url, DownlinkParams> = HashMap::new();
        //         let mut lane_params: HashMap<AbsolutePath, DownlinkParams> = HashMap::new();
        //
        //         for item in items {
        //             match item {
        //                 Item::ValueItem(value) => {
        //                     let (mut attrs, items) = match value {
        //                         Value::Record(attrs, items) if attrs.len() <= 1 => (attrs, items),
        //                         _ => {
        //                             return Err(ConfigParseError::UnexpectedValue(
        //                                 value,
        //                                 Some(CONFIG_TAG),
        //                             ))
        //                         }
        //                     };
        //
        //                     if let Some(Attr { name, value: _ }) = attrs.pop() {
        //                         match name.as_str() {
        //                             CLIENT_TAG => {
        //                                 client_params =
        //                                     Some(ClientParams::try_from_items(items, use_defaults)?);
        //                             }
        //                             DOWNLINKS_TAG => {
        //                                 downlink_params =
        //                                     Some(DownlinkParams::try_from_items(items, use_defaults)?);
        //                             }
        //                             HOST_TAG => {
        //                                 for item in items {
        //                                     let (url, params) =
        //                                         try_host_params_from_item(item, use_defaults)?;
        //                                     host_params.insert(url, params);
        //                                 }
        //                             }
        //                             LANE_TAG => {
        //                                 for item in items {
        //                                     let (path, params) =
        //                                         try_lane_params_from_item(item, use_defaults)?;
        //                                     lane_params.insert(path, params);
        //                                 }
        //                             }
        //                             _ => {
        //                                 return Err(ConfigParseError::UnexpectedAttribute(
        //                                     name.to_string(),
        //                                     Some(CONFIG_TAG),
        //                                 ))
        //                             }
        //                         }
        //                     } else {
        //                         return Err(ConfigParseError::UnnamedRecord(
        //                             Value::Record(attrs, items),
        //                             Some(CONFIG_TAG),
        //                         ));
        //                     }
        //                 }
        //                 _ => return Err(ConfigParseError::UnexpectedSlot(item, CONFIG_TAG)),
        //             }
        //         }
        //
        //         if use_defaults {
        //             client_params = client_params.or_else(|| Some(ClientParams::default()));
        //             downlink_params = downlink_params.or_else(|| Some(DownlinkParams::default()));
        //         }
        //
        //         Ok(ConfigHierarchy {
        //             client_params: client_params
        //                 .ok_or(ConfigParseError::MissingAttribute(CLIENT_TAG, CONFIG_TAG))?,
        //             default: downlink_params.ok_or(ConfigParseError::MissingAttribute(
        //                 DOWNLINKS_TAG,
        //                 CONFIG_TAG,
        //             ))?,
        //             by_host: host_params,
        //             by_lane: lane_params,
        //         })
        //     }
        // }
        //
        // fn try_host_params_from_item(
        //     item: Item,
        //     use_defaults: bool,
        // ) -> Result<(Url, DownlinkParams), ConfigParseError> {
        //     match item {
        //         Item::Slot(Value::Text(name), Value::Record(_, items)) => {
        //             let host = Url::parse(name.as_str())
        //                 .map_err(|_| ConfigParseError::InvalidKey(Value::Text(name), HOST_TAG))?;
        //             let downlink_params = DownlinkParams::try_from_items(items, use_defaults)?;
        //             Ok((host, downlink_params))
        //         }
        //         Item::Slot(value, _) => Err(ConfigParseError::UnexpectedValue(value, Some(HOST_TAG))),
        //         Item::ValueItem(value) => Err(ConfigParseError::UnexpectedValue(value, Some(HOST_TAG))),
        //     }
        // }
        //
        // fn try_lane_params_from_item(
        //     item: Item,
        //     use_defaults: bool,
        // ) -> Result<(AbsolutePath, DownlinkParams), ConfigParseError> {
        //     match item {
        //         Item::ValueItem(Value::Record(mut attrs, items)) if attrs.len() <= 1 => {
        //             if let Some(Attr { name, value }) = attrs.pop() {
        //                 if name == PATH_TAG {
        //                     let path = try_absolute_path_from_record(value)?;
        //                     let downlink_params = DownlinkParams::try_from_items(items, use_defaults)?;
        //                     Ok((path, downlink_params))
        //                 } else {
        //                     Err(ConfigParseError::UnexpectedAttribute(
        //                         name.to_string(),
        //                         Some(LANE_TAG),
        //                     ))
        //                 }
        //             } else {
        //                 Err(ConfigParseError::UnnamedRecord(
        //                     Value::Record(attrs, items),
        //                     Some(LANE_TAG),
        //                 ))
        //             }
        //         }
        //         Item::ValueItem(value) => Err(ConfigParseError::UnexpectedValue(value, Some(LANE_TAG))),
        //         _ => Err(ConfigParseError::UnexpectedSlot(item, LANE_TAG)),
        //     }
        // }
        //
        // fn try_absolute_path_from_record(record: Value) -> Result<AbsolutePath, ConfigParseError> {
        //     let mut host: Option<Url> = None;
        //     let mut node: Option<String> = None;
        //     let mut lane: Option<String> = None;
        //
        //     match record {
        //         Value::Record(_, items) => {
        //             for item in items {
        //                 match item {
        //                     Item::Slot(Value::Text(name), Value::Text(value)) => match name.as_str() {
        //                         HOST_TAG => {
        //                             host = Some(Url::parse(value.as_str()).map_err(|_| {
        //                                 ConfigParseError::InvalidKey(Value::Text(name), HOST_TAG)
        //                             })?)
        //                         }
        //                         NODE_TAG => node = Some(value.to_string()),
        //                         LANE_TAG => lane = Some(value.to_string()),
        //                         _ => {
        //                             return Err(ConfigParseError::UnexpectedKey(
        //                                 name.to_string(),
        //                                 PATH_TAG,
        //                             ))
        //                         }
        //                     },
        //                     Item::Slot(Value::Text(_), value) => {
        //                         return Err(ConfigParseError::UnexpectedValue(value, Some(LANE_TAG)))
        //                     }
        //                     Item::Slot(value, _) => {
        //                         return Err(ConfigParseError::UnexpectedValue(value, Some(LANE_TAG)))
        //                     }
        //                     Item::ValueItem(value) => {
        //                         return Err(ConfigParseError::UnexpectedValue(value, Some(LANE_TAG)))
        //                     }
        //                 }
        //             }
        //         }
        //         _ => return Err(ConfigParseError::UnexpectedValue(record, Some(LANE_TAG))),
        //     };
        //
        //     Ok(AbsolutePath::new(
        //         host.ok_or(ConfigParseError::MissingKey(HOST_TAG, PATH_TAG))?,
        //         &node.ok_or(ConfigParseError::MissingKey(NODE_TAG, PATH_TAG))?,
        //         &lane.ok_or(ConfigParseError::MissingKey(LANE_TAG, PATH_TAG))?,
        //     ))
    }

    type Key = String;
    type Tag = &'static str;
    type ParentTag = &'static str;

    #[derive(Debug)]
    pub enum ConfigParseError {
        //Error that occurs when trying to read the file.
        FileError(std::io::Error),
        // Error that occurs when parsing the file to Recon.
        ReconError(ParseFailure),
        //Error that occurs when creating downlink parameters.
        DownlinkError(String),
        // Error that occurs when a required attribute is missing in the
        // configuration file.
        MissingAttribute(Tag, ParentTag),
        // Error that occurs when a required key is missing in the
        // configuration file.
        MissingKey(Tag, ParentTag),
        // Error that occurs when a key is invalid.
        // e.g. invalid URL in the host tag.
        InvalidKey(Value, ParentTag),
        // Error that occurs when a value associated with a key is invalid.
        // e.g. `str` when expecting `i32`.
        InvalidValue(Value, ParentTag),
        // Error that occurs when an unexpected attribute is present in the
        // configuration file.
        UnexpectedAttribute(String, Option<ParentTag>),
        // Error that occurs when an unexpected slot is present
        // in the configuration file.
        UnexpectedSlot(Item, ParentTag),
        // Error the occurs when an unexpected key is present in an attribute
        // in the configuration file.
        UnexpectedKey(Key, ParentTag),
        // Error that occurs when an unexpected value is present
        // in the configuration file.
        UnexpectedValue(Value, Option<ParentTag>),
        // Error that occurs when an record without an attribute name is present in the
        // configuration file.
        UnnamedRecord(Value, Option<ParentTag>),
    }

    impl Error for ConfigParseError {}

    impl Display for ConfigParseError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                ConfigParseError::FileError(e) => write!(f, "File error: {}", e.to_string()),
                ConfigParseError::ReconError(e) => write!(f, "Recon error: {}", e.to_string()),
                ConfigParseError::DownlinkError(e) => write!(f, "Downlink error: {}", e),
                ConfigParseError::MissingAttribute(missing, parent) => {
                    write!(f, "Missing \"@{}\" attribute in \"@{}\".", missing, parent)
                }
                ConfigParseError::MissingKey(missing, parent) => {
                    write!(f, "Missing \"{}\" key in \"@{}\".", missing, parent)
                }
                ConfigParseError::InvalidKey(value, tag) => {
                    write!(f, "Invalid key \"{}\" in \"{}\".", value, tag)
                }
                ConfigParseError::InvalidValue(value, tag) => {
                    write!(f, "Invalid value \"{}\" in \"{}\".", value, tag)
                }
                ConfigParseError::UnexpectedAttribute(invalid, Some(parent)) => write!(
                    f,
                    "Unexpected attribute \"@{}\" in \"{}\".",
                    invalid, parent
                ),
                ConfigParseError::UnexpectedAttribute(invalid, None) => {
                    write!(f, "Unexpected attribute \"@{}\".", invalid)
                }
                ConfigParseError::UnexpectedSlot(unexpected, parent) => {
                    write!(f, "Unexpected slot \"{}\" in \"{}\".", unexpected, parent)
                }
                ConfigParseError::UnexpectedKey(key, tag) => {
                    write!(f, "Unexpected key \"{}\" in \"{}\".", key, tag)
                }
                ConfigParseError::UnexpectedValue(unexpected, Some(parent)) => {
                    write!(f, "Unexpected value \"{}\" in \"{}\".", unexpected, parent)
                }
                ConfigParseError::UnexpectedValue(unexpected, None) => {
                    write!(f, "Unexpected value \"{}\".", unexpected)
                }
                ConfigParseError::UnnamedRecord(unnamed, Some(parent)) => {
                    write!(f, "Unnamed record \"{}\" in \"{}\".", unnamed, parent)
                }
                ConfigParseError::UnnamedRecord(unnamed, None) => {
                    write!(f, "Unnamed record \"{}\".", unnamed)
                }
            }
        }
    }

    impl<Path: Addressable + Sync> Config for ConfigHierarchy<Path> {
        type PathType = Path;

        fn config_for(&self, path: &Self::PathType) -> DownlinkParams {
            let ConfigHierarchy {
                default,
                by_host,
                by_lane,
                ..
            } = self;
            match by_lane.get(path) {
                Some(params) => *params,
                _ => {
                    let maybe_host = path.host();

                    match maybe_host {
                        Some(host) => match by_host.get(&host) {
                            Some(params) => *params,
                            _ => *default,
                        },
                        None => *default,
                    }
                }
            }
        }

        fn client_params(&self) -> ClientParams {
            self.client_params
        }
    }

    impl<Path: Addressable> Default for ConfigHierarchy<Path> {
        fn default() -> Self {
            let client_params = Default::default();
            let default_params = Default::default();

            ConfigHierarchy::new(client_params, default_params)
        }
    }

    impl<'a, Path: Addressable> Config for Box<dyn Config<PathType = Path> + 'a> {
        type PathType = Path;

        fn config_for(&self, path: &Self::PathType) -> DownlinkParams {
            (**self).config_for(path)
        }

        fn client_params(&self) -> ClientParams {
            (**self).client_params()
        }
    }
}
