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

#[cfg(test)]
mod tests;

pub mod downlink {
    use crate::configuration::router::RouterParams;
    use common::model::{Attr, Item, Value};
    use common::warp::path::AbsolutePath;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::fmt::{Display, Formatter};
    use std::num::NonZeroUsize;
    use swim_form::Form;
    use tokio::time::Duration;
    use url::Url;

    const CONFIG_TAG: &str = "config";
    const CLIENT_TAG: &str = "client";
    const DOWNLINKS_TAG: &str = "downlinks";
    const PATH_TAG: &str = "path";
    const HOST_TAG: &str = "host";
    const LANE_TAG: &str = "lane";
    const NODE_TAG: &str = "node";
    const BUFFER_SIZE_TAG: &str = "buffer_size";
    const ROUTER_TAG: &str = "router";

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
        fn try_from_value(mut attrs: Vec<Attr>) -> Result<Self, ConfigParseError> {
            let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

            match name.as_str() {
                PROPAGATE_TAG => {
                    return Ok(BackpressureMode::Propagate);
                }
                RELEASE_TAG => {
                    if let Value::Record(_, items) = value {
                        try_release_mode_from_items(items)
                    } else {
                        return Err(ConfigParseError {});
                    }
                }
                _ => Err(ConfigParseError {}),
            }
        }
    }

    const INPUT_BUFFER_SIZE: &str = "input_buffer_size";
    const BRIDGE_BUFFER_SIZE: &str = "bridge_buffer_size";
    const MAX_ACTIVE_KEYS: &str = "max_active_keys";
    const YIELD_AFTER: &str = "yield_after";

    fn try_release_mode_from_items(items: Vec<Item>) -> Result<BackpressureMode, ConfigParseError> {
        let mut input_buffer_size: Option<NonZeroUsize> = None;
        let mut bridge_buffer_size: Option<NonZeroUsize> = None;
        let mut max_active_keys: Option<NonZeroUsize> = None;
        let mut yield_after: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    INPUT_BUFFER_SIZE => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        input_buffer_size =
                            Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    BRIDGE_BUFFER_SIZE => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        bridge_buffer_size =
                            Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    MAX_ACTIVE_KEYS => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        max_active_keys = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    YIELD_AFTER => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        yield_after = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            };
        }

        //Todo add defaults
        return match (
            input_buffer_size,
            bridge_buffer_size,
            max_active_keys,
            yield_after,
        ) {
            (
                Some(input_buffer_size),
                Some(bridge_buffer_size),
                Some(max_active_keys),
                Some(yield_after),
            ) => Ok(BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            }),
            _ => Err(ConfigParseError {}),
        };
    }

    /// Configuration for the creation and management of downlinks for a Warp client.
    pub trait Config: Send + Sync {
        /// Get the downlink configuration for a downlink a specific path.
        fn config_for(&self, path: &AbsolutePath) -> DownlinkParams;

        /// Get the global parameters for any downlink.
        fn client_params(&self) -> ClientParams;
    }

    const QUEUE_TAG: &str = "queue";
    const DROPPING_TAG: &str = "dropping";
    const BUFFERED_TAG: &str = "buffered";
    const QUEUE_SIZE_TAG: &str = "queue_size";

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

    impl MuxMode {
        fn try_from_value(mut attrs: Vec<Attr>) -> Result<Self, ConfigParseError> {
            let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

            match name.as_str() {
                QUEUE_TAG => {
                    if let Value::Record(_, items) = value {
                        try_queue_mode_from_items(items)
                    } else {
                        return Err(ConfigParseError {});
                    }
                }
                DROPPING_TAG => Ok(MuxMode::Dropping),
                BUFFERED_TAG => {
                    if let Value::Record(_, items) = value {
                        try_buffered_mode_from_items(items)
                    } else {
                        return Err(ConfigParseError {});
                    }
                }
                _ => Err(ConfigParseError {}),
            }
        }
    }

    fn try_queue_mode_from_items(items: Vec<Item>) -> Result<MuxMode, ConfigParseError> {
        let mut queue_size: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    QUEUE_SIZE_TAG => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        queue_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            }
        }

        //Todo add defaults
        return match queue_size {
            Some(queue_size) => Ok(MuxMode::Queue(queue_size)),
            _ => Err(ConfigParseError {}),
        };
    }

    fn try_buffered_mode_from_items(items: Vec<Item>) -> Result<MuxMode, ConfigParseError> {
        let mut queue_size: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    QUEUE_SIZE_TAG => {
                        //Todo replace with direct conversion
                        let size_as_i32 =
                            i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        let size = usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                        queue_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            };
        }

        //Todo add defaults
        return match queue_size {
            Some(queue_size) => Ok(MuxMode::Buffered(queue_size)),
            _ => Err(ConfigParseError {}),
        };
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
    const MUX_MODE_TAG: &str = "mux_mode";
    const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
    const ON_INVALID_TAG: &str = "on_invalid";
    const YIELD_AFTER_TAG: &str = "yield_after";
    const TERMINATE_TAG: &str = "terminate";
    const IGNORE_TAG: &str = "ignore";

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

        /// Number of operations after which a downlink will yield to the runtime.
        pub yield_after: NonZeroUsize,
    }

    impl DownlinkParams {
        pub fn new(
            back_pressure: BackpressureMode,
            mux_mode: MuxMode,
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
                        mux_mode,
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

        pub fn new_queue(
            back_pressure: BackpressureMode,
            queue_size: usize,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
            yield_after: usize,
        ) -> Result<DownlinkParams, String> {
            match NonZeroUsize::new(queue_size) {
                Some(nz) => Self::new(
                    back_pressure,
                    MuxMode::Queue(nz),
                    idle_timeout,
                    buffer_size,
                    on_invalid,
                    yield_after,
                ),
                _ => Err(BAD_BUFFER_SIZE.to_string()),
            }
        }

        pub fn new_dropping(
            back_pressure: BackpressureMode,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
            yield_after: usize,
        ) -> Result<DownlinkParams, String> {
            Self::new(
                back_pressure,
                MuxMode::Dropping,
                idle_timeout,
                buffer_size,
                on_invalid,
                yield_after,
            )
        }

        pub fn new_buffered(
            back_pressure: BackpressureMode,
            queue_size: usize,
            idle_timeout: Duration,
            buffer_size: usize,
            on_invalid: OnInvalidMessage,
            yield_after: usize,
        ) -> Result<DownlinkParams, String> {
            match NonZeroUsize::new(queue_size) {
                Some(nz) => Self::new(
                    back_pressure,
                    MuxMode::Buffered(nz),
                    idle_timeout,
                    buffer_size,
                    on_invalid,
                    yield_after,
                ),
                _ => Err(BAD_BUFFER_SIZE.to_string()),
            }
        }

        fn try_from_items(items: Vec<Item>) -> Result<Self, ConfigParseError> {
            let mut back_pressure: Option<BackpressureMode> = None;
            let mut mux_mode: Option<MuxMode> = None;
            let mut idle_timeout: Option<Duration> = None;
            let mut buffer_size: Option<usize> = None;
            let mut on_invalid: Option<OnInvalidMessage> = None;
            let mut yield_after: Option<usize> = None;

            for item in items {
                match item {
                    Item::Slot(Value::Text(name), value) => match name.as_str() {
                        BACK_PRESSURE_TAG => {
                            if let Value::Record(attrs, _) = value {
                                back_pressure = Some(BackpressureMode::try_from_value(attrs)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        MUX_MODE_TAG => {
                            if let Value::Record(attrs, _) = value {
                                mux_mode = Some(MuxMode::try_from_value(attrs)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        IDLE_TIMEOUT_TAG => {
                            //Todo replace with direct conversion
                            let timeout_as_i32 =
                                i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            let timeout =
                                u64::try_from(timeout_as_i32).map_err(|_| ConfigParseError {})?;
                            idle_timeout = Some(Duration::from_secs(timeout))
                        }
                        BUFFER_SIZE_TAG => {
                            //Todo replace with direct conversion
                            let size_as_i32 =
                                i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            let size =
                                usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                            buffer_size = Some(size);
                        }
                        ON_INVALID_TAG => {
                            let on_invalid_str =
                                String::try_from_value(&value).map_err(|_| ConfigParseError {})?;

                            match on_invalid_str.as_str() {
                                IGNORE_TAG => on_invalid = Some(OnInvalidMessage::Ignore),
                                TERMINATE_TAG => on_invalid = Some(OnInvalidMessage::Terminate),
                                _ => return Err(ConfigParseError {}),
                            }
                        }
                        YIELD_AFTER_TAG => {
                            //Todo replace with direct conversion
                            let yield_as_i32 =
                                i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            let yield_size =
                                usize::try_from(yield_as_i32).map_err(|_| ConfigParseError {})?;
                            yield_after = Some(yield_size);
                        }

                        _ => return Err(ConfigParseError {}),
                    },
                    _ => return Err(ConfigParseError {}),
                }
            }

            //Todo add defaults
            return match (
                back_pressure,
                mux_mode,
                idle_timeout,
                buffer_size,
                on_invalid,
                yield_after,
            ) {
                (
                    Some(back_pressure),
                    Some(mux_mode),
                    Some(idle_timeout),
                    Some(buffer_size),
                    Some(on_invalid),
                    Some(yield_after),
                ) => Ok(DownlinkParams::new(
                    back_pressure,
                    mux_mode,
                    idle_timeout,
                    buffer_size,
                    on_invalid,
                    yield_after,
                )
                .map_err(|_| ConfigParseError {})?),
                _ => Err(ConfigParseError {}),
            };
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
    const BAD_YIELD_AFTER: &str = "Yield after count must be positive..";
    const BAD_TIMEOUT: &str = "Timeout must be positive.";

    impl ClientParams {
        pub fn new(dl_req_buffer_size: NonZeroUsize, router_params: RouterParams) -> ClientParams {
            ClientParams {
                dl_req_buffer_size,
                router_params,
            }
        }

        fn try_from_items(items: Vec<Item>) -> Result<Self, ConfigParseError> {
            let mut buffer_size: Option<NonZeroUsize> = None;
            let mut router_params: Option<RouterParams> = None;

            for item in items {
                match item {
                    Item::Slot(Value::Text(name), value) => match name.as_str() {
                        BUFFER_SIZE_TAG => {
                            //Todo replace with direct conversion
                            let size_as_i32 =
                                i32::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            let size =
                                usize::try_from(size_as_i32).map_err(|_| ConfigParseError {})?;
                            buffer_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                        }
                        ROUTER_TAG => {
                            if let Value::Record(_, items) = value {
                                router_params = Some(RouterParams::try_from_items(items)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        _ => return Err(ConfigParseError {}),
                    },
                    _ => return Err(ConfigParseError {}),
                }
            }

            //Todo add defaults
            return match (buffer_size, router_params) {
                (Some(buffer_size), Some(router_params)) => {
                    Ok(ClientParams::new(buffer_size, router_params))
                }
                _ => Err(ConfigParseError {}),
            };
        }
    }

    /// Basic [`Config`] implementation which allows for configuration to be specified by absolute
    /// path or host and provides a default fallback.
    #[derive(Clone, Debug)]
    pub struct ConfigHierarchy {
        client_params: ClientParams,
        default: DownlinkParams,
        by_host: HashMap<Url, DownlinkParams>,
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
        pub fn for_host(&mut self, host: Url, params: DownlinkParams) {
            self.by_host.insert(host, params);
        }

        /// Add specific configuration for an absolute path (this will override host level
        /// configuration).
        pub fn for_lane(&mut self, lane: &AbsolutePath, params: DownlinkParams) {
            self.by_lane.insert(lane.clone(), params);
        }

        pub fn try_from_value(value: Value) -> Result<Self, ConfigParseError> {
            let (mut attrs, items) = match value {
                Value::Record(attrs, items) => (attrs, items),
                _ => return Err(ConfigParseError {}),
            };

            if attrs.pop().ok_or(ConfigParseError {})?.name == CONFIG_TAG {
                ConfigHierarchy::try_from_items(items)
            } else {
                return Err(ConfigParseError {});
            }
        }

        fn try_from_items(items: Vec<Item>) -> Result<Self, ConfigParseError> {
            let mut client_params: Option<ClientParams> = None;
            let mut downlink_params: Option<DownlinkParams> = None;
            let mut host_params: HashMap<Url, DownlinkParams> = HashMap::new();
            let mut lane_params: HashMap<AbsolutePath, DownlinkParams> = HashMap::new();

            for item in items {
                match item {
                    Item::ValueItem(value) => {
                        let (mut attrs, items) = match value {
                            Value::Record(attrs, items) => (attrs, items),
                            _ => return Err(ConfigParseError {}),
                        };

                        match attrs.pop().ok_or(ConfigParseError {})?.name.as_str() {
                            CLIENT_TAG => {
                                client_params = Some(ClientParams::try_from_items(items)?);
                            }
                            DOWNLINKS_TAG => {
                                downlink_params = Some(DownlinkParams::try_from_items(items)?);
                            }
                            HOST_TAG => {
                                for item in items {
                                    let (url, params) = try_host_params_from_item(item)?;
                                    host_params.insert(url, params);
                                }
                            }
                            LANE_TAG => {
                                for item in items {
                                    let (path, params) = try_lane_params_from_item(item)?;
                                    lane_params.insert(path, params);
                                }
                            }
                            _ => return Err(ConfigParseError {}),
                        }
                    }
                    _ => return Err(ConfigParseError {}),
                }
            }

            //Todo add defaults
            return match (client_params, downlink_params) {
                (Some(client_params), Some(downlink_params)) => Ok(ConfigHierarchy {
                    client_params,
                    default: downlink_params,
                    by_host: host_params,
                    by_lane: lane_params,
                }),
                _ => Err(ConfigParseError {}),
            };
        }
    }

    fn try_host_params_from_item(item: Item) -> Result<(Url, DownlinkParams), ConfigParseError> {
        match item {
            Item::Slot(Value::Text(name), Value::Record(_, items)) => {
                let downlink_params = DownlinkParams::try_from_items(items)?;
                let host = Url::parse(&name).map_err(|_| ConfigParseError {})?;
                Ok((host, downlink_params))
            }
            _ => Err(ConfigParseError {}),
        }
    }

    fn try_lane_params_from_item(
        item: Item,
    ) -> Result<(AbsolutePath, DownlinkParams), ConfigParseError> {
        match item {
            Item::ValueItem(Value::Record(mut attrs, items)) => {
                let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

                if name == PATH_TAG {
                    let path = try_absolute_path_from_record(value)?;
                    let downlink_params = DownlinkParams::try_from_items(items)?;
                    Ok((path, downlink_params))
                } else {
                    return Err(ConfigParseError {});
                }
            }
            _ => Err(ConfigParseError {}),
        }
    }

    fn try_absolute_path_from_record(record: Value) -> Result<AbsolutePath, ConfigParseError> {
        let mut host: Option<Url> = None;
        let mut node: Option<String> = None;
        let mut lane: Option<String> = None;

        match record {
            Value::Record(_, items) => {
                for item in items {
                    match item {
                        Item::Slot(Value::Text(name), Value::Text(value)) => match name.as_str() {
                            HOST_TAG => {
                                host = Some(Url::parse(&value).map_err(|_| ConfigParseError {})?)
                            }
                            NODE_TAG => node = Some(value),
                            LANE_TAG => lane = Some(value),
                            _ => return Err(ConfigParseError {}),
                        },
                        _ => return Err(ConfigParseError {}),
                    }
                }
            }
            _ => return Err(ConfigParseError {}),
        };

        //Todo add defaults
        match (host, node, lane) {
            (Some(host), Some(node), Some(lane)) => Ok(AbsolutePath::new(host, &node, &lane)),
            _ => Err(ConfigParseError {}),
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ConfigParseError {}

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
                _ => {
                    if path.host.has_host() {
                        match by_host.get(&path.host.clone()) {
                            Some(params) => *params,
                            _ => *default,
                        }
                    } else {
                        *default
                    }
                }
            }
        }

        fn client_params(&self) -> ClientParams {
            self.client_params
        }
    }

    impl Default for ConfigHierarchy {
        fn default() -> Self {
            let client_params =
                ClientParams::new(NonZeroUsize::new(2).unwrap(), Default::default());
            let timeout = Duration::from_secs(60000);
            let default_params = DownlinkParams::new_queue(
                BackpressureMode::Propagate,
                5,
                timeout,
                5,
                OnInvalidMessage::Terminate,
                256,
            )
            .unwrap();
            ConfigHierarchy::new(client_params, default_params)
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
