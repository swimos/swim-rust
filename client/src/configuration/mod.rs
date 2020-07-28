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
        fn try_from_value(
            mut attrs: Vec<Attr>,
            use_defaults: bool,
        ) -> Result<Self, ConfigParseError> {
            let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

            match name.as_str() {
                PROPAGATE_TAG => Ok(BackpressureMode::Propagate),
                RELEASE_TAG => {
                    if let Value::Record(_, items) = value {
                        try_release_mode_from_items(items, use_defaults)
                    } else {
                        Err(ConfigParseError {})
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
                    INPUT_BUFFER_SIZE => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        input_buffer_size =
                            Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    BRIDGE_BUFFER_SIZE => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        bridge_buffer_size =
                            Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    MAX_ACTIVE_KEYS => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        max_active_keys = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    YIELD_AFTER => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        yield_after = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            };
        }

        if input_buffer_size.is_none() && use_defaults {
            input_buffer_size = Some(NonZeroUsize::new(DEFAULT_INPUT_BUFFER_SIZE).unwrap())
        }

        if bridge_buffer_size.is_none() && use_defaults {
            bridge_buffer_size = Some(NonZeroUsize::new(DEFAULT_BRIDGE_BUFFER_SIZE).unwrap())
        }

        if max_active_keys.is_none() && use_defaults {
            max_active_keys = Some(NonZeroUsize::new(DEFAULT_MAX_ACTIVE_KEYS).unwrap())
        }

        if yield_after.is_none() && use_defaults {
            yield_after = Some(NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap())
        }

        Ok(BackpressureMode::Release {
            input_buffer_size: input_buffer_size.ok_or(ConfigParseError {})?,
            bridge_buffer_size: bridge_buffer_size.ok_or(ConfigParseError {})?,
            max_active_keys: max_active_keys.ok_or(ConfigParseError {})?,
            yield_after: yield_after.ok_or(ConfigParseError {})?,
        })
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

    const DEFAULT_QUEUE_SIZE: usize = 5;

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

    impl Default for MuxMode {
        fn default() -> Self {
            MuxMode::Queue(NonZeroUsize::new(DEFAULT_QUEUE_SIZE).unwrap())
        }
    }

    impl MuxMode {
        fn try_from_value(
            mut attrs: Vec<Attr>,
            use_defaults: bool,
        ) -> Result<Self, ConfigParseError> {
            let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

            match name.as_str() {
                QUEUE_TAG => {
                    if let Value::Record(_, items) = value {
                        try_queue_mode_from_items(items, use_defaults)
                    } else {
                        Err(ConfigParseError {})
                    }
                }
                DROPPING_TAG => Ok(MuxMode::Dropping),
                BUFFERED_TAG => {
                    if let Value::Record(_, items) = value {
                        try_buffered_mode_from_items(items, use_defaults)
                    } else {
                        Err(ConfigParseError {})
                    }
                }
                _ => Err(ConfigParseError {}),
            }
        }
    }

    fn try_queue_mode_from_items(
        items: Vec<Item>,
        use_defaults: bool,
    ) -> Result<MuxMode, ConfigParseError> {
        let mut queue_size: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    QUEUE_SIZE_TAG => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        queue_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            }
        }

        if queue_size.is_none() && use_defaults {
            queue_size = Some(NonZeroUsize::new(DEFAULT_QUEUE_SIZE).unwrap())
        }

        Ok(MuxMode::Queue(queue_size.ok_or(ConfigParseError {})?))
    }

    fn try_buffered_mode_from_items(
        items: Vec<Item>,
        use_defaults: bool,
    ) -> Result<MuxMode, ConfigParseError> {
        let mut queue_size: Option<NonZeroUsize> = None;

        for item in items {
            match item {
                Item::Slot(Value::Text(name), value) => match name.as_str() {
                    QUEUE_SIZE_TAG => {
                        let size =
                            usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                        queue_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                    }

                    _ => return Err(ConfigParseError {}),
                },

                _ => return Err(ConfigParseError {}),
            };
        }

        if queue_size.is_none() && use_defaults {
            queue_size = Some(NonZeroUsize::new(DEFAULT_QUEUE_SIZE).unwrap())
        }

        Ok(MuxMode::Buffered(queue_size.ok_or(ConfigParseError {})?))
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

        fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
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
                                back_pressure =
                                    Some(BackpressureMode::try_from_value(attrs, use_defaults)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        MUX_MODE_TAG => {
                            if let Value::Record(attrs, _) = value {
                                mux_mode = Some(MuxMode::try_from_value(attrs, use_defaults)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        IDLE_TIMEOUT_TAG => {
                            let timeout =
                                u64::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            idle_timeout = Some(Duration::from_secs(timeout))
                        }
                        BUFFER_SIZE_TAG => {
                            let size =
                                usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
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
                            let size =
                                usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            yield_after = Some(size);
                        }

                        _ => return Err(ConfigParseError {}),
                    },
                    _ => return Err(ConfigParseError {}),
                }
            }

            if back_pressure.is_none() && use_defaults {
                back_pressure = Some(DEFAULT_BACK_PRESSURE)
            }

            if mux_mode.is_none() && use_defaults {
                mux_mode = Some(MuxMode::default())
            }

            if idle_timeout.is_none() && use_defaults {
                idle_timeout = Some(Duration::from_secs(DEFAULT_IDLE_TIMEOUT))
            }

            if buffer_size.is_none() && use_defaults {
                buffer_size = Some(DEFAULT_DOWNLINK_BUFFER_SIZE)
            }

            if on_invalid.is_none() && use_defaults {
                on_invalid = Some(DEFAULT_ON_INVALID)
            }

            if yield_after.is_none() && use_defaults {
                yield_after = Some(DEFAULT_YIELD_AFTER)
            }

            Ok(DownlinkParams::new(
                back_pressure.ok_or(ConfigParseError {})?,
                mux_mode.ok_or(ConfigParseError {})?,
                idle_timeout.ok_or(ConfigParseError {})?,
                buffer_size.ok_or(ConfigParseError {})?,
                on_invalid.ok_or(ConfigParseError {})?,
                yield_after.ok_or(ConfigParseError {})?,
            )
            .map_err(|_| ConfigParseError {})?)
        }
    }

    impl Default for DownlinkParams {
        fn default() -> Self {
            DownlinkParams::new(
                DEFAULT_BACK_PRESSURE,
                Default::default(),
                Duration::from_secs(DEFAULT_IDLE_TIMEOUT),
                DEFAULT_DOWNLINK_BUFFER_SIZE,
                DEFAULT_ON_INVALID,
                DEFAULT_YIELD_AFTER,
            )
            .unwrap()
        }
    }

    /// Configuration parameters for all downlinks.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct ClientParams {
        /// Buffer size for servicing requests for new downlinks.
        pub dl_req_buffer_size: NonZeroUsize,

        pub router_params: RouterParams,
    }

    const DEFAULT_CLIENT_BUFFER_SIZE: usize = 2;

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

        fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
            let mut buffer_size: Option<NonZeroUsize> = None;
            let mut router_params: Option<RouterParams> = None;

            for item in items {
                match item {
                    Item::Slot(Value::Text(name), value) => match name.as_str() {
                        BUFFER_SIZE_TAG => {
                            let size =
                                usize::try_from_value(&value).map_err(|_| ConfigParseError {})?;
                            buffer_size = Some(NonZeroUsize::new(size).ok_or(ConfigParseError {})?);
                        }
                        ROUTER_TAG => {
                            if let Value::Record(_, items) = value {
                                router_params =
                                    Some(RouterParams::try_from_items(items, use_defaults)?);
                            } else {
                                return Err(ConfigParseError {});
                            }
                        }
                        _ => return Err(ConfigParseError {}),
                    },
                    _ => return Err(ConfigParseError {}),
                }
            }

            if buffer_size.is_none() && use_defaults {
                buffer_size = Some(NonZeroUsize::new(DEFAULT_CLIENT_BUFFER_SIZE).unwrap())
            }

            if router_params.is_none() && use_defaults {
                router_params = Some(RouterParams::default())
            }

            Ok(ClientParams::new(
                buffer_size.ok_or(ConfigParseError {})?,
                router_params.ok_or(ConfigParseError {})?,
            ))
        }
    }

    impl Default for ClientParams {
        fn default() -> Self {
            ClientParams::new(
                NonZeroUsize::new(DEFAULT_CLIENT_BUFFER_SIZE).unwrap(),
                Default::default(),
            )
        }
    }

    /// Basic [`Config`] implementation which allows for configuration to be specified by absolute
    /// path or host and provides a default fallback.
    #[derive(Clone, Debug, PartialEq)]
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

        pub fn try_from_value(value: Value, use_defaults: bool) -> Result<Self, ConfigParseError> {
            let (mut attrs, items) = match value {
                Value::Record(attrs, items) => (attrs, items),
                _ => return Err(ConfigParseError {}),
            };

            if attrs.pop().ok_or(ConfigParseError {})?.name == CONFIG_TAG {
                ConfigHierarchy::try_from_items(items, use_defaults)
            } else {
                Err(ConfigParseError {})
            }
        }

        fn try_from_items(items: Vec<Item>, use_defaults: bool) -> Result<Self, ConfigParseError> {
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
                                client_params =
                                    Some(ClientParams::try_from_items(items, use_defaults)?);
                            }
                            DOWNLINKS_TAG => {
                                downlink_params =
                                    Some(DownlinkParams::try_from_items(items, use_defaults)?);
                            }
                            HOST_TAG => {
                                for item in items {
                                    let (url, params) =
                                        try_host_params_from_item(item, use_defaults)?;
                                    host_params.insert(url, params);
                                }
                            }
                            LANE_TAG => {
                                for item in items {
                                    let (path, params) =
                                        try_lane_params_from_item(item, use_defaults)?;
                                    lane_params.insert(path, params);
                                }
                            }
                            _ => return Err(ConfigParseError {}),
                        }
                    }
                    _ => return Err(ConfigParseError {}),
                }
            }

            if client_params.is_none() && use_defaults {
                client_params = Some(ClientParams::default())
            }

            if downlink_params.is_none() && use_defaults {
                downlink_params = Some(DownlinkParams::default())
            }

            Ok(ConfigHierarchy {
                client_params: client_params.ok_or(ConfigParseError {})?,
                default: downlink_params.ok_or(ConfigParseError {})?,
                by_host: host_params,
                by_lane: lane_params,
            })
        }
    }

    fn try_host_params_from_item(
        item: Item,
        use_defaults: bool,
    ) -> Result<(Url, DownlinkParams), ConfigParseError> {
        match item {
            Item::Slot(Value::Text(name), Value::Record(_, items)) => {
                let host = Url::parse(&name).map_err(|_| ConfigParseError {})?;
                let downlink_params = DownlinkParams::try_from_items(items, use_defaults)?;
                Ok((host, downlink_params))
            }
            _ => Err(ConfigParseError {}),
        }
    }

    fn try_lane_params_from_item(
        item: Item,
        use_defaults: bool,
    ) -> Result<(AbsolutePath, DownlinkParams), ConfigParseError> {
        match item {
            Item::ValueItem(Value::Record(mut attrs, items)) => {
                let Attr { name, value } = attrs.pop().ok_or(ConfigParseError {})?;

                if name == PATH_TAG {
                    let path = try_absolute_path_from_record(value)?;
                    let downlink_params = DownlinkParams::try_from_items(items, use_defaults)?;
                    Ok((path, downlink_params))
                } else {
                    Err(ConfigParseError {})
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

        Ok(AbsolutePath::new(
            host.ok_or(ConfigParseError {})?,
            &node.ok_or(ConfigParseError {})?,
            &lane.ok_or(ConfigParseError {})?,
        ))
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
            let client_params = Default::default();
            let default_params = Default::default();

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
