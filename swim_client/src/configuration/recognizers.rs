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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;

use swim_common::form::structural::read::error::ExpectedEvent;
use swim_common::form::structural::read::event::ReadEvent;
use swim_common::form::structural::read::recognizer::impls::{
    AbsolutePathRecognizer, DurationRecognizer, RetryStrategyRecognizer, WebSocketConfigRecognizer,
};
use swim_common::form::structural::read::recognizer::{
    HashMapRecognizer, Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody, UrlRecognizer,
};
use swim_common::form::structural::read::ReadError;
use swim_common::model::text::Text;
use swim_common::model::ValueKind;
use swim_common::routing::remote::config::{
    RemoteConnectionsConfig, RemoteConnectionsConfigRecognizer,
};
use swim_common::warp::path::AbsolutePath;
use swim_utilities::future::retryable::RetryStrategy;

use crate::configuration::tags::{
    BACK_PRESSURE_TAG, BRIDGE_BUFFER_SIZE_TAG, BUFFER_SIZE_TAG, CONFIG_TAG, DEFAULT_TAG,
    DL_REQ_BUFFER_SIZE_TAG, DOWNLINKS_TAG, DOWNLINK_CONFIG_TAG, DOWNLINK_CONNECTIONS_TAG, HOST_TAG,
    IDLE_TIMEOUT_TAG, IGNORE_TAG, INPUT_BUFFER_SIZE_TAG, LANE_TAG, MAX_ACTIVE_KEYS_TAG,
    ON_INVALID_TAG, PROPAGATE_TAG, RELEASE_TAG, REMOTE_CONNECTIONS_TAG, RETRY_STRATEGY_TAG,
    TERMINATE_TAG, WEBSOCKET_CONNECTIONS_TAG, YIELD_AFTER_TAG,
};
use crate::configuration::{
    BackpressureMode, ClientDownlinksConfig, DownlinkConfig, DownlinkConnectionsConfig,
    OnInvalidMessage, SwimClientConfig, DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE,
    DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE, DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS,
    DEFAULT_BACK_PRESSURE_YIELD_AFTER, DEFAULT_BUFFER_SIZE, DEFAULT_DL_REQUEST_BUFFER_SIZE,
    DEFAULT_DOWNLINK_BUFFER_SIZE, DEFAULT_IDLE_TIMEOUT, DEFAULT_YIELD_AFTER,
};

impl RecognizerReadable for SwimClientConfig {
    type Rec = SwimClientConfigRecognizer;
    type AttrRec = SimpleAttrBody<SwimClientConfigRecognizer>;
    type BodyRec = SimpleRecBody<SwimClientConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        SwimClientConfigRecognizer {
            stage: SwimClientConfigStage::Init,
            downlink_connections: None,
            downlink_connections_recognizer: DownlinkConnectionsConfig::make_recognizer(),
            remote_connections: None,
            remote_connections_recognizer: RemoteConnectionsConfig::make_recognizer(),
            websocket_connections: None,
            websocket_connections_recognizer: WebSocketConfig::make_recognizer(),
            downlinks: None,
            downlinks_recognizer: ClientDownlinksConfig::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct SwimClientConfigRecognizer {
    stage: SwimClientConfigStage,
    downlink_connections: Option<DownlinkConnectionsConfig>,
    downlink_connections_recognizer: DownlinkConnectionsConfigRecognizer,
    remote_connections: Option<RemoteConnectionsConfig>,
    remote_connections_recognizer: RemoteConnectionsConfigRecognizer,
    websocket_connections: Option<WebSocketConfig>,
    websocket_connections_recognizer: WebSocketConfigRecognizer,
    downlinks: Option<ClientDownlinksConfig>,
    downlinks_recognizer: ClientDownlinksConfigRecognizer,
}

enum SwimClientConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Field(SwimClientConfigField),
}

#[derive(Clone, Copy)]
enum SwimClientConfigField {
    DownlinkConnections,
    RemoteConnections,
    WebsocketConnections,
    Downlinks,
}

impl Recognizer for SwimClientConfigRecognizer {
    type Target = SwimClientConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            SwimClientConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == CONFIG_TAG {
                        self.stage = SwimClientConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(CONFIG_TAG),
                    )))))
                }
            }
            SwimClientConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = SwimClientConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            SwimClientConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = SwimClientConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(SwimClientConfig::default()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            SwimClientConfigStage::InBody => match input {
                ReadEvent::StartAttribute(ref attr_name) => match attr_name.borrow() {
                    DOWNLINK_CONNECTIONS_TAG => {
                        self.stage = SwimClientConfigStage::Field(
                            SwimClientConfigField::DownlinkConnections,
                        );
                        self.downlink_connections_recognizer.feed_event(input);
                        None
                    }
                    REMOTE_CONNECTIONS_TAG => {
                        self.stage =
                            SwimClientConfigStage::Field(SwimClientConfigField::RemoteConnections);
                        self.remote_connections_recognizer.feed_event(input);
                        None
                    }
                    WEBSOCKET_CONNECTIONS_TAG => {
                        self.stage = SwimClientConfigStage::Field(
                            SwimClientConfigField::WebsocketConnections,
                        );
                        self.websocket_connections_recognizer.feed_event(input);
                        None
                    }
                    DOWNLINKS_TAG => {
                        self.stage = SwimClientConfigStage::Field(SwimClientConfigField::Downlinks);
                        self.downlinks_recognizer.feed_event(input);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(SwimClientConfig {
                    downlink_connections_config: self.downlink_connections.unwrap_or_default(),
                    remote_connections_config: self.remote_connections.unwrap_or_default(),
                    websocket_config: self.websocket_connections.unwrap_or_default(),
                    downlinks_config: self.downlinks.clone().unwrap_or_default(),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            SwimClientConfigStage::Field(SwimClientConfigField::DownlinkConnections) => {
                match self.downlink_connections_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.downlink_connections = Some(value);
                        self.stage = SwimClientConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            SwimClientConfigStage::Field(SwimClientConfigField::RemoteConnections) => {
                match self.remote_connections_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.remote_connections = Some(value);
                        self.stage = SwimClientConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            SwimClientConfigStage::Field(SwimClientConfigField::WebsocketConnections) => {
                match self.websocket_connections_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.websocket_connections = Some(value);
                        self.stage = SwimClientConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            SwimClientConfigStage::Field(SwimClientConfigField::Downlinks) => {
                match self.downlinks_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.downlinks = Some(value);
                        self.stage = SwimClientConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let SwimClientConfigRecognizer {
            stage,
            downlink_connections,
            downlink_connections_recognizer,
            remote_connections,
            remote_connections_recognizer,
            websocket_connections,
            websocket_connections_recognizer,
            downlinks,
            downlinks_recognizer,
        } = self;

        *stage = SwimClientConfigStage::Init;
        *downlink_connections = None;
        *remote_connections = None;
        *websocket_connections = None;
        *downlinks = None;

        downlink_connections_recognizer.reset();
        remote_connections_recognizer.reset();
        websocket_connections_recognizer.reset();
        downlinks_recognizer.reset();
    }
}

impl RecognizerReadable for DownlinkConnectionsConfig {
    type Rec = DownlinkConnectionsConfigRecognizer;
    type AttrRec = SimpleAttrBody<DownlinkConnectionsConfigRecognizer>;
    type BodyRec = SimpleRecBody<DownlinkConnectionsConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        DownlinkConnectionsConfigRecognizer {
            stage: DownlinkConnectionsConfigStage::Init,
            dl_buffer_size: None,
            buffer_size: None,
            yield_after: None,
            retry_strategy: None,
            retry_strategy_recognizer: RetryStrategy::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct DownlinkConnectionsConfigRecognizer {
    stage: DownlinkConnectionsConfigStage,
    dl_buffer_size: Option<NonZeroUsize>,
    buffer_size: Option<NonZeroUsize>,
    yield_after: Option<NonZeroUsize>,
    retry_strategy: Option<RetryStrategy>,
    retry_strategy_recognizer: RetryStrategyRecognizer,
}

enum DownlinkConnectionsConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(DownlinkConnectionsConfigField),
    Field(DownlinkConnectionsConfigField),
}

#[derive(Clone, Copy)]
enum DownlinkConnectionsConfigField {
    DlBufferSize,
    BufferSize,
    YieldAfter,
    RetryStrategy,
}

impl Recognizer for DownlinkConnectionsConfigRecognizer {
    type Target = DownlinkConnectionsConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            DownlinkConnectionsConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == DOWNLINK_CONNECTIONS_TAG {
                        self.stage = DownlinkConnectionsConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(DOWNLINK_CONNECTIONS_TAG),
                    )))))
                }
            }
            DownlinkConnectionsConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = DownlinkConnectionsConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            DownlinkConnectionsConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = DownlinkConnectionsConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(DownlinkConnectionsConfig::default()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            DownlinkConnectionsConfigStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    DL_REQ_BUFFER_SIZE_TAG => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::DlBufferSize,
                        );
                        None
                    }
                    BUFFER_SIZE_TAG => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::BufferSize,
                        );
                        None
                    }
                    YIELD_AFTER_TAG => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::YieldAfter,
                        );
                        None
                    }
                    RETRY_STRATEGY_TAG => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::RetryStrategy,
                        );
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(DownlinkConnectionsConfig {
                    dl_req_buffer_size: self
                        .dl_buffer_size
                        .unwrap_or(DEFAULT_DL_REQUEST_BUFFER_SIZE),
                    buffer_size: self.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
                    yield_after: self.yield_after.unwrap_or(DEFAULT_YIELD_AFTER),
                    retry_strategy: self.retry_strategy.unwrap_or_default(),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            DownlinkConnectionsConfigStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = DownlinkConnectionsConfigStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            DownlinkConnectionsConfigStage::Field(DownlinkConnectionsConfigField::DlBufferSize) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.dl_buffer_size = Some(value);
                        self.stage = DownlinkConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConnectionsConfigStage::Field(DownlinkConnectionsConfigField::BufferSize) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.buffer_size = Some(value);
                        self.stage = DownlinkConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConnectionsConfigStage::Field(DownlinkConnectionsConfigField::YieldAfter) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.yield_after = Some(value);
                        self.stage = DownlinkConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConnectionsConfigStage::Field(
                DownlinkConnectionsConfigField::RetryStrategy,
            ) => match self.retry_strategy_recognizer.feed_event(input)? {
                Ok(value) => {
                    self.retry_strategy = Some(value);
                    self.stage = DownlinkConnectionsConfigStage::InBody;
                    None
                }
                Err(err) => Some(Err(err)),
            },
        }
    }

    fn reset(&mut self) {
        let DownlinkConnectionsConfigRecognizer {
            stage,
            dl_buffer_size,
            buffer_size,
            yield_after,
            retry_strategy,
            retry_strategy_recognizer,
        } = self;

        *stage = DownlinkConnectionsConfigStage::Init;
        *dl_buffer_size = None;
        *buffer_size = None;
        *yield_after = None;
        *retry_strategy = None;

        retry_strategy_recognizer.reset();
    }
}

impl RecognizerReadable for ClientDownlinksConfig {
    type Rec = ClientDownlinksConfigRecognizer;
    type AttrRec = SimpleAttrBody<ClientDownlinksConfigRecognizer>;
    type BodyRec = SimpleRecBody<ClientDownlinksConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        ClientDownlinksConfigRecognizer {
            stage: ClientDownlinksConfigStage::Init,
            default: None,
            default_recognizer: DownlinkConfig::make_recognizer(),
            host: None,
            host_recognizer: HashMap::<Url, DownlinkConfig>::make_recognizer(),
            lane: None,
            lane_recognizer: HashMap::<AbsolutePath, DownlinkConfig>::make_recognizer(),
            absolute_path_recognizer: AbsolutePath::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct ClientDownlinksConfigRecognizer {
    stage: ClientDownlinksConfigStage,
    default: Option<DownlinkConfig>,
    default_recognizer: DownlinkConfigRecognizer,
    host: Option<HashMap<Url, DownlinkConfig>>,
    host_recognizer: HashMapRecognizer<UrlRecognizer, DownlinkConfigRecognizer>,
    lane: Option<HashMap<AbsolutePath, DownlinkConfig>>,
    lane_recognizer: HashMapRecognizer<AbsolutePathRecognizer, DownlinkConfigRecognizer>,
    absolute_path_recognizer: AbsolutePathRecognizer,
}

enum ClientDownlinksConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(ClientDownlinksConfigField),
    Field(ClientDownlinksConfigField),
}

#[derive(Clone, Copy)]
enum ClientDownlinksConfigField {
    Default,
    Host,
    Lane,
}

impl Recognizer for ClientDownlinksConfigRecognizer {
    type Target = ClientDownlinksConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            ClientDownlinksConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == DOWNLINKS_TAG {
                        self.stage = ClientDownlinksConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(DOWNLINKS_TAG),
                    )))))
                }
            }
            ClientDownlinksConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = ClientDownlinksConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            ClientDownlinksConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = ClientDownlinksConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(ClientDownlinksConfig::default()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            ClientDownlinksConfigStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    DEFAULT_TAG => {
                        self.stage =
                            ClientDownlinksConfigStage::Slot(ClientDownlinksConfigField::Default);
                        None
                    }
                    HOST_TAG => {
                        self.stage =
                            ClientDownlinksConfigStage::Slot(ClientDownlinksConfigField::Host);
                        None
                    }
                    LANE_TAG => {
                        self.stage =
                            ClientDownlinksConfigStage::Slot(ClientDownlinksConfigField::Lane);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(ClientDownlinksConfig {
                    default: self.default.unwrap_or_default(),
                    by_host: self.host.clone().unwrap_or_default(),
                    by_lane: self.lane.clone().unwrap_or_default(),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            ClientDownlinksConfigStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = ClientDownlinksConfigStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            ClientDownlinksConfigStage::Field(ClientDownlinksConfigField::Default) => {
                match self.default_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.default = Some(value);
                        self.stage = ClientDownlinksConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            ClientDownlinksConfigStage::Field(ClientDownlinksConfigField::Host) => {
                match self.host_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.host = Some(value);
                        self.stage = ClientDownlinksConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            ClientDownlinksConfigStage::Field(ClientDownlinksConfigField::Lane) => {
                match self.lane_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.lane = Some(value);
                        self.stage = ClientDownlinksConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let ClientDownlinksConfigRecognizer {
            stage,
            default,
            default_recognizer,
            host,
            host_recognizer,
            lane,
            lane_recognizer,
            absolute_path_recognizer,
        } = self;

        *stage = ClientDownlinksConfigStage::Init;
        *default = None;
        *host = None;
        *lane = None;

        default_recognizer.reset();
        host_recognizer.reset();
        lane_recognizer.reset();
        absolute_path_recognizer.reset();
    }
}

impl RecognizerReadable for DownlinkConfig {
    type Rec = DownlinkConfigRecognizer;
    type AttrRec = SimpleAttrBody<DownlinkConfigRecognizer>;
    type BodyRec = SimpleRecBody<DownlinkConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        DownlinkConfigRecognizer {
            stage: DownlinkConfigStage::Init,
            back_pressure: None,
            back_pressure_recognizer: BackpressureMode::make_recognizer(),
            idle_timeout: None,
            idle_timeout_recognizer: Duration::make_recognizer(),
            buffer_size: None,
            on_invalid: None,
            on_invalid_recognizer: OnInvalidMessage::make_recognizer(),
            yield_after: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct DownlinkConfigRecognizer {
    stage: DownlinkConfigStage,
    back_pressure: Option<BackpressureMode>,
    back_pressure_recognizer: BackpressureModeRecognizer,
    idle_timeout: Option<Duration>,
    idle_timeout_recognizer: DurationRecognizer,
    buffer_size: Option<NonZeroUsize>,
    on_invalid: Option<OnInvalidMessage>,
    on_invalid_recognizer: OnInvalidMessageRecognizer,
    yield_after: Option<NonZeroUsize>,
}

enum DownlinkConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(DownlinkConfigField),
    Field(DownlinkConfigField),
}

#[derive(Clone, Copy)]
enum DownlinkConfigField {
    BackPressure,
    IdleTimeout,
    BufferSize,
    OnInvalid,
    YieldAfter,
}

impl Recognizer for DownlinkConfigRecognizer {
    type Target = DownlinkConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            DownlinkConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == DOWNLINK_CONFIG_TAG {
                        self.stage = DownlinkConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(DOWNLINK_CONFIG_TAG),
                    )))))
                }
            }
            DownlinkConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = DownlinkConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            DownlinkConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = DownlinkConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(DownlinkConfig::default()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            DownlinkConfigStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    BACK_PRESSURE_TAG => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::BackPressure);
                        None
                    }
                    IDLE_TIMEOUT_TAG => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::IdleTimeout);
                        None
                    }
                    BUFFER_SIZE_TAG => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::BufferSize);
                        None
                    }
                    ON_INVALID_TAG => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::OnInvalid);
                        None
                    }
                    YIELD_AFTER_TAG => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::YieldAfter);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(DownlinkConfig {
                    back_pressure: self.back_pressure.unwrap_or_default(),
                    idle_timeout: self.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT),
                    buffer_size: self.buffer_size.unwrap_or(DEFAULT_DOWNLINK_BUFFER_SIZE),
                    on_invalid: self.on_invalid.unwrap_or_default(),
                    yield_after: self.yield_after.unwrap_or(DEFAULT_YIELD_AFTER),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            DownlinkConfigStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = DownlinkConfigStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            DownlinkConfigStage::Field(DownlinkConfigField::BackPressure) => {
                match self.back_pressure_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.back_pressure = Some(value);
                        self.stage = DownlinkConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConfigStage::Field(DownlinkConfigField::IdleTimeout) => {
                match self.idle_timeout_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.idle_timeout = Some(value);
                        self.stage = DownlinkConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConfigStage::Field(DownlinkConfigField::BufferSize) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.buffer_size = Some(value);
                        self.stage = DownlinkConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConfigStage::Field(DownlinkConfigField::OnInvalid) => {
                match self.on_invalid_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.on_invalid = Some(value);
                        self.stage = DownlinkConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            DownlinkConfigStage::Field(DownlinkConfigField::YieldAfter) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.yield_after = Some(value);
                        self.stage = DownlinkConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let DownlinkConfigRecognizer {
            stage,
            back_pressure,
            back_pressure_recognizer,
            idle_timeout,
            idle_timeout_recognizer,
            buffer_size,
            on_invalid,
            on_invalid_recognizer,
            yield_after,
        } = self;

        *stage = DownlinkConfigStage::Init;
        *back_pressure = None;
        *idle_timeout = None;
        *buffer_size = None;
        *on_invalid = None;
        *yield_after = None;

        back_pressure_recognizer.reset();
        idle_timeout_recognizer.reset();
        on_invalid_recognizer.reset();
    }
}

impl RecognizerReadable for BackpressureMode {
    type Rec = BackpressureModeRecognizer;
    type AttrRec = SimpleAttrBody<BackpressureModeRecognizer>;
    type BodyRec = SimpleRecBody<BackpressureModeRecognizer>;

    fn make_recognizer() -> Self::Rec {
        BackpressureModeRecognizer {
            stage: BackpressureModeStage::Init,
            fields: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(BackpressureModeRecognizer {
            stage: BackpressureModeStage::Init,
            fields: None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(BackpressureModeRecognizer {
            stage: BackpressureModeStage::Init,
            fields: None,
        })
    }
}

pub struct BackpressureModeRecognizer {
    stage: BackpressureModeStage,
    fields: Option<BackpressureModeFields>,
}

enum BackpressureModeStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(BackpressureModeField),
    Field(BackpressureModeField),
}

#[derive(Clone, Copy)]
enum BackpressureModeField {
    InputBufferSize,
    BridgeBufferSize,
    MaxActiveKeys,
    YieldAfter,
}

struct BackpressureModeFields {
    input_buffer_size: Option<NonZeroUsize>,
    bridge_buffer_size: Option<NonZeroUsize>,
    max_active_keys: Option<NonZeroUsize>,
    yield_after: Option<NonZeroUsize>,
}

impl Recognizer for BackpressureModeRecognizer {
    type Target = BackpressureMode;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            BackpressureModeStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    match name.borrow() {
                        PROPAGATE_TAG => {
                            self.stage = BackpressureModeStage::Tag;
                            None
                        }

                        RELEASE_TAG => {
                            self.stage = BackpressureModeStage::Tag;
                            self.fields = Some(BackpressureModeFields {
                                input_buffer_size: None,
                                bridge_buffer_size: None,
                                max_active_keys: None,
                                yield_after: None,
                            });
                            None
                        }

                        _ => Some(Err(ReadError::UnexpectedAttribute(name.into()))),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::Attribute(Some(Text::new(PROPAGATE_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(RELEASE_TAG))),
                    ]))))
                }
            }
            BackpressureModeStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = BackpressureModeStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },

            BackpressureModeStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = BackpressureModeStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    match self.fields {
                        Some(BackpressureModeFields { .. }) => {
                            Some(Ok(BackpressureMode::Release {
                                input_buffer_size: DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE,
                                bridge_buffer_size: DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE,
                                max_active_keys: DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS,
                                yield_after: DEFAULT_BACK_PRESSURE_YIELD_AFTER,
                            }))
                        }
                        None => Some(Ok(BackpressureMode::Propagate)),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            BackpressureModeStage::InBody => match self.fields {
                Some(BackpressureModeFields {
                    input_buffer_size,
                    bridge_buffer_size,
                    max_active_keys,
                    yield_after,
                }) => match input {
                    ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                        INPUT_BUFFER_SIZE_TAG => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::InputBufferSize);
                            None
                        }
                        BRIDGE_BUFFER_SIZE_TAG => {
                            self.stage = BackpressureModeStage::Slot(
                                BackpressureModeField::BridgeBufferSize,
                            );
                            None
                        }
                        MAX_ACTIVE_KEYS_TAG => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::MaxActiveKeys);
                            None
                        }
                        YIELD_AFTER_TAG => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::YieldAfter);
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(BackpressureMode::Release {
                        input_buffer_size: input_buffer_size
                            .unwrap_or(DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE),
                        bridge_buffer_size: bridge_buffer_size
                            .unwrap_or(DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE),
                        max_active_keys: max_active_keys
                            .unwrap_or(DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS),
                        yield_after: yield_after.unwrap_or(DEFAULT_BACK_PRESSURE_YIELD_AFTER),
                    })),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::Text),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
                None => match input {
                    ReadEvent::EndRecord => Some(Ok(BackpressureMode::Propagate)),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfRecord))),
                },
            },
            BackpressureModeStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = BackpressureModeStage::Field(fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            BackpressureModeStage::Field(field) => match &mut self.fields {
                None => None,
                Some(BackpressureModeFields {
                    input_buffer_size,
                    bridge_buffer_size,
                    max_active_keys,
                    yield_after,
                }) => match field {
                    BackpressureModeField::InputBufferSize => {
                        match NonZeroUsize::make_recognizer().feed_event(input)? {
                            Ok(value) => {
                                *input_buffer_size = Some(value);
                                self.stage = BackpressureModeStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    BackpressureModeField::BridgeBufferSize => {
                        match NonZeroUsize::make_recognizer().feed_event(input)? {
                            Ok(value) => {
                                *bridge_buffer_size = Some(value);
                                self.stage = BackpressureModeStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    BackpressureModeField::MaxActiveKeys => {
                        match NonZeroUsize::make_recognizer().feed_event(input)? {
                            Ok(value) => {
                                *max_active_keys = Some(value);
                                self.stage = BackpressureModeStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    BackpressureModeField::YieldAfter => {
                        match NonZeroUsize::make_recognizer().feed_event(input)? {
                            Ok(value) => {
                                *yield_after = Some(value);
                                self.stage = BackpressureModeStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                },
            },
        }
    }

    fn reset(&mut self) {
        self.stage = BackpressureModeStage::Init;
        self.fields = None;
    }
}

impl RecognizerReadable for OnInvalidMessage {
    type Rec = OnInvalidMessageRecognizer;
    type AttrRec = SimpleAttrBody<OnInvalidMessageRecognizer>;
    type BodyRec = SimpleRecBody<OnInvalidMessageRecognizer>;

    fn make_recognizer() -> Self::Rec {
        OnInvalidMessageRecognizer {
            stage: OnInvalidMessageStage::Init,
            fields: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct OnInvalidMessageRecognizer {
    stage: OnInvalidMessageStage,
    fields: Option<OnInvalidMessageFields>,
}

enum OnInvalidMessageStage {
    Init,
    Tag,
    AfterTag,
    InBody,
}
enum OnInvalidMessageFields {
    Ignore,
    Terminate,
}

impl Recognizer for OnInvalidMessageRecognizer {
    type Target = OnInvalidMessage;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            OnInvalidMessageStage::Init => match input {
                ReadEvent::StartAttribute(ref value) => match value.borrow() {
                    IGNORE_TAG => {
                        self.fields = Some(OnInvalidMessageFields::Ignore);
                        self.stage = OnInvalidMessageStage::Tag;
                        None
                    }
                    TERMINATE_TAG => {
                        self.fields = Some(OnInvalidMessageFields::Terminate);
                        self.stage = OnInvalidMessageStage::Tag;
                        None
                    }
                    _ => Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::Attribute(Some(Text::new(IGNORE_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(TERMINATE_TAG))),
                    ])))),
                },
                _ => Some(Err(
                    input.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
                )),
            },
            OnInvalidMessageStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = OnInvalidMessageStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            OnInvalidMessageStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = OnInvalidMessageStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    match self.fields.as_ref()? {
                        OnInvalidMessageFields::Terminate => Some(Ok(OnInvalidMessage::Terminate)),
                        OnInvalidMessageFields::Ignore => Some(Ok(OnInvalidMessage::Ignore)),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            OnInvalidMessageStage::InBody => match input {
                ReadEvent::EndRecord => match self.fields.as_ref()? {
                    OnInvalidMessageFields::Terminate => Some(Ok(OnInvalidMessage::Terminate)),
                    OnInvalidMessageFields::Ignore => Some(Ok(OnInvalidMessage::Ignore)),
                },
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfRecord))),
            },
        }
    }

    fn reset(&mut self) {
        let OnInvalidMessageRecognizer { stage, fields } = self;

        *stage = OnInvalidMessageStage::Init;
        *fields = None;
    }
}
