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

use url::Url;

use swim_form::structural::read::error::ExpectedEvent;
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::impls::{
    AbsolutePathRecognizer, DurationRecognizer, RetryStrategyRecognizer,
};
use swim_form::structural::read::recognizer::{
    HashMapRecognizer, Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody, UrlRecognizer,
};
use swim_form::structural::read::ReadError;
use swim_model::path::AbsolutePath;
use swim_model::{Text, ValueKind};
use swim_runtime::configuration::{WebSocketConfig, WebSocketConfigRecognizer};
use swim_runtime::configuration::recognizers::{DownlinkConfigRecognizer, DownlinkConnectionsConfigRecognizer};
use swim_runtime::remote::config::{RemoteConnectionsConfig, RemoteConnectionsConfigRecognizer};
use swim_utilities::future::retryable::RetryStrategy;

use crate::configuration::tags::{
    CONFIG_TAG, DEFAULT_TAG,
    DOWNLINKS_TAG, HOST_TAG,
    LANE_TAG, REMOTE_CONNECTIONS_TAG,
WEBSOCKET_CONNECTIONS_TAG,
};
use crate::configuration::{
    ClientDownlinksConfig, SwimClientConfig,
    DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE, DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE,
    DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS, DEFAULT_BACK_PRESSURE_YIELD_AFTER, DEFAULT_BUFFER_SIZE,
    DEFAULT_DL_REQUEST_BUFFER_SIZE, DEFAULT_DOWNLINK_BUFFER_SIZE, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_YIELD_AFTER,
};
use swim_runtime::configuration::{
    BackpressureMode, DownlinkConfig, DownlinkConnectionsConfig, OnInvalidMessage,
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
                ReadEvent::EndRecord => {
                    let ws_conf = match self.websocket_connections.as_ref() {
                        Some(conf) => (*conf).into(),
                        _ => Default::default(),
                    };
                    Some(Ok(SwimClientConfig {
                        downlink_connections_config: self.downlink_connections.unwrap_or_default(),
                        remote_connections_config: self.remote_connections.unwrap_or_default(),
                        websocket_config: ws_conf,
                        downlinks_config: self.downlinks.clone().unwrap_or_default(),
                    }))
                },
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
