use std::borrow::Borrow;
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
use swim_common::form::structural::read::error::ExpectedEvent;
use swim_common::form::structural::read::event::ReadEvent;
use swim_common::form::structural::read::recognizer::{
    AbsolutePathRecognizer, DurationRecognizer, HashMapRecognizer, Recognizer, RecognizerReadable,
    RetryStrategyRecognizer, SimpleAttrBody, SimpleRecBody, UrlRecognizer,
    WebSocketConfigRecognizer,
};
use swim_common::form::structural::read::ReadError;
use swim_common::form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_common::model::parser::ParseFailure;
use swim_common::model::text::Text;
use swim_common::model::ValueKind;
use swim_common::routing::remote::config::{
    RemoteConnectionsConfig, RemoteConnectionsConfigRecognizer,
};
use swim_common::warp::path::{AbsolutePath, Addressable};
use swim_utilities::future::retryable::strategy::RetryStrategy;
use thiserror::Error;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;

#[cfg(test)]
mod tests;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60000);
const DEFAULT_DOWNLINK_BUFFER_SIZE: usize = 32;
const DEFAULT_YIELD_AFTER: usize = 256;
const DEFAULT_BUFFER_SIZE: usize = 128;
const DEFAULT_DL_REQUEST_BUFFER_SIZE: usize = 8;
const DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE: usize = 32;
const DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE: usize = 16;
const DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS: usize = 16;
const DEFAULT_BACK_PRESSURE_YIELD_AFTER: usize = 256;

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

impl StructuralWritable for SwimClientConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr("config")?
            .complete_header(RecordBodyKind::ArrayLike, 4)?;

        body_writer = body_writer.write_value(&self.downlink_connections_config)?;
        body_writer = body_writer.write_value(&self.remote_connections_config)?;
        body_writer = body_writer.write_value(&self.websocket_config)?;
        body_writer = body_writer.write_value(&self.downlinks_config)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr("config")?
            .complete_header(RecordBodyKind::ArrayLike, 4)?;

        body_writer = body_writer.write_value_into(self.downlink_connections_config)?;
        body_writer = body_writer.write_value_into(self.remote_connections_config)?;
        body_writer = body_writer.write_value_into(self.websocket_config)?;
        body_writer = body_writer.write_value_into(self.downlinks_config)?;

        body_writer.done()
    }
}

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

const SWIM_CLIENT_CONFIG_TAG: &str = "config";

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

impl Recognizer for SwimClientConfigRecognizer {
    type Target = SwimClientConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            SwimClientConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == SWIM_CLIENT_CONFIG_TAG {
                        self.stage = SwimClientConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(SWIM_CLIENT_CONFIG_TAG),
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
                    "downlink_connections" => {
                        self.stage = SwimClientConfigStage::Field(
                            SwimClientConfigField::DownlinkConnections,
                        );
                        self.downlink_connections_recognizer.feed_event(input);
                        None
                    }
                    "remote_connections" => {
                        self.stage =
                            SwimClientConfigStage::Field(SwimClientConfigField::RemoteConnections);
                        self.remote_connections_recognizer.feed_event(input);
                        None
                    }
                    "websocket_connections" => {
                        self.stage = SwimClientConfigStage::Field(
                            SwimClientConfigField::WebsocketConnections,
                        );
                        self.websocket_connections_recognizer.feed_event(input);
                        None
                    }
                    "downlinks" => {
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

/// Configuration parameters for the router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DownlinkConnectionsConfig {
    /// Buffer size for servicing requests for new downlinks.
    pub dl_req_buffer_size: NonZeroUsize,
    /// Size of the internal buffers of the downlinks connections task.
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

impl StructuralWritable for DownlinkConnectionsConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr("downlink_connections")?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot(&"dl_req_buffer_size", &self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot(&"buffer_size", &self.buffer_size)?;
        body_writer = body_writer.write_slot(&"yield_after", &self.yield_after)?;
        body_writer = body_writer.write_slot(&"retry_strategy", &self.retry_strategy)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr("downlink_connections")?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot_into("dl_req_buffer_size", self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot_into("buffer_size", self.buffer_size)?;
        body_writer = body_writer.write_slot_into("yield_after", self.yield_after)?;
        body_writer = body_writer.write_slot_into("retry_strategy", self.retry_strategy)?;

        body_writer.done()
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

const DOWNLINK_CONNECTIONS_TAG: &str = "downlink_connections";

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

pub struct DownlinkConnectionsConfigRecognizer {
    stage: DownlinkConnectionsConfigStage,
    dl_buffer_size: Option<NonZeroUsize>,
    buffer_size: Option<NonZeroUsize>,
    yield_after: Option<NonZeroUsize>,
    retry_strategy: Option<RetryStrategy>,
    retry_strategy_recognizer: RetryStrategyRecognizer,
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
                    "dl_req_buffer_size" => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::DlBufferSize,
                        );
                        None
                    }
                    "buffer_size" => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::BufferSize,
                        );
                        None
                    }
                    "yield_after" => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::YieldAfter,
                        );
                        None
                    }
                    "retry_strategy" => {
                        self.stage = DownlinkConnectionsConfigStage::Slot(
                            DownlinkConnectionsConfigField::RetryStrategy,
                        );
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(DownlinkConnectionsConfig {
                    dl_req_buffer_size: self.dl_buffer_size.unwrap_or_else(|| {
                        NonZeroUsize::new(DEFAULT_DL_REQUEST_BUFFER_SIZE).unwrap()
                    }),
                    buffer_size: self
                        .buffer_size
                        .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_BUFFER_SIZE).unwrap()),
                    yield_after: self
                        .yield_after
                        .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap()),
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

impl StructuralWritable for ClientDownlinksConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 1;
        if !self.by_host.is_empty() {
            num_items += 1;
        }

        if !self.by_lane.is_empty() {
            num_items += 1;
        }

        let mut body_writer = header_writer
            .write_extant_attr("downlinks")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot(&"default", &self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot(&"host", &self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot(&"lane", &self.by_lane)?;
        }

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut num_items = 1;
        if !self.by_host.is_empty() {
            num_items += 1;
        }

        if !self.by_lane.is_empty() {
            num_items += 1;
        }

        let mut body_writer = header_writer
            .write_extant_attr("downlinks")?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer = body_writer.write_slot_into("default", self.default)?;

        if !self.by_host.is_empty() {
            body_writer = body_writer.write_slot_into("host", self.by_host)?;
        }

        if !self.by_lane.is_empty() {
            body_writer = body_writer.write_slot_into("lane", self.by_lane)?;
        }

        body_writer.done()
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

const CLIENT_DOWNLINKS_CONFIG_TAG: &str = "downlinks";

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
                    if name == CLIENT_DOWNLINKS_CONFIG_TAG {
                        self.stage = ClientDownlinksConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(CLIENT_DOWNLINKS_CONFIG_TAG),
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
                    "default" => {
                        self.stage =
                            ClientDownlinksConfigStage::Slot(ClientDownlinksConfigField::Default);
                        None
                    }
                    "host" => {
                        self.stage =
                            ClientDownlinksConfigStage::Slot(ClientDownlinksConfigField::Host);
                        None
                    }
                    "lane" => {
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

/// Configuration parameters for a single downlink.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DownlinkConfig {
    /// Whether the downlink propagates back-pressure.
    pub back_pressure: BackpressureMode,
    /// Timeout after which an idle downlink will be closed.
    /// Todo #412 (not yet implemented).
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
        buffer_size: NonZeroUsize,
        on_invalid: OnInvalidMessage,
        yield_after: NonZeroUsize,
    ) -> DownlinkConfig {
        DownlinkConfig {
            back_pressure,
            idle_timeout,
            buffer_size,
            on_invalid,
            yield_after,
        }
    }
}

impl From<&DownlinkConfig> for DownlinkConfig {
    fn from(conf: &DownlinkConfig) -> Self {
        *conf
    }
}

impl Default for DownlinkConfig {
    fn default() -> Self {
        DownlinkConfig::new(
            BackpressureMode::default(),
            DEFAULT_IDLE_TIMEOUT,
            NonZeroUsize::new(DEFAULT_DOWNLINK_BUFFER_SIZE).unwrap(),
            OnInvalidMessage::default(),
            NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap(),
        )
    }
}

impl StructuralWritable for DownlinkConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr("downlink_config")?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot(&"back_pressure", &self.back_pressure)?;
        body_writer = body_writer.write_slot(&"idle_timeout", &self.idle_timeout)?;
        body_writer = body_writer.write_slot(&"buffer_size", &self.buffer_size)?;
        body_writer = body_writer.write_slot(&"on_invalid", &self.on_invalid)?;
        body_writer = body_writer.write_slot(&"yield_after", &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr("downlink_config")?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot_into("back_pressure", self.back_pressure)?;
        body_writer = body_writer.write_slot_into("idle_timeout", self.idle_timeout)?;
        body_writer = body_writer.write_slot_into("buffer_size", self.buffer_size)?;
        body_writer = body_writer.write_slot_into("on_invalid", self.on_invalid)?;
        body_writer = body_writer.write_slot_into("yield_after", self.yield_after)?;

        body_writer.done()
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

const DOWNLINK_CONFIG_TAG: &str = "downlink_config";

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
                    "back_pressure" => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::BackPressure);
                        None
                    }
                    "idle_timeout" => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::IdleTimeout);
                        None
                    }
                    "buffer_size" => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::BufferSize);
                        None
                    }
                    "on_invalid" => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::OnInvalid);
                        None
                    }
                    "yield_after" => {
                        self.stage = DownlinkConfigStage::Slot(DownlinkConfigField::YieldAfter);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(DownlinkConfig {
                    back_pressure: self.back_pressure.unwrap_or_default(),
                    idle_timeout: self.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT),
                    buffer_size: self.buffer_size.unwrap_or_else(|| {
                        NonZeroUsize::new(DEFAULT_DOWNLINK_BUFFER_SIZE).unwrap()
                    }),
                    on_invalid: self.on_invalid.unwrap_or_default(),
                    yield_after: self
                        .yield_after
                        .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_YIELD_AFTER).unwrap()),
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// Mode indicating whether or not the downlink propagates back-pressure.
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

impl Default for BackpressureMode {
    fn default() -> Self {
        BackpressureMode::Propagate
    }
}

impl StructuralWritable for BackpressureMode {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr("propagate")?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("release")?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer = body_writer.write_slot(&"input_buffer_size", input_buffer_size)?;
                body_writer = body_writer.write_slot(&"bridge_buffer_size", bridge_buffer_size)?;
                body_writer = body_writer.write_slot(&"max_active_keys", max_active_keys)?;
                body_writer = body_writer.write_slot(&"yield_after", yield_after)?;

                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr("propagate")?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr("release")?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer =
                    body_writer.write_slot_into("input_buffer_size", input_buffer_size)?;
                body_writer =
                    body_writer.write_slot_into("bridge_buffer_size", bridge_buffer_size)?;
                body_writer = body_writer.write_slot_into("max_active_keys", max_active_keys)?;
                body_writer = body_writer.write_slot_into("yield_after", yield_after)?;

                body_writer.done()
            }
        }
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

const PROPAGATE_TAG: &str = "propagate";
const RELEASE_TAG: &str = "release";

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

pub struct BackpressureModeRecognizer {
    stage: BackpressureModeStage,
    fields: Option<BackpressureModeFields>,
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
                                input_buffer_size: NonZeroUsize::new(
                                    DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE,
                                )
                                .unwrap(),
                                bridge_buffer_size: NonZeroUsize::new(
                                    DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE,
                                )
                                .unwrap(),
                                max_active_keys: NonZeroUsize::new(
                                    DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS,
                                )
                                .unwrap(),
                                yield_after: NonZeroUsize::new(DEFAULT_BACK_PRESSURE_YIELD_AFTER)
                                    .unwrap(),
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
                        "input_buffer_size" => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::InputBufferSize);
                            None
                        }
                        "bridge_buffer_size" => {
                            self.stage = BackpressureModeStage::Slot(
                                BackpressureModeField::BridgeBufferSize,
                            );
                            None
                        }
                        "max_active_keys" => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::MaxActiveKeys);
                            None
                        }
                        "yield_after" => {
                            self.stage =
                                BackpressureModeStage::Slot(BackpressureModeField::YieldAfter);
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(BackpressureMode::Release {
                        input_buffer_size: input_buffer_size.unwrap_or_else(|| {
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE).unwrap()
                        }),
                        bridge_buffer_size: bridge_buffer_size.unwrap_or_else(|| {
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE).unwrap()
                        }),
                        max_active_keys: max_active_keys.unwrap_or_else(|| {
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS).unwrap()
                        }),
                        yield_after: yield_after.unwrap_or_else(|| {
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_YIELD_AFTER).unwrap()
                        }),
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

/// Instruction on how to respond when an invalid message is received for a downlink.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnInvalidMessage {
    /// Disregard the message and continue.
    Ignore,
    /// Terminate the downlink.
    Terminate,
}

impl Default for OnInvalidMessage {
    fn default() -> Self {
        OnInvalidMessage::Terminate
    }
}

impl StructuralWritable for OnInvalidMessage {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            OnInvalidMessage::Ignore => writer.write_text("ignore"),
            OnInvalidMessage::Terminate => writer.write_text("terminate"),
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            OnInvalidMessage::Ignore => writer.write_text("ignore"),
            OnInvalidMessage::Terminate => writer.write_text("terminate"),
        }
    }
}

impl RecognizerReadable for OnInvalidMessage {
    type Rec = OnInvalidMessageRecognizer;
    type AttrRec = SimpleAttrBody<OnInvalidMessageRecognizer>;
    type BodyRec = SimpleRecBody<OnInvalidMessageRecognizer>;

    fn make_recognizer() -> Self::Rec {
        OnInvalidMessageRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(OnInvalidMessageRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(OnInvalidMessageRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

const ON_INVALID_IGNORE_TAG: &str = "ignore";
const ON_INVALID_TERMINATE_TAG: &str = "terminate";

pub struct OnInvalidMessageRecognizer;

impl Recognizer for OnInvalidMessageRecognizer {
    type Target = OnInvalidMessage;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(ref value) => match value.borrow() {
                ON_INVALID_IGNORE_TAG => Some(Ok(OnInvalidMessage::Ignore)),
                ON_INVALID_TERMINATE_TAG => Some(Ok(OnInvalidMessage::Terminate)),
                _ => Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::Attribute(Some(Text::new(ON_INVALID_IGNORE_TAG))),
                    ExpectedEvent::Attribute(Some(Text::new(ON_INVALID_TERMINATE_TAG))),
                ])))),
            },
            _ => Some(Err(
                input.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
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
    fn for_lane(&mut self, lane: &Self::PathType, params: DownlinkConfig);
}

impl<'a, Path: Addressable> DownlinksConfig for Box<dyn DownlinksConfig<PathType = Path> + 'a> {
    type PathType = Path;

    fn config_for(&self, path: &Self::PathType) -> DownlinkConfig {
        (**self).config_for(path)
    }

    fn for_host(&mut self, host: Url, params: DownlinkConfig) {
        (**self).for_host(host, params)
    }

    fn for_lane(&mut self, lane: &Path, params: DownlinkConfig) {
        (**self).for_lane(lane, params)
    }
}

#[derive(Debug, Error)]
#[error("Could not process client configuration: {0}")]
pub enum ConfigError {
    File(std::io::Error),
    Parse(ParseFailure),
    Recognizer(ReadError),
}
