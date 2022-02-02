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

use flate2::Compression;
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use swim_form::structural::read::error::ExpectedEvent;
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::primitive::{U32Recognizer, UsizeRecognizer};
use swim_form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use swim_form::structural::read::ReadError;
use swim_form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_model::path::Addressable;
use swim_model::{Text, ValueKind};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::retryable::RetryStrategy;

use crate::ws::CompressionSwitcherProvider;
use ratchet::deflate::{DeflateConfig, DeflateExtProvider};
use ratchet::WebSocketConfig as RatchetConfig;
use url::Url;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_DOWNLINK_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);
const DEFAULT_YIELD_AFTER: NonZeroUsize = non_zero_usize!(256);
const DEFAULT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(128);
const DEFAULT_DL_REQUEST_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(8);
const DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);
const DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(16);
const DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS: NonZeroUsize = non_zero_usize!(16);
const DEFAULT_BACK_PRESSURE_YIELD_AFTER: NonZeroUsize = non_zero_usize!(256);
const DEFAULT_BACK_PRESSURE_KEY_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);
const WEB_SOCKET_CONFIG_TAG: &str = "websocket_connections";
const WS_COMPRESSION_NONE_TAG: &str = "none";
const WS_COMPRESSION_DEFLATE_TAG: &str = "deflate";
const NONE_TAG: &str = "none";
const DEFLATE_TAG: &str = "deflate";
const WEBSOCKET_CONNECTIONS_TAG: &str = "websocket_connections";
const MAX_MESSAGE_SIZE_TAG: &str = "max_message_size";
const COMPRESSION_TAG: &str = "compression";
const DL_REQ_BUFFER_SIZE_TAG: &str = "dl_req_buffer_size";
const BUFFER_SIZE_TAG: &str = "buffer_size";
const YIELD_AFTER_TAG: &str = "yield_after";
const PER_KEY_BUFFER_SIZE_TAG: &str = "per_key_buffer_size";
const RETRY_STRATEGY_TAG: &str = "retry_strategy";
const BACK_PRESSURE_TAG: &str = "back_pressure";
const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
const ON_INVALID_TAG: &str = "on_invalid";
const PROPAGATE_TAG: &str = "propagate";
const RELEASE_TAG: &str = "release";
const INPUT_BUFFER_SIZE_TAG: &str = "input_buffer_size";
const BRIDGE_BUFFER_SIZE_TAG: &str = "bridge_buffer_size";
const MAX_ACTIVE_KEYS_TAG: &str = "max_active_keys";
const IGNORE_TAG: &str = "ignore";
const TERMINATE_TAG: &str = "terminate";
const DOWNLINK_CONFIG_TAG: &str = "downlink_config";
pub const DOWNLINK_CONNECTIONS_TAG: &str = "downlink_connections";

pub mod recognizers;

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
            dl_req_buffer_size: DEFAULT_DL_REQUEST_BUFFER_SIZE,
            retry_strategy: RetryStrategy::default(),
            buffer_size: DEFAULT_BUFFER_SIZE,
            yield_after: DEFAULT_YIELD_AFTER,
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
            .write_extant_attr(DOWNLINK_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer = body_writer.write_slot(&DL_REQ_BUFFER_SIZE_TAG, &self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot(&BUFFER_SIZE_TAG, &self.buffer_size)?;
        body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, &self.yield_after)?;
        body_writer = body_writer.write_slot(&RETRY_STRATEGY_TAG, &self.retry_strategy)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 4)?;

        body_writer =
            body_writer.write_slot_into(DL_REQ_BUFFER_SIZE_TAG, self.dl_req_buffer_size)?;
        body_writer = body_writer.write_slot_into(BUFFER_SIZE_TAG, self.buffer_size)?;
        body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, self.yield_after)?;
        body_writer = body_writer.write_slot_into(RETRY_STRATEGY_TAG, self.retry_strategy)?;

        body_writer.done()
    }
}

impl RecognizerReadable for DownlinkConnectionsConfig {
    type Rec = recognizers::DownlinkConnectionsConfigRecognizer;
    type AttrRec = SimpleAttrBody<recognizers::DownlinkConnectionsConfigRecognizer>;
    type BodyRec = SimpleRecBody<recognizers::DownlinkConnectionsConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        recognizers::DownlinkConnectionsConfigRecognizer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
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
            DEFAULT_DOWNLINK_BUFFER_SIZE,
            OnInvalidMessage::default(),
            DEFAULT_YIELD_AFTER,
        )
    }
}

impl RecognizerReadable for DownlinkConfig {
    type Rec = recognizers::DownlinkConfigRecognizer;
    type AttrRec = SimpleAttrBody<recognizers::DownlinkConfigRecognizer>;
    type BodyRec = SimpleRecBody<recognizers::DownlinkConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        recognizers::DownlinkConfigRecognizer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
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
        /// Size of the circular buffer to use for each key of a map downlink.
        per_key_buffer_size: NonZeroUsize,
    },
}

impl Default for BackpressureMode {
    fn default() -> Self {
        BackpressureMode::Propagate
    }
}

impl RecognizerReadable for BackpressureMode {
    type Rec = recognizers::BackpressureModeRecognizer;
    type AttrRec = SimpleAttrBody<recognizers::BackpressureModeRecognizer>;
    type BodyRec = SimpleRecBody<recognizers::BackpressureModeRecognizer>;

    fn make_recognizer() -> Self::Rec {
        recognizers::BackpressureModeRecognizer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

impl RecognizerReadable for OnInvalidMessage {
    type Rec = recognizers::OnInvalidMessageRecognizer;
    type AttrRec = SimpleAttrBody<recognizers::OnInvalidMessageRecognizer>;
    type BodyRec = SimpleRecBody<recognizers::OnInvalidMessageRecognizer>;

    fn make_recognizer() -> Self::Rec {
        recognizers::OnInvalidMessageRecognizer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
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

#[derive(Clone, Debug, PartialEq)]
pub struct WebSocketConfig {
    pub config: RatchetConfig,
    pub compression: CompressionSwitcherProvider,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        WebSocketConfig {
            config: RatchetConfig::default(),
            compression: CompressionSwitcherProvider::Off,
        }
    }
}

impl From<RatchetConfig> for WebSocketConfig {
    fn from(config: RatchetConfig) -> Self {
        WebSocketConfig {
            config,
            compression: CompressionSwitcherProvider::Off,
        }
    }
}

impl RecognizerReadable for WebSocketConfig {
    type Rec = WebSocketConfigRecognizer;
    type AttrRec = SimpleAttrBody<WebSocketConfigRecognizer>;
    type BodyRec = SimpleRecBody<WebSocketConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        WebSocketConfigRecognizer {
            stage: WebSocketConfigStage::Init,
            max_message_size: None,
            compression: CompressionSwitcherProvider::Off,
            compression_recognizer: WsCompression::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }
}

pub struct WebSocketConfigRecognizer {
    stage: WebSocketConfigStage,
    max_message_size: Option<usize>,
    compression: CompressionSwitcherProvider,
    compression_recognizer: WsCompressionRecognizer,
}

enum WebSocketConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(WebSocketConfigField),
    Field(WebSocketConfigField),
}

#[derive(Clone, Copy)]
enum WebSocketConfigField {
    MaxMessageSize,
    Compression,
}

impl Recognizer for WebSocketConfigRecognizer {
    type Target = WebSocketConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            WebSocketConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == WEB_SOCKET_CONFIG_TAG {
                        self.stage = WebSocketConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(WEB_SOCKET_CONFIG_TAG),
                    )))))
                }
            }
            WebSocketConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = WebSocketConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            WebSocketConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = WebSocketConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(WebSocketConfig {
                        config: RatchetConfig::default(),
                        compression: CompressionSwitcherProvider::Off,
                    }))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            WebSocketConfigStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    MAX_MESSAGE_SIZE_TAG => {
                        self.stage =
                            WebSocketConfigStage::Slot(WebSocketConfigField::MaxMessageSize);
                        None
                    }
                    COMPRESSION_TAG => {
                        self.stage = WebSocketConfigStage::Slot(WebSocketConfigField::Compression);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(WebSocketConfig {
                    config: RatchetConfig {
                        max_message_size: self.max_message_size.unwrap_or(64 << 20),
                    },
                    compression: self.compression.clone(),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            WebSocketConfigStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = WebSocketConfigStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            WebSocketConfigStage::Field(WebSocketConfigField::MaxMessageSize) => {
                match UsizeRecognizer.feed_event(input) {
                    Some(Ok(n)) => {
                        self.max_message_size = Some(n);
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    }
                    Some(Err(e)) => Some(Err(e)),
                    _ => Some(Err(ReadError::InconsistentState)),
                }
            }
            WebSocketConfigStage::Field(WebSocketConfigField::Compression) => {
                match self.compression_recognizer.feed_event(input)? {
                    Ok(compression) => {
                        self.compression = compression.0;
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let WebSocketConfigRecognizer {
            stage,
            max_message_size,
            compression,
            compression_recognizer,
        } = self;

        *stage = WebSocketConfigStage::Init;
        *max_message_size = None;
        *compression = CompressionSwitcherProvider::Off;
        compression_recognizer.reset();
    }
}

pub struct WsCompression(CompressionSwitcherProvider);

impl From<CompressionSwitcherProvider> for WsCompression {
    fn from(config: CompressionSwitcherProvider) -> Self {
        WsCompression(config)
    }
}

impl AsRef<CompressionSwitcherProvider> for WsCompression {
    fn as_ref(&self) -> &CompressionSwitcherProvider {
        &self.0
    }
}

impl RecognizerReadable for WsCompression {
    type Rec = WsCompressionRecognizer;
    type AttrRec = SimpleAttrBody<WsCompressionRecognizer>;
    type BodyRec = SimpleRecBody<WsCompressionRecognizer>;

    fn make_recognizer() -> Self::Rec {
        WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None,
        })
    }
}

pub struct WsCompressionRecognizer {
    stage: WsCompressionRecognizerStage,
    fields: WsCompressionFields,
}

pub enum WsCompressionFields {
    None,
    Deflate(Option<u32>),
}

enum WsCompressionRecognizerStage {
    Init,
    Tag,
    AfterTag,
    InBody,
}

impl Recognizer for WsCompressionRecognizer {
    type Target = WsCompression;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            WsCompressionRecognizerStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    match name.borrow() {
                        WS_COMPRESSION_NONE_TAG => {
                            self.stage = WsCompressionRecognizerStage::Tag;
                            self.fields = WsCompressionFields::None;
                            None
                        }
                        WS_COMPRESSION_DEFLATE_TAG => {
                            self.stage = WsCompressionRecognizerStage::Tag;
                            self.fields = WsCompressionFields::Deflate(None);
                            None
                        }
                        _ => Some(Err(ReadError::UnexpectedAttribute(name.into()))),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::Attribute(Some(Text::new(WS_COMPRESSION_NONE_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(WS_COMPRESSION_DEFLATE_TAG))),
                    ]))))
                }
            }
            WsCompressionRecognizerStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = WsCompressionRecognizerStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            WsCompressionRecognizerStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = WsCompressionRecognizerStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    match self.fields {
                        WsCompressionFields::None => {
                            Some(Ok(WsCompression(CompressionSwitcherProvider::Off)))
                        }
                        WsCompressionFields::Deflate { .. } => {
                            Some(Ok(WsCompression(CompressionSwitcherProvider::On(
                                Arc::new(DeflateExtProvider::default()),
                            ))))
                        }
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            WsCompressionRecognizerStage::InBody => match &mut self.fields {
                WsCompressionFields::None => match &input {
                    ReadEvent::EndRecord => {
                        Some(Ok(WsCompression(CompressionSwitcherProvider::Off)))
                    }
                    _ => Some(Err(ReadError::InconsistentState)),
                },
                WsCompressionFields::Deflate(value) => {
                    if matches!(&input, ReadEvent::EndRecord) {
                        match value {
                            None => Some(Ok(WsCompression(CompressionSwitcherProvider::On(
                                Arc::new(DeflateExtProvider::default()),
                            )))),
                            Some(value) => {
                                Some(Ok(WsCompression(CompressionSwitcherProvider::On(
                                    Arc::new(DeflateExtProvider::with_config(
                                        DeflateConfig::for_compression_level(Compression::new(
                                            *value,
                                        )),
                                    )),
                                ))))
                            }
                        }
                    } else {
                        match U32Recognizer.feed_event(input) {
                            Some(Ok(n)) => {
                                *value = Some(n);
                                self.stage = WsCompressionRecognizerStage::InBody;
                                None
                            }
                            Some(Err(e)) => Some(Err(e)),
                            _ => Some(Err(ReadError::InconsistentState)),
                        }
                    }
                }
            },
        }
    }

    fn reset(&mut self) {
        let WsCompressionRecognizer { stage, fields } = self;
        *stage = WsCompressionRecognizerStage::Init;
        *fields = WsCompressionFields::None;
    }
}

impl StructuralWritable for WsCompression {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        match &self.0 {
            CompressionSwitcherProvider::Off => {
                let header_writer = writer.record(1)?;

                let body_writer = header_writer
                    .write_extant_attr(NONE_TAG)?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;
                body_writer.done()
            }
            CompressionSwitcherProvider::On(provider) => {
                let header_writer = writer.record(1)?;

                let mut body_writer = header_writer
                    .write_extant_attr(DEFLATE_TAG)?
                    .complete_header(RecordBodyKind::ArrayLike, 1)?;

                body_writer =
                    body_writer.write_value(&provider.config().compression_level().level())?;
                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

impl StructuralWritable for WebSocketConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let num_items = 3;

        let mut body_writer = header_writer
            .write_extant_attr(WEBSOCKET_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, num_items)?;

        body_writer =
            body_writer.write_slot(&MAX_MESSAGE_SIZE_TAG, &self.config.max_message_size)?;

        let comp = WsCompression(self.compression.clone());
        body_writer = body_writer.write_slot(&COMPRESSION_TAG, &comp)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        self.write_with(writer)
    }
}

impl StructuralWritable for DownlinkConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONFIG_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot(&BACK_PRESSURE_TAG, &self.back_pressure)?;
        body_writer = body_writer.write_slot(&IDLE_TIMEOUT_TAG, &self.idle_timeout)?;
        body_writer = body_writer.write_slot(&BUFFER_SIZE_TAG, &self.buffer_size)?;
        body_writer = body_writer.write_slot(&ON_INVALID_TAG, &self.on_invalid)?;
        body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        let mut body_writer = header_writer
            .write_extant_attr(DOWNLINK_CONFIG_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;

        body_writer = body_writer.write_slot_into(BACK_PRESSURE_TAG, self.back_pressure)?;
        body_writer = body_writer.write_slot_into(IDLE_TIMEOUT_TAG, self.idle_timeout)?;
        body_writer = body_writer.write_slot_into(BUFFER_SIZE_TAG, self.buffer_size)?;
        body_writer = body_writer.write_slot_into(ON_INVALID_TAG, self.on_invalid)?;
        body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, self.yield_after)?;

        body_writer.done()
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
                    .write_extant_attr(PROPAGATE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
                per_key_buffer_size,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(RELEASE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer = body_writer.write_slot(&INPUT_BUFFER_SIZE_TAG, input_buffer_size)?;
                body_writer =
                    body_writer.write_slot(&BRIDGE_BUFFER_SIZE_TAG, bridge_buffer_size)?;
                body_writer = body_writer.write_slot(&MAX_ACTIVE_KEYS_TAG, max_active_keys)?;
                body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, yield_after)?;
                body_writer =
                    body_writer.write_slot(&PER_KEY_BUFFER_SIZE_TAG, per_key_buffer_size)?;
                body_writer.done()
            }
        }
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        match self {
            BackpressureMode::Propagate => {
                let header_writer = writer.record(1)?;
                header_writer
                    .write_extant_attr(PROPAGATE_TAG)?
                    .complete_header(RecordBodyKind::Mixed, 0)?
                    .done()
            }
            BackpressureMode::Release {
                input_buffer_size,
                bridge_buffer_size,
                max_active_keys,
                yield_after,
                per_key_buffer_size,
            } => {
                let header_writer = writer.record(1)?;
                let mut body_writer = header_writer
                    .write_extant_attr(RELEASE_TAG)?
                    .complete_header(RecordBodyKind::MapLike, 4)?;

                body_writer =
                    body_writer.write_slot_into(INPUT_BUFFER_SIZE_TAG, input_buffer_size)?;
                body_writer =
                    body_writer.write_slot_into(BRIDGE_BUFFER_SIZE_TAG, bridge_buffer_size)?;
                body_writer = body_writer.write_slot_into(MAX_ACTIVE_KEYS_TAG, max_active_keys)?;
                body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, yield_after)?;
                body_writer =
                    body_writer.write_slot_into(PER_KEY_BUFFER_SIZE_TAG, per_key_buffer_size)?;
                body_writer.done()
            }
        }
    }
}

impl StructuralWritable for OnInvalidMessage {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        match self {
            OnInvalidMessage::Ignore => header_writer.write_extant_attr(IGNORE_TAG)?,
            OnInvalidMessage::Terminate => header_writer.write_extant_attr(TERMINATE_TAG)?,
        }
        .complete_header(RecordBodyKind::Mixed, 0)?
        .done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;

        match self {
            OnInvalidMessage::Ignore => header_writer.write_extant_attr(IGNORE_TAG)?,
            OnInvalidMessage::Terminate => header_writer.write_extant_attr(TERMINATE_TAG)?,
        }
        .complete_header(RecordBodyKind::Mixed, 0)?
        .done()
    }
}
