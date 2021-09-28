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
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use swim_common::form::structural::read::ReadError;
use swim_common::form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_common::form::Form;
use swim_common::model::text::Text;
use swim_common::model::ValueKind;
use swim_common::routing::remote::config::RemoteConnectionsConfig;
use swim_common::warp::path::{AbsolutePath, Addressable};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;
use utilities::future::retryable::strategy::{Quantity, RetryStrategy};

//Todo dm this needs to be changed after config from file is done.
// #[cfg(test)]
// mod tests;

const BAD_BUFFER_SIZE: &str = "Buffer sizes must be positive.";
const BAD_YIELD_AFTER: &str = "Yield after count must be positive..";
const BAD_TIMEOUT: &str = "Timeout must be positive.";

const DEFAULT_IDLE_TIMEOUT: u64 = 60000;
const DEFAULT_DOWNLINK_BUFFER_SIZE: usize = 5;
const DEFAULT_YIELD_AFTER: usize = 256;
const DEFAULT_BUFFER_SIZE: usize = 100;
const DEFAULT_DL_REQUEST_BUFFER_SIZE: usize = 8;
const DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE: usize = 32;
const DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE: usize = 16;
const DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS: usize = 16;
const DEFAULT_BACK_PRESSURE_YIELD_AFTER: usize = 256;

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

//Todo dm
impl RecognizerReadable for SwimClientConfig {
    type Rec = SwimClientConfigRecognizer;
    type AttrRec = SimpleAttrBody<SwimClientConfigRecognizer>;
    type BodyRec = SimpleRecBody<SwimClientConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        SwimClientConfigRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(SwimClientConfigRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(SwimClientConfigRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

//Todo dm
pub struct SwimClientConfigRecognizer;

impl Recognizer for SwimClientConfigRecognizer {
    type Target = SwimClientConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        unimplemented!()
    }

    fn reset(&mut self) {}
}

//Todo dm
#[test]
fn test_foo() {
    // let mut object = ClientDownlinksConfig::default();
    // object.for_host(
    //     url::Url::parse(&"warp://127.0.0.1:9001".to_string()).unwrap(),
    //     Default::default(),
    // );
    //
    // object.for_lane(
    //     &AbsolutePath::new(
    //         url::Url::parse(&"warp://127.0.0.2:9001".to_string()).unwrap(),
    //         "foo",
    //         "bar",
    //     ),
    //     Default::default(),
    // );

    let object = DownlinkConfig::default();

    eprintln!("object.as_value() = {}", object.as_value());
    let new_object = BackpressureMode::try_from_value(&object.as_value()).unwrap();
    eprintln!("new_object.as_value() = {}", new_object.as_value());

    // let config = SwimClientConfig::new(
    //     Default::default(),
    //     Default::default(),
    //     Default::default(),
    //     downlinks,
    // );
    // println!("{}", config.as_value());
    //
    // let value = config.as_value();
    //
    // let config_restored = ClientDownlinksConfig::try_from_value(&value).unwrap();

    // @downlinks{
    //     default:{
    //         back_pressure:@propagate,
    //         idle_timeout:@duration{
    //             secs:60000,
    //             nanos:0
    //         },
    //         buffer_size:5,
    //         on_invalid:terminate,
    //         yield_after:256
    //     },
    //     host:{
    //         "warp://127.0.0.1:9001":{
    //             back_pressure:@propagate,
    //             idle_timeout:@duration{
    //                 secs:60000,
    //                 nanos:0
    //             },
    //             buffer_size:5,
    //             on_invalid:terminate,
    //             yield_after:256
    //         }
    //     },
    //     lane:{
    //         @path{
    //             host:"warp://127.0.0.2:9001",
    //             node:foo,
    //             lane:bar
    //         }:{
    //             back_pressure:@propagate,
    //             idle_timeout:@duration{
    //                 secs:60000,
    //                 nanos:0
    //             },
    //             buffer_size:5,
    //             on_invalid:terminate,
    //             yield_after:256
    //         }
    //     }
    // }
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
#[derive(Form, Clone, Copy, Debug, Eq, PartialEq)]
#[form(tag = "downlink_connections")]
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

#[derive(Clone, Debug)]
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

impl Default for ClientDownlinksConfig {
    fn default() -> Self {
        ClientDownlinksConfig::new(Default::default())
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

//Todo dm
impl RecognizerReadable for ClientDownlinksConfig {
    type Rec = ClientDownlinksConfigRecognizer;
    type AttrRec = SimpleAttrBody<ClientDownlinksConfigRecognizer>;
    type BodyRec = SimpleRecBody<ClientDownlinksConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        ClientDownlinksConfigRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(ClientDownlinksConfigRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(ClientDownlinksConfigRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

//Todo dm
pub struct ClientDownlinksConfigRecognizer;

impl Recognizer for ClientDownlinksConfigRecognizer {
    type Target = ClientDownlinksConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        unimplemented!()
    }

    fn reset(&mut self) {}
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
            BackpressureMode::default(),
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT),
            DEFAULT_DOWNLINK_BUFFER_SIZE,
            OnInvalidMessage::default(),
            DEFAULT_YIELD_AFTER,
        )
        .unwrap()
    }
}

impl StructuralWritable for DownlinkConfig {
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let mut body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::ArrayLike, 5)?;

        body_writer = body_writer.write_slot(&"back_pressure", &self.back_pressure)?;
        body_writer = body_writer.write_slot(&"idle_timeout", &self.idle_timeout)?;
        body_writer = body_writer.write_slot(&"buffer_size", &self.buffer_size)?;
        body_writer = body_writer.write_slot(&"on_invalid", &self.on_invalid)?;
        body_writer = body_writer.write_slot(&"yield_after", &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let mut body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::ArrayLike, 5)?;

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
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(DownlinkConfigRecognizer {
            stage: DownlinkConfigStage::Init,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(DownlinkConfigRecognizer {
            stage: DownlinkConfigStage::Init,
        })
    }
}

//Todo dm
pub struct DownlinkConfigRecognizer {
    stage: DownlinkConfigStage,
}

enum DownlinkConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(BackpressureModeField),
    Field(BackpressureModeField),
}

impl Recognizer for DownlinkConfigRecognizer {
    type Target = DownlinkConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        unimplemented!()
    }

    fn reset(&mut self) {}
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
                        input_buffer_size: input_buffer_size.unwrap_or(
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE).unwrap(),
                        ),
                        bridge_buffer_size: bridge_buffer_size.unwrap_or(
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE).unwrap(),
                        ),
                        max_active_keys: max_active_keys.unwrap_or(
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS).unwrap(),
                        ),
                        yield_after: yield_after.unwrap_or(
                            NonZeroUsize::new(DEFAULT_BACK_PRESSURE_YIELD_AFTER).unwrap(),
                        ),
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
