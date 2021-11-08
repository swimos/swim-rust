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

use crate::form::structural::read::error::ExpectedEvent;
use crate::form::structural::read::event::ReadEvent;
use crate::form::structural::read::recognizer::impls::{
    DurationRecognizer, RetryStrategyRecognizer,
};
use crate::form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::form::structural::read::ReadError;
use crate::form::structural::write::{
    BodyWriter, HeaderWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use crate::model::text::Text;
use crate::model::ValueKind;
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::future::retryable::RetryStrategy;

mod swim_common {
    pub use crate::*;
}

const REMOTE_CONNECTIONS_TAG: &str = "remote_connections";
const ROUTER_BUFFER_SIZE_TAG: &str = "router_buffer_size";
const CHANNEL_BUFFER_SIZE_TAG: &str = "channel_buffer_size";
const ACTIVITY_TIMEOUT_TAG: &str = "activity_timeout";
const WRITE_TIMEOUT_TAG: &str = "write_timeout";
const CONNECTION_RETRIES_TAG: &str = "connection_retries";
const YIELD_AFTER_TAG: &str = "yield_after";

const DEFAULT_ROUTER_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);
const DEFAULT_CHANNEL_BUFFER_SIZE: NonZeroUsize = non_zero_usize!(32);
const DEFAULT_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_YIELD_AFTER: NonZeroUsize = non_zero_usize!(256);

/// Configuration parameters for remote connection management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteConnectionsConfig {
    /// Buffer size for sending routing requests for a router instance.
    pub router_buffer_size: NonZeroUsize,
    /// Buffer size for the channel to send data to the task managing a single connection.
    pub channel_buffer_size: NonZeroUsize,
    /// Time after which to close an inactive connection.
    pub activity_timeout: Duration,
    /// If a pending write does not complete after this period, fail
    pub write_timeout: Duration,
    /// Strategy for retrying a connection.
    pub connection_retries: RetryStrategy,
    /// The number of events to process before yielding execution back to the runtime.
    pub yield_after: NonZeroUsize,
}

impl RemoteConnectionsConfig {
    pub fn new(
        router_buffer_size: NonZeroUsize,
        channel_buffer_size: NonZeroUsize,
        activity_timeout: Duration,
        write_timeout: Duration,
        connection_retries: RetryStrategy,
        yield_after: NonZeroUsize,
    ) -> Self {
        RemoteConnectionsConfig {
            router_buffer_size,
            channel_buffer_size,
            activity_timeout,
            write_timeout,
            connection_retries,
            yield_after,
        }
    }
}

impl Default for RemoteConnectionsConfig {
    fn default() -> Self {
        RemoteConnectionsConfig {
            router_buffer_size: DEFAULT_ROUTER_BUFFER_SIZE,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            activity_timeout: DEFAULT_ACTIVITY_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            connection_retries: Default::default(),
            yield_after: DEFAULT_YIELD_AFTER,
        }
    }
}

impl StructuralWritable for RemoteConnectionsConfig {
    fn num_attributes(&self) -> usize {
        1
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(REMOTE_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 6)?;

        body_writer = body_writer.write_slot(&ROUTER_BUFFER_SIZE_TAG, &self.router_buffer_size)?;
        body_writer =
            body_writer.write_slot(&CHANNEL_BUFFER_SIZE_TAG, &self.channel_buffer_size)?;
        body_writer = body_writer.write_slot(&ACTIVITY_TIMEOUT_TAG, &self.activity_timeout)?;
        body_writer = body_writer.write_slot(&WRITE_TIMEOUT_TAG, &self.write_timeout)?;
        body_writer = body_writer.write_slot(&CONNECTION_RETRIES_TAG, &self.connection_retries)?;
        body_writer = body_writer.write_slot(&YIELD_AFTER_TAG, &self.yield_after)?;

        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(REMOTE_CONNECTIONS_TAG)?
            .complete_header(RecordBodyKind::MapLike, 6)?;

        body_writer =
            body_writer.write_slot_into(ROUTER_BUFFER_SIZE_TAG, self.router_buffer_size)?;
        body_writer =
            body_writer.write_slot_into(CHANNEL_BUFFER_SIZE_TAG, self.channel_buffer_size)?;
        body_writer = body_writer.write_slot_into(ACTIVITY_TIMEOUT_TAG, self.activity_timeout)?;
        body_writer = body_writer.write_slot_into(WRITE_TIMEOUT_TAG, self.write_timeout)?;
        body_writer =
            body_writer.write_slot_into(CONNECTION_RETRIES_TAG, self.connection_retries)?;
        body_writer = body_writer.write_slot_into(YIELD_AFTER_TAG, self.yield_after)?;

        body_writer.done()
    }
}

impl RecognizerReadable for RemoteConnectionsConfig {
    type Rec = RemoteConnectionsConfigRecognizer;
    type AttrRec = SimpleAttrBody<RemoteConnectionsConfigRecognizer>;
    type BodyRec = SimpleRecBody<RemoteConnectionsConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        RemoteConnectionsConfigRecognizer {
            stage: RemoteConnectionsConfigStage::Init,
            router_buffer_size: None,
            channel_buffer_size: None,
            activity_timeout: None,
            activity_timeout_recognizer: Duration::make_recognizer(),
            write_timeout: None,
            write_timeout_recognizer: Duration::make_recognizer(),
            connection_retries: None,
            connection_retries_recognizer: RetryStrategy::make_recognizer(),
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

enum RemoteConnectionsConfigStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(RemoteConnectionsConfigField),
    Field(RemoteConnectionsConfigField),
}

#[derive(Clone, Copy)]
enum RemoteConnectionsConfigField {
    RouterBufferSize,
    ChannelBufferSize,
    ActivityTimeout,
    WriteTimeout,
    ConnectionRetries,
    YieldAfter,
}

pub struct RemoteConnectionsConfigRecognizer {
    stage: RemoteConnectionsConfigStage,
    router_buffer_size: Option<NonZeroUsize>,
    channel_buffer_size: Option<NonZeroUsize>,
    activity_timeout: Option<Duration>,
    activity_timeout_recognizer: DurationRecognizer,
    write_timeout: Option<Duration>,
    write_timeout_recognizer: DurationRecognizer,
    connection_retries: Option<RetryStrategy>,
    connection_retries_recognizer: RetryStrategyRecognizer,
    yield_after: Option<NonZeroUsize>,
}

impl Recognizer for RemoteConnectionsConfigRecognizer {
    type Target = RemoteConnectionsConfig;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            RemoteConnectionsConfigStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == REMOTE_CONNECTIONS_TAG {
                        self.stage = RemoteConnectionsConfigStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(REMOTE_CONNECTIONS_TAG),
                    )))))
                }
            }
            RemoteConnectionsConfigStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = RemoteConnectionsConfigStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            RemoteConnectionsConfigStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = RemoteConnectionsConfigStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(RemoteConnectionsConfig::default()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            RemoteConnectionsConfigStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    ROUTER_BUFFER_SIZE_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::RouterBufferSize,
                        );
                        None
                    }
                    CHANNEL_BUFFER_SIZE_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::ChannelBufferSize,
                        );
                        None
                    }
                    ACTIVITY_TIMEOUT_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::ActivityTimeout,
                        );
                        None
                    }
                    WRITE_TIMEOUT_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::WriteTimeout,
                        );
                        None
                    }
                    CONNECTION_RETRIES_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::ConnectionRetries,
                        );
                        None
                    }
                    YIELD_AFTER_TAG => {
                        self.stage = RemoteConnectionsConfigStage::Slot(
                            RemoteConnectionsConfigField::YieldAfter,
                        );
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(RemoteConnectionsConfig {
                    router_buffer_size: self
                        .router_buffer_size
                        .unwrap_or(DEFAULT_ROUTER_BUFFER_SIZE),
                    channel_buffer_size: self
                        .channel_buffer_size
                        .unwrap_or(DEFAULT_CHANNEL_BUFFER_SIZE),
                    activity_timeout: self.activity_timeout.unwrap_or(DEFAULT_ACTIVITY_TIMEOUT),
                    write_timeout: self.write_timeout.unwrap_or(DEFAULT_WRITE_TIMEOUT),
                    connection_retries: self.connection_retries.unwrap_or_default(),
                    yield_after: self.yield_after.unwrap_or(DEFAULT_YIELD_AFTER),
                })),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            RemoteConnectionsConfigStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = RemoteConnectionsConfigStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            RemoteConnectionsConfigStage::Field(RemoteConnectionsConfigField::RouterBufferSize) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.router_buffer_size = Some(value);
                        self.stage = RemoteConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            RemoteConnectionsConfigStage::Field(
                RemoteConnectionsConfigField::ChannelBufferSize,
            ) => match NonZeroUsize::make_recognizer().feed_event(input)? {
                Ok(value) => {
                    self.channel_buffer_size = Some(value);
                    self.stage = RemoteConnectionsConfigStage::InBody;
                    None
                }
                Err(err) => Some(Err(err)),
            },
            RemoteConnectionsConfigStage::Field(RemoteConnectionsConfigField::ActivityTimeout) => {
                match self.activity_timeout_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.activity_timeout = Some(value);
                        self.stage = RemoteConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            RemoteConnectionsConfigStage::Field(RemoteConnectionsConfigField::WriteTimeout) => {
                match self.write_timeout_recognizer.feed_event(input)? {
                    Ok(value) => {
                        self.write_timeout = Some(value);
                        self.stage = RemoteConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            RemoteConnectionsConfigStage::Field(
                RemoteConnectionsConfigField::ConnectionRetries,
            ) => match self.connection_retries_recognizer.feed_event(input)? {
                Ok(value) => {
                    self.connection_retries = Some(value);
                    self.stage = RemoteConnectionsConfigStage::InBody;
                    None
                }
                Err(err) => Some(Err(err)),
            },
            RemoteConnectionsConfigStage::Field(RemoteConnectionsConfigField::YieldAfter) => {
                match NonZeroUsize::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.yield_after = Some(value);
                        self.stage = RemoteConnectionsConfigStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let RemoteConnectionsConfigRecognizer {
            stage,
            router_buffer_size,
            channel_buffer_size,
            activity_timeout,
            activity_timeout_recognizer,
            write_timeout,
            write_timeout_recognizer,
            connection_retries,
            connection_retries_recognizer,
            yield_after,
        } = self;

        *stage = RemoteConnectionsConfigStage::Init;
        *router_buffer_size = None;
        *channel_buffer_size = None;
        *activity_timeout = None;
        *write_timeout = None;
        *connection_retries = None;
        *yield_after = None;

        activity_timeout_recognizer.reset();
        write_timeout_recognizer.reset();
        connection_retries_recognizer.reset();
    }
}
