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

use crate::configuration::DownlinkConnectionsConfig;
use crate::configuration::{
    BackpressureMode, DownlinkConfig, OnInvalidMessage, DEFAULT_BACK_PRESSURE_BRIDGE_BUFFER_SIZE,
    DEFAULT_BACK_PRESSURE_INPUT_BUFFER_SIZE, DEFAULT_BACK_PRESSURE_MAX_ACTIVE_KEYS,
    DEFAULT_BACK_PRESSURE_YIELD_AFTER, DEFAULT_BUFFER_SIZE, DEFAULT_DL_REQUEST_BUFFER_SIZE,
    DEFAULT_DOWNLINK_BUFFER_SIZE, DEFAULT_IDLE_TIMEOUT, DEFAULT_YIELD_AFTER, DOWNLINK_CONFIG_TAG,
    DOWNLINK_CONNECTIONS_TAG, IGNORE_TAG, PROPAGATE_TAG, RELEASE_TAG, TERMINATE_TAG,
};
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_form::structural::read::error::{ExpectedEvent, ReadError};
use swim_form::structural::read::event::ReadEvent;
use swim_form::structural::read::recognizer::impls::{DurationRecognizer, RetryStrategyRecognizer};
use swim_form::structural::read::recognizer::{Recognizer, RecognizerReadable};
use swim_model::{Text, ValueKind};
use swim_utilities::future::retryable::RetryStrategy;

#[derive(Default)]
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

impl Default for DownlinkConnectionsConfigStage {
    fn default() -> Self {
        DownlinkConnectionsConfigStage::Init
    }
}
#[derive(Clone, Copy)]
enum DownlinkConnectionsConfigField {
    DlBufferSize,
    BufferSize,
    YieldAfter,
    RetryStrategy,
}

use super::{
    BACK_PRESSURE_TAG, BRIDGE_BUFFER_SIZE_TAG, BUFFER_SIZE_TAG, DL_REQ_BUFFER_SIZE_TAG,
    IDLE_TIMEOUT_TAG, INPUT_BUFFER_SIZE_TAG, MAX_ACTIVE_KEYS_TAG, ON_INVALID_TAG,
    RETRY_STRATEGY_TAG, YIELD_AFTER_TAG,
};

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

#[derive(Default)]
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

impl Default for DownlinkConfigStage {
    fn default() -> Self {
        DownlinkConfigStage::Init
    }
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

#[derive(Default)]
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

impl Default for BackpressureModeStage {
    fn default() -> Self {
        BackpressureModeStage::Init
    }
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

#[derive(Default)]
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

impl Default for OnInvalidMessageStage {
    fn default() -> Self {
        OnInvalidMessageStage::Init
    }
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
