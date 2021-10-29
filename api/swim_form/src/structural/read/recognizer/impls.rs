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

use crate::structural::read::error::ExpectedEvent;
use crate::structural::read::event::{NumericValue, ReadEvent};
use crate::structural::read::recognizer::primitive::NonZeroUsizeRecognizer;
use crate::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::structural::read::ReadError;
use crate::structural::tags::{
    ABSOLUTE_PATH_TAG, DELAY_TAG, DURATION_TAG,
    HOST_TAG, INFINITE_TAG, LANE_TAG, MAX_BACKOFF_TAG, MAX_INTERVAL_TAG,
    NANOS_TAG, NODE_TAG, RETRIES_TAG,
    RETRY_EXPONENTIAL_TAG, RETRY_IMMEDIATE_TAG, RETRY_INTERVAL_TAG, RETRY_NONE_TAG, SECS_TAG,
};
use swim_model::{Text, ValueKind};
use swim_model::path::AbsolutePath;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_utilities::future::retryable::strategy::{
    DEFAULT_EXPONENTIAL_MAX_BACKOFF, DEFAULT_EXPONENTIAL_MAX_INTERVAL, DEFAULT_IMMEDIATE_RETRIES,
    DEFAULT_INTERVAL_DELAY, DEFAULT_INTERVAL_RETRIES,
};
use swim_utilities::future::retryable::{Quantity, RetryStrategy};
use url::Url;

enum RetryStrategyStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(RetryStrategyField),
    Field(RetryStrategyField),
}

impl Default for RetryStrategyStage {
    fn default() -> Self {
        RetryStrategyStage::Init
    }
}

#[derive(Clone, Copy)]
enum RetryStrategyField {
    ImmediateRetries,
    IntervalDelay,
    IntervalRetries,
    ExponentialMaxInterval,
    ExponentialMaxBackoff,
}

impl RecognizerReadable for RetryStrategy {
    type Rec = RetryStrategyRecognizer;
    type AttrRec = SimpleAttrBody<RetryStrategyRecognizer>;
    type BodyRec = SimpleRecBody<RetryStrategyRecognizer>;

    fn make_recognizer() -> Self::Rec {
        RetryStrategyRecognizer {
            stage: RetryStrategyStage::Init,
            fields: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(RetryStrategyRecognizer {
            stage: RetryStrategyStage::Init,
            fields: None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(RetryStrategyRecognizer {
            stage: RetryStrategyStage::Init,
            fields: None,
        })
    }
}

#[derive(Default)]
pub struct RetryStrategyRecognizer {
    stage: RetryStrategyStage,
    fields: Option<RetryStrategyFields>,
}

pub enum RetryStrategyFields {
    Immediate {
        retries: Option<NonZeroUsize>,
        retries_recognizer: Option<NonZeroUsizeRecognizer>,
    },
    Interval {
        retries: Option<Quantity<NonZeroUsize>>,
        delay: Option<Duration>,
        retries_recognizer: Option<QuantityRecognizer<NonZeroUsize>>,
        delay_recognizer: Option<DurationRecognizer>,
    },
    Exponential {
        max_interval: Option<Duration>,
        max_backoff: Option<Quantity<Duration>>,
        max_interval_recognizer: Option<DurationRecognizer>,
        max_backoff_recognizer: Option<QuantityRecognizer<Duration>>,
    },
}

impl Recognizer for RetryStrategyRecognizer {
    type Target = RetryStrategy;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            RetryStrategyStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    match name.borrow() {
                        RETRY_IMMEDIATE_TAG => {
                            self.stage = RetryStrategyStage::Tag;
                            self.fields = Some(RetryStrategyFields::Immediate {
                                retries: None,
                                retries_recognizer: None,
                            });
                            None
                        }
                        RETRY_INTERVAL_TAG => {
                            self.stage = RetryStrategyStage::Tag;
                            self.fields = Some(RetryStrategyFields::Interval {
                                retries: None,
                                delay: None,
                                retries_recognizer: None,
                                delay_recognizer: None,
                            });
                            None
                        }
                        RETRY_EXPONENTIAL_TAG => {
                            self.stage = RetryStrategyStage::Tag;
                            self.fields = Some(RetryStrategyFields::Exponential {
                                max_interval: None,
                                max_backoff: None,
                                max_interval_recognizer: None,
                                max_backoff_recognizer: None,
                            });
                            None
                        }
                        RETRY_NONE_TAG => {
                            self.stage = RetryStrategyStage::Tag;
                            None
                        }
                        _ => Some(Err(ReadError::UnexpectedAttribute(name.into()))),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::Attribute(Some(Text::new(RETRY_IMMEDIATE_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(RETRY_INTERVAL_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(RETRY_EXPONENTIAL_TAG))),
                        ExpectedEvent::Attribute(Some(Text::new(RETRY_NONE_TAG))),
                    ]))))
                }
            }
            RetryStrategyStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = RetryStrategyStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            RetryStrategyStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = RetryStrategyStage::InBody;
                    None
                } else if matches!(&input, ReadEvent::EndRecord) {
                    match self.fields {
                        Some(RetryStrategyFields::Immediate { .. }) => {
                            Some(Ok(RetryStrategy::default_immediate()))
                        }
                        Some(RetryStrategyFields::Interval { .. }) => {
                            Some(Ok(RetryStrategy::default_interval()))
                        }
                        Some(RetryStrategyFields::Exponential { .. }) => {
                            Some(Ok(RetryStrategy::default_exponential()))
                        }
                        None => Some(Ok(RetryStrategy::none())),
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::RecordBody,
                        ExpectedEvent::EndOfRecord,
                    ]))))
                }
            }
            RetryStrategyStage::InBody => match self.fields {
                Some(RetryStrategyFields::Immediate {
                         retries,
                         ref mut retries_recognizer,
                     }) => match input {
                    ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                        RETRIES_TAG => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::ImmediateRetries);
                            *retries_recognizer = Some(NonZeroUsize::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => {
                        Some(Ok(RetryStrategy::immediate(retries.unwrap_or_else(|| {
                            NonZeroUsize::new(DEFAULT_IMMEDIATE_RETRIES).unwrap()
                        }))))
                    }
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::Text),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
                Some(RetryStrategyFields::Interval {
                         delay,
                         retries,
                         ref mut delay_recognizer,
                         ref mut retries_recognizer,
                     }) => match input {
                    ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                        DELAY_TAG => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::IntervalDelay);
                            *delay_recognizer = Some(Duration::make_recognizer());
                            None
                        }
                        RETRIES_TAG => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::IntervalRetries);
                            *retries_recognizer = Some(Quantity::<NonZeroUsize>::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::interval(
                        delay.unwrap_or_else(|| Duration::from_secs(DEFAULT_INTERVAL_DELAY)),
                        retries.unwrap_or_else(|| {
                            Quantity::Finite(NonZeroUsize::new(DEFAULT_INTERVAL_RETRIES).unwrap())
                        }),
                    ))),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::Text),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
                Some(RetryStrategyFields::Exponential {
                         max_interval,
                         max_backoff,
                         ref mut max_interval_recognizer,
                         ref mut max_backoff_recognizer,
                     }) => match input {
                    ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                        MAX_INTERVAL_TAG => {
                            self.stage = RetryStrategyStage::Slot(
                                RetryStrategyField::ExponentialMaxInterval,
                            );
                            *max_interval_recognizer = Some(Duration::make_recognizer());
                            None
                        }
                        MAX_BACKOFF_TAG => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::ExponentialMaxBackoff);
                            *max_backoff_recognizer = Some(Quantity::<Duration>::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::exponential(
                        max_interval.unwrap_or(DEFAULT_EXPONENTIAL_MAX_INTERVAL),
                        max_backoff.unwrap_or(Quantity::Finite(DEFAULT_EXPONENTIAL_MAX_BACKOFF)),
                    ))),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::Text),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
                None => match input {
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::none())),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfRecord))),
                },
            },
            RetryStrategyStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = RetryStrategyStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            RetryStrategyStage::Field(field) => match &mut self.fields {
                Some(RetryStrategyFields::Immediate {
                         retries,
                         retries_recognizer,
                     }) => match field {
                    RetryStrategyField::ImmediateRetries => {
                        match retries_recognizer.as_mut()?.feed_event(input)? {
                            Ok(value) => {
                                *retries = Some(value);
                                self.stage = RetryStrategyStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    _ => None,
                },
                Some(RetryStrategyFields::Interval {
                         retries,
                         retries_recognizer,
                         delay,
                         delay_recognizer,
                     }) => match field {
                    RetryStrategyField::IntervalRetries => {
                        match retries_recognizer.as_mut()?.feed_event(input)? {
                            Ok(value) => {
                                *retries = Some(value);
                                self.stage = RetryStrategyStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    RetryStrategyField::IntervalDelay => {
                        match delay_recognizer.as_mut()?.feed_event(input)? {
                            Ok(value) => {
                                *delay = Some(value);
                                self.stage = RetryStrategyStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    _ => None,
                },
                Some(RetryStrategyFields::Exponential {
                         max_interval,
                         max_interval_recognizer,
                         max_backoff,
                         max_backoff_recognizer,
                     }) => match field {
                    RetryStrategyField::ExponentialMaxInterval => {
                        match max_interval_recognizer.as_mut()?.feed_event(input)? {
                            Ok(value) => {
                                *max_interval = Some(value);
                                self.stage = RetryStrategyStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    RetryStrategyField::ExponentialMaxBackoff => {
                        match max_backoff_recognizer.as_mut()?.feed_event(input)? {
                            Ok(value) => {
                                *max_backoff = Some(value);
                                self.stage = RetryStrategyStage::InBody;
                                None
                            }
                            Err(err) => Some(Err(err)),
                        }
                    }
                    _ => None,
                },
                None => None,
            },
        }
    }

    fn reset(&mut self) {
        self.stage = RetryStrategyStage::Init;
        self.fields = None;
    }
}

impl<T: RecognizerReadable> RecognizerReadable for Quantity<T> {
    type Rec = QuantityRecognizer<T>;
    type AttrRec = SimpleAttrBody<QuantityRecognizer<T>>;
    type BodyRec = SimpleRecBody<QuantityRecognizer<T>>;

    fn make_recognizer() -> Self::Rec {
        QuantityRecognizer {
            recognizer: T::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(QuantityRecognizer {
            recognizer: T::make_recognizer(),
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(QuantityRecognizer {
            recognizer: T::make_recognizer(),
        })
    }
}

pub struct QuantityRecognizer<T: RecognizerReadable> {
    recognizer: T::Rec,
}

impl<T: RecognizerReadable> Recognizer for QuantityRecognizer<T> {
    type Target = Quantity<T>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(value) if value == INFINITE_TAG => Some(Ok(Quantity::Infinite)),
            _ => match self.recognizer.feed_event(input)? {
                Ok(val) => Some(Ok(Quantity::Finite(val))),
                Err(err) => Some(Err(err)),
            },
        }
    }

    fn reset(&mut self) {
        self.recognizer.reset()
    }
}

#[derive(Default)]
pub struct DurationRecognizer {
    stage: DurationStage,
    secs: Option<u64>,
    nanos: Option<u32>,
}

enum DurationStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(DurationField),
    Field(DurationField),
}

impl Default for DurationStage {
    fn default() -> Self {
        DurationStage::Init
    }
}

#[derive(Clone, Copy)]
enum DurationField {
    Secs,
    Nanos,
}

impl RecognizerReadable for Duration {
    type Rec = DurationRecognizer;
    type AttrRec = SimpleAttrBody<DurationRecognizer>;
    type BodyRec = SimpleRecBody<DurationRecognizer>;

    fn make_recognizer() -> Self::Rec {
        DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(DurationRecognizer {
            stage: DurationStage::Init,
            secs: None,
            nanos: None,
        })
    }
}

impl Recognizer for DurationRecognizer {
    type Target = Duration;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            DurationStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == DURATION_TAG {
                        self.stage = DurationStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(DURATION_TAG),
                    )))))
                }
            }
            DurationStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = DurationStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            DurationStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = DurationStage::InBody;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            DurationStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    SECS_TAG => {
                        self.stage = DurationStage::Slot(DurationField::Secs);
                        None
                    }
                    NANOS_TAG => {
                        self.stage = DurationStage::Slot(DurationField::Nanos);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(Duration::new(
                    self.secs.unwrap_or_default(),
                    self.nanos.unwrap_or_default(),
                ))),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            DurationStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = DurationStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            DurationStage::Field(DurationField::Secs) => match input {
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    self.secs = Some(n);
                    self.stage = DurationStage::InBody;
                    None
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            DurationStage::Field(DurationField::Nanos) => match input {
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = u32::try_from(n) {
                        self.nanos = Some(m);
                        self.stage = DurationStage::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
        }
    }

    fn reset(&mut self) {
        let DurationRecognizer { stage, secs, nanos } = self;
        *stage = DurationStage::Init;
        *secs = None;
        *nanos = None;
    }
}

pub struct AbsolutePathRecognizer {
    stage: AbsolutePathStage,
    host: Option<Url>,
    node: Option<Text>,
    lane: Option<Text>,
}

enum AbsolutePathStage {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(AbsolutePathField),
    Field(AbsolutePathField),
}

#[derive(Clone, Copy)]
enum AbsolutePathField {
    Host,
    Node,
    Lane,
}

impl RecognizerReadable for AbsolutePath {
    type Rec = AbsolutePathRecognizer;
    type AttrRec = SimpleAttrBody<AbsolutePathRecognizer>;
    type BodyRec = SimpleRecBody<AbsolutePathRecognizer>;

    fn make_recognizer() -> Self::Rec {
        AbsolutePathRecognizer {
            stage: AbsolutePathStage::Init,
            host: None,
            node: None,
            lane: None,
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(AbsolutePathRecognizer {
            stage: AbsolutePathStage::Init,
            host: None,
            node: None,
            lane: None,
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(AbsolutePathRecognizer {
            stage: AbsolutePathStage::Init,
            host: None,
            node: None,
            lane: None,
        })
    }
}

impl AbsolutePathRecognizer {
    fn try_done(&mut self) -> Result<AbsolutePath, ReadError> {
        let AbsolutePathRecognizer {
            host, node, lane, ..
        } = self;

        let mut missing = vec![];
        if host.is_none() {
            missing.push(Text::new(HOST_TAG));
        }
        if node.is_none() {
            missing.push(Text::new(NODE_TAG));
        }
        if lane.is_none() {
            missing.push(Text::new(LANE_TAG));
        }
        if let (Some(host), Some(node), Some(lane)) = (host.take(), node.take(), lane.take()) {
            Ok(AbsolutePath { host, node, lane })
        } else {
            Err(ReadError::MissingFields(missing))
        }
    }
}

impl Recognizer for AbsolutePathRecognizer {
    type Target = AbsolutePath;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.stage {
            AbsolutePathStage::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == ABSOLUTE_PATH_TAG {
                        self.stage = AbsolutePathStage::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(ABSOLUTE_PATH_TAG),
                    )))))
                }
            }
            AbsolutePathStage::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.stage = AbsolutePathStage::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            AbsolutePathStage::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = AbsolutePathStage::InBody;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            AbsolutePathStage::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    HOST_TAG => {
                        self.stage = AbsolutePathStage::Slot(AbsolutePathField::Host);
                        None
                    }
                    NODE_TAG => {
                        self.stage = AbsolutePathStage::Slot(AbsolutePathField::Node);
                        None
                    }
                    LANE_TAG => {
                        self.stage = AbsolutePathStage::Slot(AbsolutePathField::Lane);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(self.try_done()),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            AbsolutePathStage::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = AbsolutePathStage::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            AbsolutePathStage::Field(AbsolutePathField::Host) => {
                match Url::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.host = Some(value);
                        self.stage = AbsolutePathStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            AbsolutePathStage::Field(AbsolutePathField::Lane) => {
                match Text::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.lane = Some(value);
                        self.stage = AbsolutePathStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            AbsolutePathStage::Field(AbsolutePathField::Node) => {
                match Text::make_recognizer().feed_event(input)? {
                    Ok(value) => {
                        self.node = Some(value);
                        self.stage = AbsolutePathStage::InBody;
                        None
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let AbsolutePathRecognizer {
            stage,
            host,
            node,
            lane,
        } = self;
        *stage = AbsolutePathStage::Init;
        *host = None;
        *node = None;
        *lane = None;
    }
}
