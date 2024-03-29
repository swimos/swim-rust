// Copyright 2015-2023 Swim Inc.
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
use crate::structural::read::event::ReadEvent;
use crate::structural::read::recognizer::primitive::{
    NonZeroUsizeRecognizer, U32Recognizer, U64Recognizer,
};
use crate::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::structural::read::ReadError;
use crate::structural::tags::{
    DELAY_TAG, DURATION_TAG, INFINITE_TAG, MAX_BACKOFF_TAG, MAX_INTERVAL_TAG, NANOS_TAG,
    RETRIES_TAG, RETRY_EXPONENTIAL_TAG, RETRY_IMMEDIATE_TAG, RETRY_INTERVAL_TAG, RETRY_NONE_TAG,
    SECS_TAG,
};
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_model::{Text, ValueKind};
use swim_utilities::future::retryable::strategy::{
    DEFAULT_EXPONENTIAL_MAX_BACKOFF, DEFAULT_EXPONENTIAL_MAX_INTERVAL, DEFAULT_IMMEDIATE_RETRIES,
    DEFAULT_INTERVAL_DELAY, DEFAULT_INTERVAL_RETRIES,
};
use swim_utilities::future::retryable::{Quantity, RetryStrategy};

#[derive(Default)]
enum RetryStrategyStage {
    #[default]
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(RetryStrategyField),
    Field(RetryStrategyField),
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
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::immediate(
                        retries.unwrap_or(DEFAULT_IMMEDIATE_RETRIES),
                    ))),
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
                        retries.unwrap_or(Quantity::Finite(DEFAULT_INTERVAL_RETRIES)),
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

#[derive(Default)]
enum DurationStage {
    #[default]
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(DurationField),
    Field(DurationField),
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
            DurationStage::Field(DurationField::Secs) => match U64Recognizer.feed_event(input) {
                Some(Ok(n)) => {
                    self.secs = Some(n);
                    self.stage = DurationStage::InBody;
                    None
                }
                Some(Err(e)) => Some(Err(e)),
                _ => Some(Err(ReadError::InconsistentState)),
            },
            DurationStage::Field(DurationField::Nanos) => match U32Recognizer.feed_event(input) {
                Some(Ok(n)) => {
                    self.nanos = Some(n);
                    self.stage = DurationStage::InBody;
                    None
                }
                Some(Err(e)) => Some(Err(e)),
                _ => Some(Err(ReadError::InconsistentState)),
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
