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

use crate::structural::read::recognizer::primitive::{
    NonZeroUsizeRecognizer, U32Recognizer, U64Recognizer,
};
use crate::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::structural::read::ExpectedEvent;
use crate::structural::read::NumericValue;
use crate::structural::read::ReadError;
use crate::structural::read::ReadEvent;
use crate::structural::tags::{
    DELAY_TAG, DURATION_TAG, INFINITE_TAG, MAX_BACKOFF_TAG, MAX_INTERVAL_TAG, NANOS_TAG,
    RETRIES_TAG, RETRY_EXPONENTIAL_TAG, RETRY_IMMEDIATE_TAG, RETRY_INTERVAL_TAG, RETRY_NONE_TAG,
    SECS_TAG,
};
use chrono::LocalResult;
use chrono::TimeZone;
use chrono::Utc;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;
use swimos_model::Timestamp;
use swimos_model::{Text, ValueKind};
use swimos_utilities::future::{ExponentialStrategy, IntervalStrategy, Quantity, RetryStrategy};
use swimos_utilities::routing::RouteUri;

use super::BodyStage;
use super::FirstOf;
use super::MappedRecognizer;
use super::OrdinalFieldsRecognizer;

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

#[doc(hidden)]
#[derive(Default)]
pub struct RetryStrategyRecognizer {
    stage: RetryStrategyStage,
    fields: Option<RetryStrategyFields>,
}

enum RetryStrategyFields {
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
                        let strat = if let Some(retries) = retries {
                            RetryStrategy::immediate(retries)
                        } else {
                            RetryStrategy::default_immediate()
                        };
                        Some(Ok(strat))
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
                    ReadEvent::EndRecord => {
                        let mut strat = IntervalStrategy::default_interval();
                        strat.delay = delay;
                        match retries {
                            Some(Quantity::Finite(n)) => strat.retry = Quantity::Finite(n.get()),
                            Some(Quantity::Infinite) => strat.retry = Quantity::Infinite,
                            _ => {}
                        }
                        Some(Ok(RetryStrategy::Interval(strat)))
                    }
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
                    ReadEvent::EndRecord => {
                        let mut strat = ExponentialStrategy::default();
                        if let Some(interval) = max_interval {
                            strat.max_interval = interval
                        };
                        if let Some(backoff) = max_backoff {
                            strat.max_backoff = backoff
                        };
                        Some(Ok(RetryStrategy::Exponential(strat)))
                    }
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

#[doc(hidden)]
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

#[doc(hidden)]
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

pub struct RouteUriRecognizer;

impl Recognizer for RouteUriRecognizer {
    type Target = RouteUri;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                let result = RouteUri::from_str(txt.borrow());
                let uri = result.map_err(move |_| ReadError::Malformatted {
                    text: Text::from(txt),
                    message: Text::new("Not a valid relative URI."),
                });
                Some(uri)
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl RecognizerReadable for RouteUri {
    type Rec = RouteUriRecognizer;
    type AttrRec = SimpleAttrBody<RouteUriRecognizer>;
    type BodyRec = SimpleRecBody<RouteUriRecognizer>;

    fn make_recognizer() -> Self::Rec {
        RouteUriRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(RouteUriRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(RouteUriRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

#[derive(Debug)]
pub struct TimestampRecognizer;

fn check_parse_time_result<T, V>(me: LocalResult<T>, ts: &V) -> Result<T, ReadError>
where
    V: Display,
{
    match me {
        LocalResult::Single(val) => Ok(val),
        _ => Err(ReadError::Message(
            format!("Failed to parse timestamp: {}", ts).into(),
        )),
    }
}

impl Recognizer for TimestampRecognizer {
    type Target = Timestamp;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                let result = check_parse_time_result(
                    Utc.timestamp_opt(n / 1_000_000, (n % 1_000_000) as u32),
                    &n,
                )
                .map(Timestamp::from)
                .map_err(|_| ReadError::NumberOutOfRange);

                Some(result)
            }
            ReadEvent::Number(NumericValue::UInt(n)) => {
                let result = check_parse_time_result(
                    Utc.timestamp_opt((n / 1_000_000) as i64, (n % 1_000_000) as u32),
                    &n,
                )
                .map(Timestamp::from)
                .map_err(|_| ReadError::NumberOutOfRange);
                Some(result)
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl RecognizerReadable for Timestamp {
    type Rec = TimestampRecognizer;
    type AttrRec = SimpleAttrBody<TimestampRecognizer>;
    type BodyRec = SimpleRecBody<TimestampRecognizer>;

    fn make_recognizer() -> Self::Rec {
        TimestampRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(TimestampRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(TimestampRecognizer)
    }
}

/// Recognizes a vector of values of the same type.
#[derive(Debug)]
pub struct VecRecognizer<T, R> {
    is_attr_body: bool,
    stage: BodyStage,
    vector: Vec<T>,
    rec: R,
}

impl<T, R: Recognizer<Target = T>> Recognizer for VecRecognizer<T, R> {
    type Target = Vec<T>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            BodyStage::Init => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = BodyStage::Between;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            BodyStage::Between => match &input {
                ReadEvent::EndRecord if !self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.vector)))
                }
                ReadEvent::EndAttribute if self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.vector)))
                }
                _ => {
                    self.stage = BodyStage::Item;
                    match self.rec.feed_event(input)? {
                        Ok(t) => {
                            self.vector.push(t);
                            self.rec.reset();
                            self.stage = BodyStage::Between;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
            },
            BodyStage::Item => match self.rec.feed_event(input)? {
                Ok(t) => {
                    self.vector.push(t);
                    self.rec.reset();
                    self.stage = BodyStage::Between;
                    None
                }
                Err(e) => Some(Err(e)),
            },
        }
    }

    fn reset(&mut self) {
        self.stage = BodyStage::Init;
        self.vector.clear();
        self.rec.reset();
    }
}

impl<T, R> VecRecognizer<T, R> {
    pub fn new(is_attr_body: bool, rec: R) -> Self {
        let stage = if is_attr_body {
            BodyStage::Between
        } else {
            BodyStage::Init
        };
        VecRecognizer {
            is_attr_body,
            stage,
            vector: vec![],
            rec,
        }
    }
}

pub type CollaspsibleRec<R> = FirstOf<R, SimpleAttrBody<R>>;

impl<T: RecognizerReadable> RecognizerReadable for Vec<T> {
    type Rec = VecRecognizer<T, T::Rec>;
    type AttrRec = CollaspsibleRec<VecRecognizer<T, T::Rec>>;
    type BodyRec = VecRecognizer<T, T::Rec>;

    fn make_recognizer() -> Self::Rec {
        VecRecognizer::new(false, T::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        FirstOf::new(
            VecRecognizer::new(true, T::make_recognizer()),
            SimpleAttrBody::new(VecRecognizer::new(false, T::make_recognizer())),
        )
    }

    fn make_body_recognizer() -> Self::BodyRec {
        Self::make_recognizer()
    }
}

#[derive(Debug)]
pub struct OptionRecognizer<R> {
    inner: R,
    reading_value: bool,
}

impl<R> OptionRecognizer<R> {
    fn new(inner: R) -> Self {
        OptionRecognizer {
            inner,
            reading_value: false,
        }
    }
}

impl<R: Recognizer> Recognizer for OptionRecognizer<R> {
    type Target = Option<R::Target>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let OptionRecognizer {
            inner,
            reading_value,
        } = self;
        if *reading_value {
            inner.feed_event(input).map(|r| r.map(Option::Some))
        } else {
            match &input {
                ReadEvent::Extant => {
                    let r = inner.feed_event(ReadEvent::Extant);
                    if matches!(&r, Some(Err(_))) {
                        Some(Ok(None))
                    } else {
                        r.map(|r| r.map(Option::Some))
                    }
                }
                _ => {
                    *reading_value = true;
                    inner.feed_event(input).map(|r| r.map(Option::Some))
                }
            }
        }
    }

    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        match self.inner.try_flush() {
            Some(Ok(r)) => Some(Ok(Some(r))),
            _ => Some(Ok(None)),
        }
    }

    fn reset(&mut self) {
        self.reading_value = false;
        self.inner.reset();
    }
}

pub type MakeOption<T> = fn(T) -> Option<T>;

impl<T: RecognizerReadable> RecognizerReadable for Option<T> {
    type Rec = OptionRecognizer<T::Rec>;
    type AttrRec = FirstOf<EmptyAttrRecognizer<T>, MappedRecognizer<T::AttrRec, MakeOption<T>>>;
    type BodyRec = FirstOf<EmptyBodyRecognizer<T>, MappedRecognizer<T::BodyRec, MakeOption<T>>>;

    fn make_recognizer() -> Self::Rec {
        OptionRecognizer::new(T::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        FirstOf::new(
            EmptyAttrRecognizer::default(),
            MappedRecognizer::new(T::make_attr_recognizer(), Option::Some),
        )
    }

    fn make_body_recognizer() -> Self::BodyRec {
        FirstOf::new(
            EmptyBodyRecognizer::default(),
            MappedRecognizer::new(T::make_body_recognizer(), Option::Some),
        )
    }

    fn on_absent() -> Option<Self> {
        Some(None)
    }

    fn is_simple() -> bool {
        T::is_simple()
    }
}

pub struct EmptyBodyRecognizer<T> {
    seen_start: bool,
    _type: PhantomData<fn() -> Option<T>>,
}

impl<T> Default for EmptyBodyRecognizer<T> {
    fn default() -> Self {
        EmptyBodyRecognizer {
            seen_start: false,
            _type: PhantomData,
        }
    }
}

impl<T> Recognizer for EmptyBodyRecognizer<T> {
    type Target = Option<T>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if self.seen_start {
            if matches!(input, ReadEvent::EndRecord) {
                Some(Ok(None))
            } else {
                Some(Err(input.kind_error(ExpectedEvent::EndOfRecord)))
            }
        } else if matches!(input, ReadEvent::StartBody) {
            self.seen_start = true;
            None
        } else {
            Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
        }
    }

    fn reset(&mut self) {
        self.seen_start = false;
    }
}

pub struct EmptyAttrRecognizer<T> {
    seen_extant: bool,
    _type: PhantomData<fn() -> Option<T>>,
}

impl<T> Default for EmptyAttrRecognizer<T> {
    fn default() -> Self {
        EmptyAttrRecognizer {
            seen_extant: false,
            _type: PhantomData,
        }
    }
}

impl<T> Recognizer for EmptyAttrRecognizer<T> {
    type Target = Option<T>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if matches!(&input, ReadEvent::EndAttribute) {
            Some(Ok(None))
        } else if !self.seen_extant && matches!(&input, ReadEvent::Extant) {
            self.seen_extant = true;
            None
        } else {
            Some(Err(input.kind_error(ExpectedEvent::EndOfAttribute)))
        }
    }

    fn reset(&mut self) {
        self.seen_extant = false;
    }
}

#[derive(Debug)]
enum MapStage {
    Init,
    Between,
    Key,
    Slot,
    Value,
}

/// [`Recognizer`] for [`HashMap`]s encoded as key-value pairs in the slots of the record body.
#[derive(Debug)]
pub struct HashMapRecognizer<RK: Recognizer, RV: Recognizer> {
    is_attr_body: bool,
    stage: MapStage,
    key: Option<RK::Target>,
    map: HashMap<RK::Target, RV::Target>,
    key_rec: RK,
    val_rec: RV,
}

impl<RK: Recognizer, RV: Recognizer> HashMapRecognizer<RK, RV> {
    fn new(key_rec: RK, val_rec: RV) -> Self {
        HashMapRecognizer {
            is_attr_body: false,
            stage: MapStage::Init,
            key: None,
            map: HashMap::new(),
            key_rec,
            val_rec,
        }
    }

    fn new_attr(key_rec: RK, val_rec: RV) -> Self {
        HashMapRecognizer {
            is_attr_body: true,
            stage: MapStage::Between,
            key: None,
            map: HashMap::new(),
            key_rec,
            val_rec,
        }
    }
}

impl<K, V> RecognizerReadable for HashMap<K, V>
where
    K: Eq + Hash + RecognizerReadable,
    V: RecognizerReadable,
{
    type Rec = HashMapRecognizer<K::Rec, V::Rec>;
    type AttrRec = HashMapRecognizer<K::Rec, V::Rec>;
    type BodyRec = HashMapRecognizer<K::Rec, V::Rec>;

    fn make_recognizer() -> Self::Rec {
        HashMapRecognizer::new(K::make_recognizer(), V::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        HashMapRecognizer::new_attr(K::make_recognizer(), V::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        HashMapRecognizer::new(K::make_recognizer(), V::make_recognizer())
    }
}

impl<RK, RV> Recognizer for HashMapRecognizer<RK, RV>
where
    RK: Recognizer,
    RV: Recognizer,
    RK::Target: Eq + Hash,
{
    type Target = HashMap<RK::Target, RV::Target>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            MapStage::Init => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.stage = MapStage::Between;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            MapStage::Between => match &input {
                ReadEvent::EndRecord if !self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.map)))
                }
                ReadEvent::EndAttribute if self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.map)))
                }
                _ => {
                    self.stage = MapStage::Key;
                    match self.key_rec.feed_event(input)? {
                        Ok(t) => {
                            self.key = Some(t);
                            self.key_rec.reset();
                            self.stage = MapStage::Slot;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
            },
            MapStage::Key => match self.key_rec.feed_event(input)? {
                Ok(t) => {
                    self.key = Some(t);
                    self.key_rec.reset();
                    self.stage = MapStage::Slot;
                    None
                }
                Err(e) => Some(Err(e)),
            },
            MapStage::Slot => {
                if matches!(&input, ReadEvent::Slot) {
                    self.stage = MapStage::Value;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            MapStage::Value => match self.val_rec.feed_event(input)? {
                Ok(v) => {
                    if let Some(k) = self.key.take() {
                        self.map.insert(k, v);
                        self.val_rec.reset();
                        self.stage = MapStage::Between;
                        None
                    } else {
                        Some(Err(ReadError::InconsistentState))
                    }
                }
                Err(e) => Some(Err(e)),
            },
        }
    }

    fn reset(&mut self) {
        self.key = None;
        self.stage = MapStage::Init;
        self.key_rec.reset();
        self.val_rec.reset();
    }
}

macro_rules! impl_readable_tuple {
    ( $len:expr => ($([$idx:pat, $pname:ident, $vname:ident, $rname:ident])+)) => {
        const _: () = {

            type Builder<$($pname),+> = (($(Option<$pname>,)+), ($(<$pname as RecognizerReadable>::Rec,)+));

            fn select_feed<$($pname: RecognizerReadable),+>(builder: &mut Builder<$($pname),+>, i: u32, input: ReadEvent<'_>) -> Option<Result<(), ReadError>> {
                let (($($vname,)+), ($($rname,)+)) = builder;
                match i {
                    $(
                        $idx => {
                            let result = $rname.feed_event(input)?;
                            match result {
                                Ok(v) => {
                                    *$vname = Some(v);
                                    Some(Ok(()))
                                },
                                Err(e) => {
                                    Some(Err(e))
                                }
                            }
                        },
                    )+
                    _ => Some(Err(ReadError::InconsistentState))
                }

            }

            fn on_done<$($pname: RecognizerReadable),+>(builder: &mut Builder<$($pname),+>) -> Result<($($pname,)+), ReadError> {
                let (($($vname,)+), _) = builder;
                if let ($(Some($vname),)+) = ($($vname.take(),)+) {
                    Ok(($($vname,)+))
                } else {
                    Err(ReadError::IncompleteRecord)
                }
            }

            fn reset<$($pname: RecognizerReadable),+>(builder: &mut Builder<$($pname),+>) {
                let (($($vname,)+), ($($rname,)+)) = builder;

                $(*$vname = None;)+
                $($rname.reset();)+
            }

            impl<$($pname: RecognizerReadable),+> RecognizerReadable for ($($pname,)+) {
                type Rec = OrdinalFieldsRecognizer<($($pname,)+), Builder<$($pname),+>>;
                type AttrRec = FirstOf<SimpleAttrBody<Self::Rec>, Self::Rec>;
                type BodyRec = Self::Rec;

                fn make_recognizer() -> Self::Rec {
                    OrdinalFieldsRecognizer::new(
                        (Default::default(), ($(<$pname as RecognizerReadable>::make_recognizer(),)+)),
                        $len,
                        select_feed,
                        on_done,
                        reset,
                    )
                }

                fn make_attr_recognizer() -> Self::AttrRec {
                    let attr = OrdinalFieldsRecognizer::new_attr(
                        (Default::default(), ($(<$pname as RecognizerReadable>::make_recognizer(),)+)),
                        $len,
                        select_feed,
                        on_done,
                        reset,
                    );
                    FirstOf::new(SimpleAttrBody::new(Self::make_recognizer()), attr)
                }

                fn make_body_recognizer() -> Self::BodyRec {
                    Self::make_recognizer()
                }
            }
        };
    }
}

impl_readable_tuple! { 1 => ([0, T0, v0, r0]) }
impl_readable_tuple! { 2 => ([0, T0, v0, r0] [1, T1, v1, r1]) }
impl_readable_tuple! { 3 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2]) }
impl_readable_tuple! { 4 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3]) }
impl_readable_tuple! { 5 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4]) }
impl_readable_tuple! { 6 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5]) }
impl_readable_tuple! { 7 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6]) }
impl_readable_tuple! { 8 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6] [7, T7, v7, r7]) }
impl_readable_tuple! { 9 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6] [7, T7, v7, r7] [8, T8, v8, r8]) }
impl_readable_tuple! { 10 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6] [7, T7, v7, r7] [8, T8, v8, r8] [9, T9, v9, r9]) }
impl_readable_tuple! { 11 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6] [7, T7, v7, r7] [8, T8, v8, r8] [9, T9, v9, r9] [10, T10, v10, r10]) }
impl_readable_tuple! { 12 => ([0, T0, v0, r0] [1, T1, v1, r1] [2, T2, v2, r2] [3, T3, v3, r3] [4, T4, v4, r4] [5, T5, v5, r5] [6, T6, v6, r6] [7, T7, v7, r7] [8, T8, v8, r8] [9, T9, v9, r9] [10, T10, v10, r10] [11, T11, v11, r11]) }
