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

/// [`Recognizer`] implementations for basic types.
pub mod primitive;
#[cfg(test)]
mod tests;

use crate::form::structural::generic::coproduct::{CCons, CNil, Unify};
use crate::form::structural::read::error::ExpectedEvent;
use crate::form::structural::read::event::{NumericValue, ReadEvent};
use crate::form::structural::read::materializers::value::{
    AttrBodyMaterializer, DelegateBodyMaterializer, ValueMaterializer,
};
use crate::form::structural::read::recognizer::primitive::{
    DataRecognizer, NonZeroUsizeRecognizer,
};
use crate::form::structural::read::ReadError;
use crate::form::structural::Tag;
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Value, ValueKind};
use crate::warp::path::AbsolutePath;
use flate2::Compression;
use num_bigint::{BigInt, BigUint};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::option::Option::None;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::extensions::compression::WsCompression;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;
use utilities::future::retryable::strategy::{
    Quantity, RetryStrategy, DEFAULT_EXPONENTIAL_MAX_BACKOFF, DEFAULT_EXPONENTIAL_MAX_INTERVAL,
    DEFAULT_IMMEDIATE_RETRIES, DEFAULT_INTERVAL_DELAY, DEFAULT_INTERVAL_RETRIES,
};
use utilities::iteratee::Iteratee;
use utilities::uri::RelativeUri;

/// Trait for types that can be recognized by a [`Recognizer`] state machine.
pub trait RecognizerReadable: Sized {
    type Rec: Recognizer<Target = Self>;
    type AttrRec: Recognizer<Target = Self>;
    type BodyRec: Recognizer<Target = Self>;

    /// Create a state machine that will recognize the representation of the type as a stream of
    /// [`ReadEvent`]s.
    fn make_recognizer() -> Self::Rec;

    /// Create a state machine that will recognize the represenation of the type as a stream of
    /// [`ReadEvent`]s from the body of an attribute. (In most cases this will be indentical
    /// to the `make_recognizer` state machine, followed by an end attribute event. However,
    /// for types that can be reprsented as records with no attributes, this may be more complex
    /// as it is permissible for the record to be collapsed into the body of the attribute.)
    fn make_attr_recognizer() -> Self::AttrRec;

    /// Create a state machine that will recognize the represenation of the type as a stream of
    /// [`ReadEvent`]s where the read process has been delegated to a sub-value. This will
    /// be identical to the `make_recognizer` state machine save where the type is simple or
    /// is the generic model type [`Value`].
    fn make_body_recognizer() -> Self::BodyRec;

    /// If this value is expected as the value of a record field but was not present a default
    /// value will be used if provided here. For example, [`Option`] values are set to `None` if
    /// not provided.
    fn on_absent() -> Option<Self> {
        None
    }

    /// A value is simple if it can be represented by a single [`ReadEvent`].
    fn is_simple() -> bool {
        false
    }
}

/// Trait for state machines that can recognize a type from a stream of [`ReadEvent`]s. A
/// recognizer maybe be used multiple times but the `reset` method must be called between each
/// use.
pub trait Recognizer {
    /// The type that is produced by the state machine.
    type Target;

    /// Feed a single event into the state machine. The result will return nothing if more
    /// events are required. Otherwise it will return the completed value or an error if the
    /// event provided was invalid for the type.
    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>>;

    /// Attempt the produce a value based on the events that have already been provided, if possible.
    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        None
    }

    /// Reset the state machine so that it can be used again.
    fn reset(&mut self);

    /// Wrap this state machine as an [`Iteratee`].
    fn as_iteratee(&mut self) -> RecognizerIteratee<'_, Self>
    where
        Self: Sized,
    {
        RecognizerIteratee(self)
    }
}

pub struct RecognizerIteratee<'b, R>(&'b mut R);

impl<'a, 'b, R> Iteratee<ReadEvent<'a>> for RecognizerIteratee<'b, R>
where
    R: Recognizer,
{
    type Item = Result<R::Target, ReadError>;

    fn feed(&mut self, input: ReadEvent<'a>) -> Option<Self::Item> {
        let RecognizerIteratee(inner) = self;
        inner.feed_event(input)
    }

    fn flush(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let RecognizerIteratee(inner) = self;
        inner.try_flush()
    }
}
macro_rules! simple_readable {
    ($target:ty, $recog:ident) => {
        impl RecognizerReadable for $target {
            type Rec = primitive::$recog;
            type AttrRec = SimpleAttrBody<primitive::$recog>;
            type BodyRec = SimpleRecBody<primitive::$recog>;

            fn make_recognizer() -> Self::Rec {
                primitive::$recog
            }

            fn make_attr_recognizer() -> Self::AttrRec {
                SimpleAttrBody::new(primitive::$recog)
            }

            fn make_body_recognizer() -> Self::BodyRec {
                SimpleRecBody::new(primitive::$recog)
            }

            fn is_simple() -> bool {
                true
            }
        }
    };
}

simple_readable!((), UnitRecognizer);
simple_readable!(i32, I32Recognizer);
simple_readable!(i64, I64Recognizer);
simple_readable!(u32, U32Recognizer);
simple_readable!(u64, U64Recognizer);
simple_readable!(usize, UsizeRecognizer);
simple_readable!(NonZeroUsize, NonZeroUsizeRecognizer);
simple_readable!(f64, F64Recognizer);
simple_readable!(BigInt, BigIntRecognizer);
simple_readable!(BigUint, BigUintRecognizer);
simple_readable!(String, StringRecognizer);
simple_readable!(Text, TextRecognizer);
simple_readable!(Vec<u8>, DataRecognizer);
simple_readable!(bool, BoolRecognizer);

impl RecognizerReadable for Blob {
    type Rec = MappedRecognizer<DataRecognizer, fn(Vec<u8>) -> Blob>;
    type AttrRec = SimpleAttrBody<Self::Rec>;
    type BodyRec = SimpleRecBody<Self::Rec>;

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(DataRecognizer, Blob::from_vec)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }

    fn is_simple() -> bool {
        true
    }
}

type BoxU8Vec = fn(Vec<u8>) -> Box<[u8]>;

impl RecognizerReadable for Box<[u8]> {
    type Rec = MappedRecognizer<DataRecognizer, BoxU8Vec>;
    type AttrRec = SimpleAttrBody<Self::Rec>;
    type BodyRec = SimpleRecBody<Self::Rec>;

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(DataRecognizer, Vec::into_boxed_slice)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(Self::make_recognizer())
    }

    fn is_simple() -> bool {
        true
    }
}

impl RecognizerReadable for RelativeUri {
    type Rec = RelativeUriRecognizer;
    type AttrRec = SimpleAttrBody<RelativeUriRecognizer>;
    type BodyRec = SimpleRecBody<RelativeUriRecognizer>;

    fn make_recognizer() -> Self::Rec {
        RelativeUriRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(RelativeUriRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(RelativeUriRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

pub struct RelativeUriRecognizer;

impl Recognizer for RelativeUriRecognizer {
    type Target = RelativeUri;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                let result = RelativeUri::from_str(txt.borrow());
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

impl RecognizerReadable for Url {
    type Rec = UrlRecognizer;
    type AttrRec = SimpleAttrBody<UrlRecognizer>;
    type BodyRec = SimpleRecBody<UrlRecognizer>;

    fn make_recognizer() -> Self::Rec {
        UrlRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(UrlRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(UrlRecognizer)
    }

    fn is_simple() -> bool {
        true
    }
}

pub struct UrlRecognizer;

impl Recognizer for UrlRecognizer {
    type Target = Url;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                let result = Url::from_str(txt.borrow());
                let url = result.map_err(move |_| ReadError::Malformatted {
                    text: Text::from(txt),
                    message: Text::new("Not a valid URL."),
                });
                Some(url)
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

/// Recognizes a vector of values of the same type.
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
    fn new(is_attr_body: bool, rec: R) -> Self {
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

#[derive(Clone, Copy)]
enum BodyStage {
    Init,
    Item,
    Between,
}

type Selector<Flds> = for<'a> fn(&mut Flds, u32, ReadEvent<'a>) -> Option<Result<(), ReadError>>;

struct Bitset(u32, u64);

impl Bitset {
    fn new(cap: u32) -> Self {
        Bitset(cap, 0)
    }

    fn set(&mut self, index: u32) {
        let Bitset(cap, bits) = self;
        if index <= *cap {
            *bits |= 1 << index
        }
    }

    fn get(&self, index: u32) -> Option<bool> {
        let Bitset(cap, bits) = self;
        if index <= *cap {
            Some((*bits >> index) & 0x1 != 0)
        } else {
            None
        }
    }

    fn clear(&mut self) {
        self.1 = 0
    }
}

enum BodyFieldState {
    Init,
    Between,
    ExpectingSlot,
    SlotValue,
}

/// A [`Recognizer`] that can be configured to recognize any type that can be represented as a
/// record consisting of named fields. The `Flds` type is the state of the state machine that
/// stores the value for each named field as it is encouted in the output. Another function is then
/// called to transform this state into the final result.
pub struct NamedFieldsRecognizer<T, Flds> {
    is_attr_body: bool,
    state: BodyFieldState,
    fields: Flds,
    progress: Bitset,
    select_index: fn(&str) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> NamedFieldsRecognizer<T, Flds> {
    /// Configure a recognizer.
    ///
    /// #Arguments
    ///
    /// * `fields` - The state for the state machine.
    /// * `select_index` - Map the field names of the type to integer indicies.
    /// * `num_fields` - The total number of named fields in the representation.
    /// * `select_recog` - Attempt to update the state with the next read event.
    /// * `on_done` - Called when the recognizer is complete to convert the state to the output type.
    /// * `reset` - Called to reset the state to its initial value.
    pub fn new(
        fields: Flds,
        select_index: fn(&str) -> Option<u32>,
        num_fields: u32,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        NamedFieldsRecognizer {
            is_attr_body: false,
            state: BodyFieldState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index,
            index: 0,
            select_recog,
            on_done,
            reset,
        }
    }

    /// Configure a recognizer for the type unpacked into an attribute body.
    ///
    /// #Arguments
    ///
    /// * `fields` - The state for the state machine.
    /// * `select_index` - Map the field names of the type to integer indicies.
    /// * `num_fields` - The total number of named fields in the representation.
    /// * `select_recog` - Attempt to update the state with the next read event.
    /// * `on_done` - Called when the recognizer is complete to convert the state to the output type.
    /// * `reset` - Called to reset the state to its initial value.
    pub fn new_attr(
        fields: Flds,
        select_index: fn(&str) -> Option<u32>,
        num_fields: u32,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        NamedFieldsRecognizer {
            is_attr_body: true,
            state: BodyFieldState::Between,
            fields,
            progress: Bitset::new(num_fields),
            select_index,
            index: 0,
            select_recog,
            on_done,
            reset,
        }
    }
}

impl<T, Flds> Recognizer for NamedFieldsRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let NamedFieldsRecognizer {
            is_attr_body,
            state,
            fields,
            progress,
            select_index,
            index,
            select_recog,
            on_done,
            ..
        } = self;
        match state {
            BodyFieldState::Init => {
                if matches!(input, ReadEvent::StartBody) {
                    *state = BodyFieldState::Between;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            BodyFieldState::Between => match input {
                ReadEvent::EndRecord if !*is_attr_body => Some(on_done(fields)),
                ReadEvent::EndAttribute if *is_attr_body => Some(on_done(fields)),
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(name.borrow()) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::from(name))))
                        } else {
                            *index = i;
                            *state = BodyFieldState::ExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedSlot))
                    }
                }
                ow => {
                    let mut expected = vec![];
                    if *is_attr_body {
                        expected.push(ExpectedEvent::EndOfAttribute);
                    } else {
                        expected.push(ExpectedEvent::EndOfRecord);
                    }
                    expected.push(ExpectedEvent::ValueEvent(ValueKind::Text));
                    Some(Err(ow.kind_error(ExpectedEvent::Or(expected))))
                }
            },
            BodyFieldState::ExpectingSlot => {
                if matches!(input, ReadEvent::Slot) {
                    *state = BodyFieldState::SlotValue;
                    None
                } else {
                    Some(Err(ReadError::unexpected_kind(
                        ValueKind::Text,
                        Some(ExpectedEvent::Slot),
                    )))
                }
            }
            BodyFieldState::SlotValue => {
                let r = select_recog(fields, *index, input)?;
                if let Err(e) = r {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = BodyFieldState::Between;
                    None
                }
            }
        }
    }

    fn reset(&mut self) {
        self.progress.clear();
        self.state = BodyFieldState::Init;
        (self.reset)(&mut self.fields)
    }
}

/// A [`Recognizer`] that can be configured to recognize any type that can be represented as a
/// record consisting of a tuple of values. The `Flds` type is the state of the state machine that
/// stores the value for each named field as it is encouted in the output. Another function is then
/// called to transform this state into the final result.
pub struct OrdinalFieldsRecognizer<T, Flds> {
    is_attr_body: bool,
    state: BodyStage,
    fields: Flds,
    num_fields: u32,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> OrdinalFieldsRecognizer<T, Flds> {
    /// Configure a recognizer.
    ///
    /// #Arguments
    ///
    /// * `fields` - The state for the state machine.
    /// * `num_fields` - The total number of named fields in the representation.
    /// * `select_recog` - Attempt to update the state with the next read event.
    /// * `on_done` - Called when the recognizer is complete to convert the state to the output type.
    /// * `reset` - Called to reset the state to its initial value.
    pub fn new(
        fields: Flds,
        num_fields: u32,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        OrdinalFieldsRecognizer {
            is_attr_body: false,
            state: BodyStage::Init,
            fields,
            num_fields,
            index: 0,
            select_recog,
            on_done,
            reset,
        }
    }

    /// Configure a recognizer for the type unpacked into an attribute body.
    ///
    /// #Arguments
    ///
    /// * `fields` - The state for the state machine.
    /// * `num_fields` - The total number of named fields in the representation.
    /// * `select_recog` - Attempt to update the state with the next read event.
    /// * `on_done` - Called when the recognizer is complete to convert the state to the output type.
    /// * `reset` - Called to reset the state to its initial value.
    pub fn new_attr(
        fields: Flds,
        num_fields: u32,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        OrdinalFieldsRecognizer {
            is_attr_body: true,
            state: BodyStage::Between,
            fields,
            num_fields,
            index: 0,
            select_recog,
            on_done,
            reset,
        }
    }
}

impl<T, Flds> Recognizer for OrdinalFieldsRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let OrdinalFieldsRecognizer {
            is_attr_body,
            state,
            fields,
            num_fields,
            index,
            select_recog,
            on_done,
            ..
        } = self;
        if matches!(state, BodyStage::Init) {
            if matches!(input, ReadEvent::StartBody) {
                *state = BodyStage::Between;
                None
            } else {
                Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
            }
        } else if matches!(state, BodyStage::Between)
            && ((!*is_attr_body && matches!(&input, ReadEvent::EndRecord))
                || (*is_attr_body && matches!(&input, ReadEvent::EndAttribute)))
        {
            Some(on_done(fields))
        } else if *index == *num_fields {
            Some(Err(ReadError::UnexpectedItem))
        } else {
            let r = select_recog(fields, *index, input)?;
            if let Err(e) = r {
                Some(Err(e))
            } else {
                *index += 1;
                *state = BodyStage::Between;
                None
            }
        }
    }

    fn reset(&mut self) {
        self.index = 0;
        self.state = BodyStage::Init;
        (self.reset)(&mut self.fields)
    }
}

/// Wraps another [`Recognizer`] to recognize the same type as an attribute body (terminated by
/// a final end attribute event).
pub struct SimpleAttrBody<R: Recognizer> {
    after_content: bool,
    value: Option<R::Target>,
    delegate: R,
}

impl<R: Recognizer> SimpleAttrBody<R> {
    pub fn new(rec: R) -> Self {
        SimpleAttrBody {
            after_content: false,
            value: None,
            delegate: rec,
        }
    }
}

impl<R: Recognizer> Recognizer for SimpleAttrBody<R> {
    type Target = R::Target;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if self.after_content {
            if matches!(&input, ReadEvent::EndAttribute) {
                if let Some(t) = self.value.take() {
                    Some(Ok(t))
                } else {
                    Some(Err(ReadError::IncompleteRecord))
                }
            } else {
                Some(Err(input.kind_error(ExpectedEvent::EndOfAttribute)))
            }
        } else {
            let r = self.delegate.feed_event(input)?;
            self.after_content = true;
            match r {
                Ok(t) => {
                    self.value = Some(t);
                    None
                }
                Err(e) => Some(Err(e)),
            }
        }
    }

    fn reset(&mut self) {
        self.after_content = false;
        self.value = None;
        self.delegate.reset();
    }
}

/// Runs two [`Recognizer`]s in parallel returning the result of the first that completes
/// successfully. If both complete with an error, the error from the recognizer that failed
/// last is returned.
pub struct FirstOf<R1, R2> {
    first_active: bool,
    second_active: bool,
    recognizer1: R1,
    recognizer2: R2,
}

impl<R1, R2> FirstOf<R1, R2>
where
    R1: Recognizer,
    R2: Recognizer<Target = R1::Target>,
{
    pub fn new(recognizer1: R1, recognizer2: R2) -> Self {
        FirstOf {
            first_active: true,
            second_active: true,
            recognizer1,
            recognizer2,
        }
    }
}

impl<R1, R2> Recognizer for FirstOf<R1, R2>
where
    R1: Recognizer,
    R2: Recognizer<Target = R1::Target>,
{
    type Target = R1::Target;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let FirstOf {
            first_active,
            second_active,
            recognizer1,
            recognizer2,
        } = self;
        if *first_active && *second_active {
            match recognizer1.feed_event(input.clone()) {
                r @ Some(Ok(_)) => {
                    *first_active = false;
                    *second_active = false;
                    return r;
                }
                Some(Err(_)) => {
                    *first_active = false;
                }
                _ => {}
            }
            match recognizer2.feed_event(input) {
                r @ Some(Ok(_)) => {
                    *first_active = false;
                    *second_active = false;
                    r
                }
                r @ Some(Err(_)) => {
                    *second_active = false;
                    if *first_active {
                        None
                    } else {
                        r
                    }
                }
                _ => None,
            }
        } else if *first_active {
            let r = recognizer1.feed_event(input)?;
            *first_active = false;
            Some(r)
        } else if *second_active {
            let r = recognizer2.feed_event(input)?;
            *second_active = false;
            Some(r)
        } else {
            Some(Err(ReadError::InconsistentState))
        }
    }

    fn reset(&mut self) {
        self.first_active = true;
        self.second_active = true;
        self.recognizer1.reset();
        self.recognizer2.reset();
    }
}

/// A key to specify the field that has been encounted in the stream of events when reading
/// a type with labelled body fields.
#[derive(Clone, Copy)]
pub enum LabelledFieldKey<'a> {
    /// The name of the tag when this is used to populate a field.
    Tag,
    Header,
    /// Another attribute after the tag.
    Attr(&'a str),
    /// A labelled slot in the body of the record.
    Item(&'a str),
}

#[derive(Clone, Copy)]
enum LabelledStructState {
    Init,
    Header,
    NoHeader,
    AttrBetween,
    AttrItem,
    BodyBetween,
    BodyExpectingSlot,
    BodyItem,
}

#[derive(Clone, Copy)]
enum OrdinalStructState {
    Init,
    Header,
    NoHeader,
    AttrBetween,
    AttrItem,
    BodyBetween,
    BodyItem,
}

/// A key to specify the field that has been encounted in the stream of events when reading
/// a type with un-labelled body fields.
#[derive(Clone, Copy)]
pub enum OrdinalFieldKey<'a> {
    /// The name of the tag when this is used to populate a field.
    Tag,
    Header,
    /// Another attribute after the tag.
    Attr(&'a str),
    /// The first field in the body of the record. All other body fields are assumed to ocurr
    /// sequentially after the first.
    FirstItem,
}

/// Specifies whether the tag attribute for a record is expected to be a fixed string or should
/// be used to populate one of the fields.
pub enum TagSpec {
    Fixed(&'static str),
    Field,
}

/// This type is used to encode the standard encoding of Rust structs, with labelled fields, as
/// generated by the derivation macro for [`RecognizerReadable`]. It should not generally be
/// necessary to use this type explicitly.
pub struct LabelledStructRecognizer<T, Flds> {
    tag: TagSpec,
    state: LabelledStructState,
    fields: Flds,
    progress: Bitset,
    index: u32,
    vtable: LabelledVTable<T, Flds>,
}

/// The derivation macro produces the functions that are used to populate this table to provide
/// the specific parts of the implementation.
pub struct LabelledVTable<T, Flds> {
    select_index: for<'a> fn(LabelledFieldKey<'a>) -> Option<u32>,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> LabelledVTable<T, Flds> {
    pub fn new(
        select_index: for<'a> fn(LabelledFieldKey<'a>) -> Option<u32>,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        LabelledVTable {
            select_index,
            select_recog,
            on_done,
            reset,
        }
    }
}

/// The derivation macro produces the functions that are used to populate this table to provide
/// the specific parts of the implementation.
pub struct OrdinalVTable<T, Flds> {
    select_index: for<'a> fn(OrdinalFieldKey<'a>) -> Option<u32>,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> OrdinalVTable<T, Flds> {
    pub fn new(
        select_index: for<'a> fn(OrdinalFieldKey<'a>) -> Option<u32>,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        OrdinalVTable {
            select_index,
            select_recog,
            on_done,
            reset,
        }
    }
}

impl<T, Flds> LabelledStructRecognizer<T, Flds> {
    /// #Arguments
    /// * `tag` - The expected name of the first attribute or an inidcation that it should be used
    /// to populate a field.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn new(
        tag: TagSpec,
        fields: Flds,
        num_fields: u32,
        vtable: LabelledVTable<T, Flds>,
    ) -> Self {
        LabelledStructRecognizer {
            tag,
            state: LabelledStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            index: 0,
            vtable,
        }
    }

    /// This constructor is used for enumeration variants. The primary different is that the tag
    /// of the record has already been read before this recognizers is called so the initial state
    /// is skipped.
    ///
    /// #Arguments
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn variant(fields: Flds, num_fields: u32, vtable: LabelledVTable<T, Flds>) -> Self {
        let (state, index) = if let Some(i) = (vtable.select_index)(LabelledFieldKey::Header) {
            (LabelledStructState::Header, i)
        } else {
            (LabelledStructState::NoHeader, 0)
        };
        LabelledStructRecognizer {
            tag: TagSpec::Fixed(""),
            state,
            fields,
            progress: Bitset::new(num_fields),
            index,
            vtable,
        }
    }
}

impl<T, Flds> OrdinalStructRecognizer<T, Flds> {
    /// #Arguments
    /// * `tag` - The expected name of the first attribute or an inidcation that it should be used
    /// to populate a field.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn new(
        tag: TagSpec,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        OrdinalStructRecognizer {
            tag,
            state: OrdinalStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            index: 0,
            vtable,
        }
    }

    /// This constructor is used for enumeration variants. The primary different is that the tag
    /// of the record has already been read before this recognizers is called so the initial state
    /// is skipped.
    ///
    /// #Arguments
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn variant(fields: Flds, num_fields: u32, vtable: OrdinalVTable<T, Flds>) -> Self {
        let (state, index) = if let Some(i) = (vtable.select_index)(OrdinalFieldKey::Header) {
            (OrdinalStructState::Header, i)
        } else {
            (OrdinalStructState::NoHeader, 0)
        };
        OrdinalStructRecognizer {
            tag: TagSpec::Fixed(""),
            state,
            fields,
            progress: Bitset::new(num_fields),
            index,
            vtable,
        }
    }
}

impl<T, Flds> Recognizer for LabelledStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let LabelledStructRecognizer {
            tag,
            state,
            fields,
            progress,
            index,
            vtable:
                LabelledVTable {
                    select_index,
                    select_recog,
                    on_done,
                    ..
                },
            ..
        } = self;

        match state {
            LabelledStructState::Init => match input {
                ReadEvent::StartAttribute(name) => match tag {
                    TagSpec::Fixed(tag) => {
                        if name == *tag {
                            if let Some(i) = select_index(LabelledFieldKey::Header) {
                                *index = i;
                                *state = LabelledStructState::Header;
                            } else {
                                *state = LabelledStructState::NoHeader;
                            }
                            None
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(name.into())))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(LabelledFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                if let Some(i) = select_index(LabelledFieldKey::Header) {
                                    *index = i;
                                    *state = LabelledStructState::Header;
                                } else {
                                    *state = LabelledStructState::NoHeader;
                                }
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => {
                    let name = if let TagSpec::Fixed(name) = tag {
                        Some(Text::new(name))
                    } else {
                        None
                    };
                    Some(Err(ow.kind_error(ExpectedEvent::Attribute(name))))
                }
            },
            LabelledStructState::Header => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    *state = LabelledStructState::AttrBetween;
                    None
                }
            }
            LabelledStructState::NoHeader => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    *state = LabelledStructState::AttrBetween;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            LabelledStructState::AttrBetween => match input {
                ReadEvent::StartBody => {
                    *state = LabelledStructState::BodyBetween;
                    None
                }
                ReadEvent::StartAttribute(name) => {
                    if let Some(i) = select_index(LabelledFieldKey::Attr(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = LabelledStructState::AttrItem;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::RecordBody,
                    ExpectedEvent::Attribute(None),
                ])))),
            },
            LabelledStructState::AttrItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::AttrBetween;
                    None
                }
            }
            LabelledStructState::BodyBetween => match input {
                ReadEvent::EndRecord => Some(on_done(fields)),
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(LabelledFieldKey::Item(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = LabelledStructState::BodyExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::EndOfRecord,
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                ])))),
            },
            LabelledStructState::BodyExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = LabelledStructState::BodyItem;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            LabelledStructState::BodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::BodyBetween;
                    None
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = LabelledStructState::Init;
        self.progress.clear();
        self.index = 0;
        (self.vtable.reset)(&mut self.fields);
    }
}

/// This type is used to encode the standard encoding of Rust structs, with unlabelled fields, as
/// generated by the derivation macro for [`RecognizerReadable`]. It should not generally be
/// necessary to use this type explicitly.
pub struct OrdinalStructRecognizer<T, Flds> {
    tag: TagSpec,
    state: OrdinalStructState,
    fields: Flds,
    progress: Bitset,
    index: u32,
    vtable: OrdinalVTable<T, Flds>,
}

impl<T, Flds> Recognizer for OrdinalStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let OrdinalStructRecognizer {
            tag,
            state,
            fields,
            progress,
            index,
            vtable:
                OrdinalVTable {
                    select_index,
                    select_recog,
                    on_done,
                    ..
                },
            ..
        } = self;

        match state {
            OrdinalStructState::Init => match input {
                ReadEvent::StartAttribute(name) => match tag {
                    TagSpec::Fixed(tag) => {
                        if name == *tag {
                            if let Some(i) = select_index(OrdinalFieldKey::Header) {
                                *index = i;
                                *state = OrdinalStructState::Header;
                            } else {
                                *state = OrdinalStructState::NoHeader;
                            }
                            None
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(name.into())))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(OrdinalFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                if let Some(i) = select_index(OrdinalFieldKey::Header) {
                                    *index = i;
                                    *state = OrdinalStructState::Header;
                                } else {
                                    *state = OrdinalStructState::NoHeader;
                                }
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => {
                    let name = if let TagSpec::Fixed(name) = tag {
                        Some(Text::new(name))
                    } else {
                        None
                    };
                    Some(Err(ow.kind_error(ExpectedEvent::Attribute(name))))
                }
            },
            OrdinalStructState::Header => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
            }
            OrdinalStructState::NoHeader => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            OrdinalStructState::AttrBetween => match &input {
                ReadEvent::StartBody => {
                    *state = OrdinalStructState::BodyBetween;
                    if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                    }
                    None
                }
                ReadEvent::StartAttribute(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::Attr(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = OrdinalStructState::AttrItem;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                    }
                }
                _ => Some(Err(input.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::RecordBody,
                    ExpectedEvent::Attribute(None),
                ])))),
            },
            OrdinalStructState::AttrItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
            }
            OrdinalStructState::BodyBetween => match &input {
                ReadEvent::EndRecord => Some(on_done(fields)),
                _ => {
                    *state = OrdinalStructState::BodyItem;
                    if let Err(e) = select_recog(fields, *index, input)? {
                        Some(Err(e))
                    } else {
                        progress.set(*index);
                        *index += 1;
                        *state = OrdinalStructState::BodyBetween;
                        None
                    }
                }
            },
            OrdinalStructState::BodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *index += 1;
                    *state = OrdinalStructState::BodyBetween;
                    None
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = OrdinalStructState::Init;
        self.progress.clear();
        self.index = 0;
        (self.vtable.reset)(&mut self.fields);
    }
}

type MakeArc<T> = fn(T) -> Arc<T>;

impl<T: RecognizerReadable> RecognizerReadable for Arc<T> {
    type Rec = MappedRecognizer<T::Rec, MakeArc<T>>;
    type AttrRec = MappedRecognizer<T::AttrRec, MakeArc<T>>;
    type BodyRec = MappedRecognizer<T::BodyRec, MakeArc<T>>;

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(T::make_recognizer(), Arc::new)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        MappedRecognizer::new(T::make_attr_recognizer(), Arc::new)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        MappedRecognizer::new(T::make_body_recognizer(), Arc::new)
    }

    fn on_absent() -> Option<Self> {
        T::on_absent().map(Arc::new)
    }

    fn is_simple() -> bool {
        T::is_simple()
    }
}

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

/// Runs another recognizer and then transforms its result by applying a function to it.
pub struct MappedRecognizer<R, F> {
    inner: R,
    f: F,
}

impl<R, F> MappedRecognizer<R, F> {
    pub fn new(inner: R, f: F) -> Self {
        MappedRecognizer { inner, f }
    }
}

impl<U, R, F> Recognizer for MappedRecognizer<R, F>
where
    R: Recognizer,
    F: Fn(R::Target) -> U,
{
    type Target = U;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let MappedRecognizer { inner, f, .. } = self;
        inner.feed_event(input).map(|r| r.map(f))
    }

    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        let MappedRecognizer { inner, f } = self;
        inner.try_flush().map(|r| r.map(f))
    }

    fn reset(&mut self) {
        self.inner.reset()
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

impl RecognizerReadable for Value {
    type Rec = ValueMaterializer;
    type AttrRec = AttrBodyMaterializer;
    type BodyRec = DelegateBodyMaterializer;

    fn make_recognizer() -> Self::Rec {
        ValueMaterializer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        AttrBodyMaterializer::default()
    }

    fn make_body_recognizer() -> Self::BodyRec {
        DelegateBodyMaterializer::default()
    }
}

enum MapStage {
    Init,
    Between,
    Key,
    Slot,
    Value,
}

/// [`Recognizer`] for [`HashMap`]s encoded as key-value pairs in the slots of the record body.
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

pub fn feed_field<R>(
    name: &'static str,
    field: &mut Option<R::Target>,
    recognizer: &mut R,
    event: ReadEvent<'_>,
) -> Option<Result<(), ReadError>>
where
    R: Recognizer,
{
    if field.is_some() {
        Some(Err(ReadError::DuplicateField(Text::new(name))))
    } else {
        match recognizer.feed_event(event) {
            Some(Ok(t)) => {
                *field = Some(t);
                Some(Ok(()))
            }
            Some(Err(e)) => Some(Err(e)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum DelegateStructState {
    Init,
    Header,
    NoHeader,
    AttrBetween,
    AttrItem,
    Delegated,
}

/// This type is used to encode the standard encoding of Rust structs, where the body of the record
/// is determined by one of the fields, generated by the derivation macro for
/// [`RecognizerReadable`]. It should not generally be necessary to use this type explicitly.
pub struct DelegateStructRecognizer<T, Flds> {
    tag: TagSpec,
    state: DelegateStructState,
    fields: Flds,
    progress: Bitset,
    select_index: for<'a> fn(OrdinalFieldKey<'a>) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> DelegateStructRecognizer<T, Flds> {
    /// #Arguments
    /// * `tag` - The expected name of the first attribute or an inidcation that it should be used
    /// to populate a field.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn new(
        tag: TagSpec,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        DelegateStructRecognizer {
            tag,
            state: DelegateStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }

    /// This constructor is used for enumeration variants. The primary different is that the tag
    /// of the record has already been read before this recognizers is called so the initial state
    /// is skipped.
    ///
    /// #Arguments
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn variant(fields: Flds, num_fields: u32, vtable: OrdinalVTable<T, Flds>) -> Self {
        let (state, index) = if let Some(i) = (vtable.select_index)(OrdinalFieldKey::Header) {
            (DelegateStructState::Header, i)
        } else {
            (DelegateStructState::NoHeader, 0)
        };
        DelegateStructRecognizer {
            tag: TagSpec::Fixed(""),
            state,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }
}

impl<T, Flds> Recognizer for DelegateStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let DelegateStructRecognizer {
            tag,
            state,
            fields,
            progress,
            select_index,
            index,
            select_recog,
            on_done,
            ..
        } = self;

        match state {
            DelegateStructState::Init => match input {
                ReadEvent::StartAttribute(name) => match tag {
                    TagSpec::Fixed(tag) => {
                        if name == *tag {
                            if let Some(i) = select_index(OrdinalFieldKey::Header) {
                                *index = i;
                                *state = DelegateStructState::Header;
                            } else {
                                *state = DelegateStructState::NoHeader;
                            }
                            None
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(name.into())))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(OrdinalFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                if let Some(i) = select_index(OrdinalFieldKey::Header) {
                                    *index = i;
                                    *state = DelegateStructState::Header;
                                } else {
                                    *state = DelegateStructState::NoHeader;
                                }
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => {
                    let name = if let TagSpec::Fixed(name) = tag {
                        Some(Text::new(name))
                    } else {
                        None
                    };
                    Some(Err(ow.kind_error(ExpectedEvent::Attribute(name))))
                }
            },
            DelegateStructState::Header => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    *state = DelegateStructState::AttrBetween;
                    None
                }
            }
            DelegateStructState::NoHeader => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    *state = DelegateStructState::AttrBetween;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            DelegateStructState::AttrBetween => match input {
                ReadEvent::StartBody => {
                    if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                        *state = DelegateStructState::Delegated;
                        if let Err(e) = select_recog(fields, *index, ReadEvent::StartBody)? {
                            Some(Err(e))
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    } else {
                        Some(Err(ReadError::InconsistentState))
                    }
                }
                ReadEvent::StartAttribute(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::Attr(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = DelegateStructState::AttrItem;
                            None
                        }
                    } else if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                        *state = DelegateStructState::Delegated;
                        if let Err(e) =
                            select_recog(fields, *index, ReadEvent::StartAttribute(name))?
                        {
                            Some(Err(e))
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::RecordBody,
                    ExpectedEvent::Attribute(None),
                ])))),
            },
            DelegateStructState::AttrItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = DelegateStructState::AttrBetween;
                    None
                }
            }
            DelegateStructState::Delegated => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    Some(on_done(fields))
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = DelegateStructState::Init;
        self.progress.clear();
        self.index = 0;
        (self.reset)(&mut self.fields);
    }
}

impl Recognizer for CNil {
    type Target = CNil;

    fn feed_event(&mut self, _input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        self.explode()
    }

    fn reset(&mut self) {}
}

impl<H, T> Recognizer for CCons<H, T>
where
    H: Recognizer,
    T: Recognizer,
{
    type Target = CCons<H::Target, T::Target>;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self {
            CCons::Head(h) => h.feed_event(input).map(|r| r.map(CCons::Head)),
            CCons::Tail(t) => t.feed_event(input).map(|r| r.map(CCons::Tail)),
        }
    }

    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        match self {
            CCons::Head(h) => h.try_flush().map(|r| r.map(CCons::Head)),
            CCons::Tail(t) => t.try_flush().map(|r| r.map(CCons::Tail)),
        }
    }

    fn reset(&mut self) {
        match self {
            CCons::Head(h) => h.reset(),
            CCons::Tail(t) => t.reset(),
        }
    }
}

/// This type is used to encode the standard encoding of Rust enums, generated by the derivation
/// macro for [`RecognizerReadable`]. It should not generally be necessary to use this type
/// explicitly. The type parameter `Var` is the type of a recognizer that can recognizer any of
/// the variants of the enum.
pub struct TaggedEnumRecognizer<Var> {
    select_var: fn(&str) -> Option<Var>,
    variant: Option<Var>,
}

impl<Var> TaggedEnumRecognizer<Var> {
    /// #Arguments
    /// * `select_var` - A function that configures the wrapped recognizer to expect the
    /// representation of the appropriate variant, based on the name of the tag attribute.
    pub fn new(select_var: fn(&str) -> Option<Var>) -> Self {
        TaggedEnumRecognizer {
            select_var,
            variant: None,
        }
    }
}

impl<Var> Recognizer for TaggedEnumRecognizer<Var>
where
    Var: Recognizer,
    Var::Target: Unify,
{
    type Target = <<Var as Recognizer>::Target as Unify>::Out;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let TaggedEnumRecognizer {
            select_var,
            variant,
        } = self;
        match variant {
            None => match input {
                ReadEvent::StartAttribute(name) => {
                    *variant = select_var(name.borrow());
                    if variant.is_some() {
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::Attribute(None)))),
            },
            Some(var) => var.feed_event(input).map(|r| r.map(Unify::unify)),
        }
    }

    fn reset(&mut self) {
        self.variant = None;
    }
}

#[derive(Clone, Copy)]
enum UnitStructState {
    Init,
    Tag,
    SeenExtant,
    AfterTag,
    Body,
}

/// Simple [`Recognizer`] for unit structs and enum variants. This is used by the derive macros
/// to avoid generating superfluous code in the degenerate case.
pub struct UnitStructRecognizer<T> {
    tag: &'static str,
    state: UnitStructState,
    on_done: fn() -> T,
}

impl<T> UnitStructRecognizer<T> {
    /// #Arguments
    /// * `tag` - The expected name of the tag attribute.
    /// * `on_done` - Factory to create an instance.
    pub fn new(tag: &'static str, on_done: fn() -> T) -> Self {
        UnitStructRecognizer {
            tag,
            state: UnitStructState::Init,
            on_done,
        }
    }

    /// For an enum variant, the wrapping [`Recognizer`] will already have read the tag name.
    /// #Arguments
    /// * `on_done` - Factory to create an instance.
    pub fn variant(on_done: fn() -> T) -> Self {
        UnitStructRecognizer {
            tag: "",
            state: UnitStructState::Tag,
            on_done,
        }
    }
}

impl<T> Recognizer for UnitStructRecognizer<T> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let UnitStructRecognizer {
            tag,
            state,
            on_done,
        } = self;
        match *state {
            UnitStructState::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == *tag {
                        *state = UnitStructState::Tag;
                        None
                    } else {
                        Some(Err(ReadError::UnexpectedAttribute(name.into())))
                    }
                } else {
                    Some(Err(
                        input.kind_error(ExpectedEvent::Attribute(Some(Text::new(*tag))))
                    ))
                }
            }
            UnitStructState::Tag => match input {
                ReadEvent::Extant => {
                    *state = UnitStructState::SeenExtant;
                    None
                }
                ReadEvent::EndAttribute => {
                    *state = UnitStructState::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            UnitStructState::SeenExtant => {
                if matches!(&input, ReadEvent::EndAttribute) {
                    *state = UnitStructState::AfterTag;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::EndOfAttribute)))
                }
            }
            UnitStructState::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    *state = UnitStructState::Body;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            UnitStructState::Body => {
                if matches!(&input, ReadEvent::EndRecord) {
                    Some(Ok(on_done()))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::EndOfRecord)))
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = UnitStructState::Init
    }
}

/// A recognizer that always fails (used for uninhabited types).
pub struct RecognizeNothing<T>(PhantomData<T>);

impl<T> Default for RecognizeNothing<T> {
    fn default() -> Self {
        RecognizeNothing(PhantomData)
    }
}

impl<T> Recognizer for RecognizeNothing<T> {
    type Target = T;

    fn feed_event(&mut self, _input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        Some(Err(ReadError::Message(
            format!("{} is uninhabited.", std::any::type_name::<T>()).into(),
        )))
    }

    fn reset(&mut self) {}
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
                                    None
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

pub struct TagRecognizer<T>(PhantomData<fn(&str) -> T>);

impl<T: Tag> Default for TagRecognizer<T> {
    fn default() -> Self {
        TagRecognizer(PhantomData)
    }
}

impl<T: Tag> Recognizer for TagRecognizer<T> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let result = if let ReadEvent::TextValue(txt) = input {
            T::from_str(txt.borrow()).map_err(|message| ReadError::Malformatted {
                text: txt.into(),
                message,
            })
        } else {
            Err(input.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text)))
        };
        Some(result)
    }

    fn reset(&mut self) {}
}

enum HeaderState {
    Init,
    ExpectingBody,
    BodyItem,
    BetweenSlots,
    ExpectingSlot,
    SlotItem,
    End,
}

/// A key to specify the field that has been encounted in the stream of events when reading
/// a header of a record.
#[derive(Clone, Copy)]
pub enum HeaderFieldKey<'a> {
    /// The first value inside the tag attribute.
    HeaderBody,
    /// A labelled slot in the tag attribute.
    HeaderSlot(&'a str),
}

/// The derivation macro produces the functions that are used to populate this table to provide
/// the specific parts of the implementation.
pub struct HeaderVTable<T, Flds> {
    select_index: for<'a> fn(HeaderFieldKey<'a>) -> Option<u32>,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> HeaderVTable<T, Flds> {
    pub fn new(
        select_index: for<'a> fn(HeaderFieldKey<'a>) -> Option<u32>,
        select_recog: Selector<Flds>,
        on_done: fn(&mut Flds) -> Result<T, ReadError>,
        reset: fn(&mut Flds),
    ) -> Self {
        HeaderVTable {
            select_index,
            select_recog,
            on_done,
            reset,
        }
    }
}

impl<T, Flds> Clone for HeaderVTable<T, Flds> {
    fn clone(&self) -> Self {
        HeaderVTable {
            select_index: self.select_index,
            select_recog: self.select_recog,
            on_done: self.on_done,
            reset: self.reset,
        }
    }
}

impl<T, Flds> Copy for HeaderVTable<T, Flds> {}

/// This type is used to recognize the header for types which lift fields into it. It is used
/// by the derivation macro for [`RecognizerReadable`]. It should not generally be necessary to use
/// this type explicitly.
pub struct HeaderRecognizer<T, Flds> {
    has_body: bool,
    flattened: bool,
    state: HeaderState,
    fields: Flds,
    progress: Bitset,
    index: u32,
    vtable: HeaderVTable<T, Flds>,
}

/// Create a recognizer for the body of the header attribute of a record.
///
/// #Arguments
/// * `has_body` - Whehter there is a field lifted to be the body of the header.
/// * `make_fields` - Factory to construct the state of the recognizer.
/// * `num_slots` - The number of slots lifted into the header.
/// * `vtable` - Functions that are generated by the macro that determine how incoming events
/// modify the state.
pub fn header_recognizer<T, Flds, MkFlds>(
    has_body: bool,
    make_fields: MkFlds,
    num_slots: u32,
    vtable: HeaderVTable<T, Flds>,
) -> FirstOf<HeaderRecognizer<T, Flds>, HeaderRecognizer<T, Flds>>
where
    MkFlds: Fn() -> Flds,
{
    let simple = HeaderRecognizer::new(has_body, true, num_slots, make_fields(), vtable);
    let flattened = HeaderRecognizer::new(has_body, false, num_slots, make_fields(), vtable);
    FirstOf::new(simple, flattened)
}

/// #Arguments
/// * `has_body` - Whehter there is a field lifted to be the body of the header.
/// * `flattened` - Whether the record containing the fields has been flattened into the attribute
/// body (and so does not have explicit record body delimiting).
/// * `num_slots` - The number of slots lifted into the header.
/// * `fields` - The state of the recognizer.
/// * `vtable` - Functions that are generated by the macro that determine how incoming events
/// modify the state.
impl<T, Flds> HeaderRecognizer<T, Flds> {
    pub fn new(
        has_body: bool,
        flattened: bool,
        num_slots: u32,
        fields: Flds,
        vtable: HeaderVTable<T, Flds>,
    ) -> Self {
        let state = if flattened {
            if !has_body {
                HeaderState::BetweenSlots
            } else {
                HeaderState::ExpectingBody
            }
        } else {
            HeaderState::Init
        };

        let num_fields = if !has_body { num_slots } else { num_slots + 1 };

        HeaderRecognizer {
            has_body,
            flattened,
            state,
            fields,
            progress: Bitset::new(num_fields),
            index: 0,
            vtable,
        }
    }
}

impl<T, Flds> Recognizer for HeaderRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let HeaderRecognizer {
            has_body,
            flattened,
            state,
            fields,
            progress,
            index,
            vtable:
                HeaderVTable {
                    select_index,
                    select_recog,
                    on_done,
                    ..
                },
        } = self;
        match *state {
            HeaderState::Init => {
                if matches!(&input, ReadEvent::StartBody) {
                    *state = if !*has_body {
                        HeaderState::BetweenSlots
                    } else {
                        HeaderState::ExpectingBody
                    };
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            HeaderState::ExpectingBody => {
                if *flattened && matches!(&input, ReadEvent::EndAttribute) {
                    Some(on_done(fields))
                } else if !*flattened && matches!(&input, ReadEvent::EndRecord) {
                    *state = HeaderState::End;
                    None
                } else if let Some(i) = select_index(HeaderFieldKey::HeaderBody) {
                    *index = i;
                    *state = HeaderState::BodyItem;
                    if let Err(e) = select_recog(fields, *index, input)? {
                        Some(Err(e))
                    } else {
                        *state = HeaderState::BetweenSlots;
                        progress.set(*index);
                        None
                    }
                } else {
                    Some(Err(ReadError::InconsistentState))
                }
            }
            HeaderState::BodyItem | HeaderState::SlotItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    *state = HeaderState::BetweenSlots;
                    progress.set(*index);
                    None
                }
            }
            HeaderState::BetweenSlots => match input {
                ReadEvent::EndAttribute if *flattened => Some(on_done(fields)),
                ReadEvent::EndRecord if !*flattened => {
                    *state = HeaderState::End;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(HeaderFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::from(name))))
                        } else {
                            *index = i;
                            *state = HeaderState::ExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => {
                    let mut expected = vec![];
                    if *flattened {
                        expected.push(ExpectedEvent::EndOfAttribute);
                    } else {
                        expected.push(ExpectedEvent::EndOfRecord);
                    }
                    expected.push(ExpectedEvent::ValueEvent(ValueKind::Text));
                    Some(Err(ow.kind_error(ExpectedEvent::Or(expected))))
                }
            },
            HeaderState::ExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = HeaderState::SlotItem;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            HeaderState::End => {
                if matches!(&input, ReadEvent::EndAttribute) {
                    Some(on_done(fields))
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::EndOfAttribute)))
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = if self.flattened {
            if !self.has_body {
                HeaderState::BetweenSlots
            } else {
                HeaderState::ExpectingBody
            }
        } else {
            HeaderState::Init
        };
        (self.vtable.reset)(&mut self.fields);
        self.progress.clear();
        self.index = 0;
    }
}

pub fn take_fields<T: Default, U, V>(state: &mut (T, U, V)) -> Result<T, ReadError> {
    Ok(std::mem::take(&mut state.0))
}

enum SimpleRecBodyState {
    Init,
    ReadingValue,
    AfterValue,
}

/// Wraps another simple [`Recognizer`] to recognize the same type as the single item in the body
/// of a record.
pub struct SimpleRecBody<R: Recognizer> {
    state: SimpleRecBodyState,
    value: Option<R::Target>,
    delegate: R,
}

impl<R: Recognizer> SimpleRecBody<R> {
    pub fn new(wrapped: R) -> Self {
        SimpleRecBody {
            state: SimpleRecBodyState::Init,
            value: None,
            delegate: wrapped,
        }
    }
}

impl<R: Recognizer> Recognizer for SimpleRecBody<R> {
    type Target = R::Target;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let SimpleRecBody {
            state,
            value,
            delegate,
        } = self;

        match state {
            SimpleRecBodyState::Init => {
                if matches!(input, ReadEvent::StartBody) {
                    *state = SimpleRecBodyState::ReadingValue;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            SimpleRecBodyState::ReadingValue => match delegate.feed_event(input)? {
                Ok(v) => {
                    *value = Some(v);
                    *state = SimpleRecBodyState::AfterValue;
                    None
                }
                Err(e) => Some(Err(e)),
            },
            SimpleRecBodyState::AfterValue => {
                if matches!(input, ReadEvent::EndRecord) {
                    value.take().map(Ok)
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::EndOfRecord)))
                }
            }
        }
    }

    fn reset(&mut self) {
        self.state = SimpleRecBodyState::Init;
        self.value = None;
        self.delegate.reset();
    }
}

const RETRY_IMMEDIATE_TAG: &str = "immediate";
const RETRY_INTERVAL_TAG: &str = "interval";
const RETRY_EXPONENTIAL_TAG: &str = "exponential";
const RETRY_NONE_TAG: &str = "none";
const DURATION_TAG: &str = "duration";
const WEB_SOCKET_CONFIG_TAG: &str = "websocket_connections";
const WS_COMPRESSION_NONE_TAG: &str = "none";
const WS_COMPRESSION_DEFLATE_TAG: &str = "deflate";
const ABSOLUTE_PATH_TAG: &str = "path";

enum RetryStrategyStage {
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
                        "retries" => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::ImmediateRetries);
                            *retries_recognizer = Some(NonZeroUsize::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::immediate(
                        retries.unwrap_or(NonZeroUsize::new(DEFAULT_IMMEDIATE_RETRIES).unwrap()),
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
                        "delay" => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::IntervalDelay);
                            *delay_recognizer = Some(Duration::make_recognizer());
                            None
                        }
                        "retries" => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::IntervalRetries);
                            *retries_recognizer = Some(Quantity::<NonZeroUsize>::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::interval(
                        delay.unwrap_or(Duration::from_secs(DEFAULT_INTERVAL_DELAY)),
                        retries.unwrap_or(Quantity::Finite(
                            NonZeroUsize::new(DEFAULT_INTERVAL_RETRIES).unwrap(),
                        )),
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
                        "max_interval" => {
                            self.stage = RetryStrategyStage::Slot(
                                RetryStrategyField::ExponentialMaxInterval,
                            );
                            *max_interval_recognizer = Some(Duration::make_recognizer());
                            None
                        }
                        "max_backoff" => {
                            self.stage =
                                RetryStrategyStage::Slot(RetryStrategyField::ExponentialMaxBackoff);
                            *max_backoff_recognizer = Some(Quantity::<Duration>::make_recognizer());
                            None
                        }
                        ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                    },
                    ReadEvent::EndRecord => Some(Ok(RetryStrategy::exponential(
                        max_interval
                            .unwrap_or(Duration::from_secs(DEFAULT_EXPONENTIAL_MAX_INTERVAL)),
                        max_backoff.unwrap_or(Quantity::Finite(Duration::from_secs(
                            DEFAULT_EXPONENTIAL_MAX_BACKOFF,
                        ))),
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
            ReadEvent::TextValue(value) if value == "infinite" => Some(Ok(Quantity::Infinite)),
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
                    "secs" => {
                        self.stage = DurationStage::Slot(DurationField::Secs);
                        None
                    }
                    "nanos" => {
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

impl RecognizerReadable for WebSocketConfig {
    type Rec = WebSocketConfigRecognizer;
    type AttrRec = SimpleAttrBody<WebSocketConfigRecognizer>;
    type BodyRec = SimpleRecBody<WebSocketConfigRecognizer>;

    fn make_recognizer() -> Self::Rec {
        WebSocketConfigRecognizer {
            stage: WebSocketConfigStage::Init,
            max_send_queue: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: None,
            compression: None,
            compression_recognizer: WsCompression::make_recognizer(),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(WebSocketConfigRecognizer {
            stage: WebSocketConfigStage::Init,
            max_send_queue: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: None,
            compression: None,
            compression_recognizer: WsCompression::make_recognizer(),
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(WebSocketConfigRecognizer {
            stage: WebSocketConfigStage::Init,
            max_send_queue: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: None,
            compression: None,
            compression_recognizer: WsCompression::make_recognizer(),
        })
    }
}

pub struct WebSocketConfigRecognizer {
    stage: WebSocketConfigStage,
    max_send_queue: Option<usize>,
    max_message_size: Option<usize>,
    max_frame_size: Option<usize>,
    accept_unmasked_frames: Option<bool>,
    compression: Option<WsCompression>,
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
    MaxSendQueue,
    MaxMessageSize,
    MaxFrameSize,
    AcceptUnmaskedFrames,
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
                        max_send_queue: None,
                        max_message_size: None,
                        max_frame_size: None,
                        accept_unmasked_frames: false,
                        compression: WsCompression::None(None),
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
                    "max_send_queue" => {
                        self.stage = WebSocketConfigStage::Slot(WebSocketConfigField::MaxSendQueue);
                        None
                    }
                    "max_message_size" => {
                        self.stage =
                            WebSocketConfigStage::Slot(WebSocketConfigField::MaxMessageSize);
                        None
                    }
                    "max_frame_size" => {
                        self.stage = WebSocketConfigStage::Slot(WebSocketConfigField::MaxFrameSize);
                        None
                    }
                    "accept_unmasked_frames" => {
                        self.stage =
                            WebSocketConfigStage::Slot(WebSocketConfigField::AcceptUnmaskedFrames);
                        None
                    }
                    "compression" => {
                        self.stage = WebSocketConfigStage::Slot(WebSocketConfigField::Compression);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(Ok(WebSocketConfig {
                    max_send_queue: self.max_send_queue,
                    max_message_size: self.max_message_size,
                    max_frame_size: self.max_frame_size,
                    accept_unmasked_frames: self.accept_unmasked_frames.unwrap_or(false),
                    compression: self.compression.unwrap_or(WsCompression::None(None)),
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
            WebSocketConfigStage::Field(WebSocketConfigField::MaxSendQueue) => match input {
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = usize::try_from(n) {
                        self.max_send_queue = Some(m);
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            WebSocketConfigStage::Field(WebSocketConfigField::MaxMessageSize) => match input {
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = usize::try_from(n) {
                        self.max_message_size = Some(m);
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            WebSocketConfigStage::Field(WebSocketConfigField::MaxFrameSize) => match input {
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = usize::try_from(n) {
                        self.max_frame_size = Some(m);
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            WebSocketConfigStage::Field(WebSocketConfigField::AcceptUnmaskedFrames) => {
                match input {
                    ReadEvent::Boolean(value) => {
                        self.accept_unmasked_frames = Some(value);
                        self.stage = WebSocketConfigStage::InBody;
                        None
                    }
                    ow => Some(Err(
                        ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Boolean))
                    )),
                }
            }
            WebSocketConfigStage::Field(WebSocketConfigField::Compression) => {
                match self.compression_recognizer.feed_event(input)? {
                    Ok(compression) => {
                        self.compression = Some(compression);
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
            max_send_queue,
            max_message_size,
            max_frame_size,
            accept_unmasked_frames,
            compression,
            compression_recognizer,
        } = self;

        *stage = WebSocketConfigStage::Init;
        *max_send_queue = None;
        *max_message_size = None;
        *max_frame_size = None;
        *accept_unmasked_frames = None;
        *compression = None;
        *compression_recognizer = WsCompression::make_recognizer();
    }
}

impl RecognizerReadable for WsCompression {
    type Rec = WsCompressionRecognizer;
    type AttrRec = SimpleAttrBody<WsCompressionRecognizer>;
    type BodyRec = SimpleRecBody<WsCompressionRecognizer>;

    fn make_recognizer() -> Self::Rec {
        WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None(None),
        }
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None(None),
        })
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(WsCompressionRecognizer {
            stage: WsCompressionRecognizerStage::Init,
            fields: WsCompressionFields::None(None),
        })
    }
}

pub struct WsCompressionRecognizer {
    stage: WsCompressionRecognizerStage,
    fields: WsCompressionFields,
}

pub enum WsCompressionFields {
    None(Option<usize>),
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
                            self.fields = WsCompressionFields::None(None);
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
                        WsCompressionFields::None(_) => Some(Ok(WsCompression::None(None))),
                        WsCompressionFields::Deflate { .. } => {
                            Some(Ok(WsCompression::Deflate(DeflateConfig::default())))
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
                WsCompressionFields::None(value) => match input {
                    ReadEvent::Number(NumericValue::UInt(n)) => {
                        if let Ok(m) = usize::try_from(n) {
                            *value = Some(m);
                            self.stage = WsCompressionRecognizerStage::InBody;
                            None
                        } else {
                            Some(Err(ReadError::NumberOutOfRange))
                        }
                    }
                    ReadEvent::EndRecord => Some(Ok(WsCompression::None(*value))),
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::UInt64),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
                WsCompressionFields::Deflate(value) => match input {
                    ReadEvent::Number(NumericValue::UInt(n)) => {
                        if let Ok(m) = u32::try_from(n) {
                            *value = Some(m);
                            self.stage = WsCompressionRecognizerStage::InBody;
                            None
                        } else {
                            Some(Err(ReadError::NumberOutOfRange))
                        }
                    }
                    ReadEvent::EndRecord => match value {
                        None => Some(Ok(WsCompression::Deflate(DeflateConfig::default()))),
                        Some(value) => Some(Ok(WsCompression::Deflate(
                            DeflateConfig::with_compression_level(Compression::new(*value)),
                        ))),
                    },
                    ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                        ExpectedEvent::ValueEvent(ValueKind::UInt32),
                        ExpectedEvent::EndOfRecord,
                    ])))),
                },
            },
        }
    }

    fn reset(&mut self) {
        let WsCompressionRecognizer { stage, fields } = self;
        *stage = WsCompressionRecognizerStage::Init;
        *fields = WsCompressionFields::None(None);
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
            missing.push(Text::new("host"));
        }
        if node.is_none() {
            missing.push(Text::new("node"));
        }
        if lane.is_none() {
            missing.push(Text::new("lane"));
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
                    "host" => {
                        self.stage = AbsolutePathStage::Slot(AbsolutePathField::Host);
                        None
                    }
                    "node" => {
                        self.stage = AbsolutePathStage::Slot(AbsolutePathField::Node);
                        None
                    }
                    "lane" => {
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
