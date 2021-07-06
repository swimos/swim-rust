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
use crate::form::structural::read::event::ReadEvent;
use crate::form::structural::read::materializers::value::{
    AttrBodyMaterializer, ValueMaterializer,
};
use crate::form::structural::read::recognizer::primitive::DataRecognizer;
use crate::form::structural::read::ReadError;
use crate::form::structural::Tag;
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Value, ValueKind};
use num_bigint::{BigInt, BigUint};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::option::Option::None;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;
use utilities::iteratee::Iteratee;
use utilities::uri::RelativeUri;

/// Trait for types that can be recognized by a [`Recognizer`] state machine.
pub trait RecognizerReadable: Sized {
    type Rec: Recognizer<Target = Self>;
    type AttrRec: Recognizer<Target = Self>;

    /// Create a state machine that will recognize the represenation of the type as a stream of
    /// [`ReadEvent`]s.
    fn make_recognizer() -> Self::Rec;

    /// Create a state machine that will recognize the represenation of the type as a stream of
    /// [`ReadEvent`]s from the body of an attribute. (In most cases this will be indentical
    /// to the `make_recognizer` state machine, followed by an end attribute event. However,
    /// for types that can be reprsented as records with no attributes, this may be more complex
    /// as it is permissible for the record to be collapsed into the body of the attribute.)
    fn make_attr_recognizer() -> Self::AttrRec;

    /// If this value is expected as teh value of a record field but was not present a default
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

fn bad_kind(event: &ReadEvent<'_>) -> ReadError {
    event.kind_error()
}

macro_rules! simple_readable {
    ($target:ty, $recog:ident) => {
        impl RecognizerReadable for $target {
            type Rec = primitive::$recog;
            type AttrRec = SimpleAttrBody<primitive::$recog>;

            fn make_recognizer() -> Self::Rec {
                primitive::$recog
            }

            fn make_attr_recognizer() -> Self::AttrRec {
                SimpleAttrBody::new(primitive::$recog)
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

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(DataRecognizer, Blob::from_vec)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }
}

type BoxU8Vec = fn(Vec<u8>) -> Box<[u8]>;

impl RecognizerReadable for Box<[u8]> {
    type Rec = MappedRecognizer<DataRecognizer, BoxU8Vec>;
    type AttrRec = SimpleAttrBody<Self::Rec>;

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(DataRecognizer, Vec::into_boxed_slice)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }
}

impl RecognizerReadable for RelativeUri {
    type Rec = RelativeUriRecognizer;
    type AttrRec = SimpleAttrBody<RelativeUriRecognizer>;

    fn make_recognizer() -> Self::Rec {
        RelativeUriRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(RelativeUriRecognizer)
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
            ow => Some(Err(bad_kind(&ow))),
        }
    }

    fn reset(&mut self) {}
}

impl RecognizerReadable for Url {
    type Rec = UrlRecognizer;
    type AttrRec = SimpleAttrBody<UrlRecognizer>;

    fn make_recognizer() -> Self::Rec {
        UrlRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(UrlRecognizer)
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
            ow => Some(Err(bad_kind(&ow))),
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
                    Some(Err(bad_kind(&input)))
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
        VecRecognizer {
            is_attr_body,
            stage: BodyStage::Init,
            vector: vec![],
            rec,
        }
    }
}

impl<T: RecognizerReadable> RecognizerReadable for Vec<T> {
    type Rec = VecRecognizer<T, T::Rec>;
    type AttrRec = VecRecognizer<T, T::Rec>;

    fn make_recognizer() -> Self::Rec {
        VecRecognizer::new(false, T::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        VecRecognizer::new(true, T::make_recognizer())
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
    /// * `on_done` - Called when the recognizer is complete to conver the state to the output type.
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
    /// * `on_done` - Called when the recognizer is complete to conver the state to the output type.
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
                    Some(Err(bad_kind(&input)))
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
                ow => Some(Err(bad_kind(&ow))),
            },
            BodyFieldState::ExpectingSlot => {
                if matches!(input, ReadEvent::Slot) {
                    *state = BodyFieldState::SlotValue;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedKind(ValueKind::Text)))
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
    /// * `on_done` - Called when the recognizer is complete to conver the state to the output type.
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
    /// * `on_done` - Called when the recognizer is complete to conver the state to the output type.
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
                Some(Err(bad_kind(&input)))
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
                Some(Err(bad_kind(&input)))
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

/// Runs two [`Recognizer`]s in paralell returning the result of the first that completes
/// successfully. If bothe complete with an error, the errof from the recognizer that failed
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
            let r = recognizer1.feed_event(input.clone())?;
            *first_active = false;
            Some(r)
        } else if *second_active {
            let r = recognizer2.feed_event(input.clone())?;
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
    /// The first value inside the tag attribute.
    HeaderBody,
    /// A labelled slot in the tag attribute.
    HeaderSlot(&'a str),
    /// Another attribute after the tag.
    Attr(&'a str),
    /// A labelled slot in the body of the record.
    Item(&'a str),
}

#[derive(Clone, Copy)]
enum LabelledStructState {
    Init,
    HeaderInit,
    HeaderInitNested,
    HeaderBodyItem,
    HeaderBodyItemNested,
    HeaderBetween,
    HeaderBetweenNested,
    HeaderExpectingSlot,
    HeaderExpectingSlotNested,
    HeaderItem,
    HeaderItemNested,
    HeaderEnd,
    AttrBetween,
    AttrItem,
    BodyBetween,
    BodyExpectingSlot,
    BodyItem,
}

#[derive(Clone, Copy)]
enum OrdinalStructState {
    Init,
    HeaderInit,
    HeaderInitNested,
    HeaderBodyItem,
    HeaderBodyItemNested,
    HeaderBetween,
    HeaderBetweenNested,
    HeaderExpectingSlot,
    HeaderExpectingSlotNested,
    HeaderItem,
    HeaderItemNested,
    HeaderEnd,
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
    /// The first value inside the tag attribute.
    HeaderBody,
    /// A labelled slot in the tag attribute.
    HeaderSlot(&'a str),
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HeaderKind {
    BodyOnly,
    SlotsOnly,
    Both,
}

impl HeaderKind {

    fn has_body(&self) -> bool {
        matches!(self, HeaderKind::BodyOnly | HeaderKind::Both)
    }

}


/// This type is used to encode the standard encoding of Rust structs, with labelled fields, as
/// generated by the derivation macro for [`RecognizerReadable`]. It should not generally be
/// necessary to use this type explicitly.
pub struct LabelledStructRecognizer<T, Flds> {
    tag: TagSpec,
    header_kind: HeaderKind,
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
    /// * `header_kind` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute and if there are slots in the header.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn new(
        tag: TagSpec,
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: LabelledVTable<T, Flds>,
    ) -> Self {
        LabelledStructRecognizer {
            tag,
            header_kind,
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
    /// * `has_header_body` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn variant(
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: LabelledVTable<T, Flds>,
    ) -> Self {
        LabelledStructRecognizer {
            tag: TagSpec::Fixed(""),
            header_kind,
            state: LabelledStructState::HeaderInit,
            fields,
            progress: Bitset::new(num_fields),
            index: 0,
            vtable,
        }
    }
}

impl<T, Flds> OrdinalStructRecognizer<T, Flds> {
    /// #Arguments
    /// * `tag` - The expected name of the first attribute or an inidcation that it should be used
    /// to populate a field.
    /// * `header_kind` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute and if there are slots in the header.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn new(
        tag: TagSpec,
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        OrdinalStructRecognizer {
            tag,
            header_kind,
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
    /// * `header_kind` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute and if there are slots in the header.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    pub fn variant(
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        OrdinalStructRecognizer {
            tag: TagSpec::Fixed(""),
            header_kind,
            state: OrdinalStructState::HeaderInit,
            fields,
            progress: Bitset::new(num_fields),
            index: 0,
            vtable,
        }
    }
}

impl<T, Flds> Recognizer for LabelledStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let LabelledStructRecognizer {
            tag,
            header_kind,
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
                            *state = LabelledStructState::HeaderInit;
                            None
                        } else {
                            Some(Err(bad_kind(&ReadEvent::StartAttribute(name))))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(LabelledFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                *state = LabelledStructState::HeaderInit;
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => Some(Err(bad_kind(&ow))),
            },
            LabelledStructState::HeaderInit => {
                match &input {
                    ReadEvent::EndAttribute => {
                        *state = LabelledStructState::AttrBetween;
                        None
                    }
                    ReadEvent::StartBody if *header_kind != HeaderKind::BodyOnly => {
                        *state = LabelledStructState::HeaderInitNested;
                        None
                    }
                    _ => {
                        if header_kind.has_body() {
                            *state = LabelledStructState::HeaderBodyItem;
                            if let Some(i) = select_index(LabelledFieldKey::HeaderBody) {
                                *index = i;
                            } else {
                                return Some(Err(ReadError::InconsistentState));
                            }
                            if let Err(e) = select_recog(fields, *index, input)? {
                                Some(Err(e))
                            } else {
                                *state = LabelledStructState::HeaderBetween;
                                None
                            }
                        } else {
                            if let ReadEvent::TextValue(name) = input {
                                if let Some(i) = select_index(LabelledFieldKey::HeaderSlot(name.borrow())) {
                                    if progress.get(i).unwrap_or(false) {
                                        Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                    } else {
                                        *index = i;
                                        *state = LabelledStructState::HeaderExpectingSlot;
                                        None
                                    }
                                } else {
                                    Some(Err(ReadError::UnexpectedField(name.into())))
                                }
                            } else {
                                Some(Err(bad_kind(&input)))
                            }
                        }
                    }
                }
            }
            LabelledStructState::HeaderInitNested => {
                if matches!(&input, ReadEvent::EndRecord) {
                    *state = LabelledStructState::HeaderEnd;
                    None
                } else {
                    if header_kind.has_body() {
                        *state = LabelledStructState::HeaderBodyItemNested;
                        if let Some(i) = select_index(LabelledFieldKey::HeaderBody) {
                            *index = i;
                        } else {
                            return Some(Err(ReadError::InconsistentState));
                        }
                        if let Err(e) = select_recog(fields, *index, input)? {
                            Some(Err(e))
                        } else {
                            *state = LabelledStructState::HeaderBetween;
                            None
                        }
                    } else {
                        if let ReadEvent::TextValue(name) = input {
                            if let Some(i) = select_index(LabelledFieldKey::HeaderSlot(name.borrow())) {
                                if progress.get(i).unwrap_or(false) {
                                    Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                } else {
                                    *index = i;
                                    *state = LabelledStructState::HeaderExpectingSlotNested;
                                    None
                                }
                            } else {
                                Some(Err(ReadError::UnexpectedField(name.into())))
                            }
                        } else {
                            Some(Err(bad_kind(&input)))
                        }
                    }
                }
            }
            LabelledStructState::HeaderBodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetween;
                    None
                }
            }
            LabelledStructState::HeaderBodyItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetweenNested;
                    None
                }
            }
            LabelledStructState::HeaderBetween => match input {
                ReadEvent::EndAttribute => {
                    *state = LabelledStructState::AttrBetween;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(LabelledFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = LabelledStructState::HeaderExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            LabelledStructState::HeaderBetweenNested => match input {
                ReadEvent::EndRecord => {
                    *state = LabelledStructState::HeaderEnd;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(LabelledFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = LabelledStructState::HeaderExpectingSlotNested;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            LabelledStructState::HeaderExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = LabelledStructState::HeaderItem;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            LabelledStructState::HeaderExpectingSlotNested => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = LabelledStructState::HeaderItemNested;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            LabelledStructState::HeaderItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetween;
                    None
                }
            }
            LabelledStructState::HeaderItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetweenNested;
                    None
                }
            }
            LabelledStructState::HeaderEnd => {
                if matches!(&input, ReadEvent::EndAttribute) {
                    *state = LabelledStructState::AttrBetween;
                    None
                } else {
                    Some(Err(input.kind_error()))
                }
            }
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
                ow => Some(Err(bad_kind(&ow))),
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
                ow => Some(Err(bad_kind(&ow))),
            },
            LabelledStructState::BodyExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = LabelledStructState::BodyItem;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
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
    header_kind: HeaderKind,
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
            header_kind,
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
                            *state = OrdinalStructState::HeaderInit;
                            None
                        } else {
                            Some(Err(bad_kind(&ReadEvent::StartAttribute(name))))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(OrdinalFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                *state = OrdinalStructState::HeaderInit;
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => Some(Err(bad_kind(&ow))),
            },
            OrdinalStructState::HeaderInit => {
                match &input {
                    ReadEvent::EndAttribute => {
                        *state = OrdinalStructState::AttrBetween;
                        None
                    }
                    ReadEvent::StartBody if *header_kind != HeaderKind::BodyOnly => {
                        *state = OrdinalStructState::HeaderInitNested;
                        None
                    }
                    _ => {
                        if header_kind.has_body() {
                            *state = OrdinalStructState::HeaderBodyItem;
                            if let Some(i) = select_index(OrdinalFieldKey::HeaderBody) {
                                *index = i;
                            } else {
                                return Some(Err(ReadError::InconsistentState));
                            }
                            if let Err(e) = select_recog(fields, *index, input)? {
                                Some(Err(e))
                            } else {
                                *state = OrdinalStructState::HeaderBetween;
                                None
                            }
                        } else {
                            if let ReadEvent::TextValue(name) = input {
                                if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                                    if progress.get(i).unwrap_or(false) {
                                        Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                    } else {
                                        *index = i;
                                        *state = OrdinalStructState::HeaderExpectingSlot;
                                        None
                                    }
                                } else {
                                    Some(Err(ReadError::UnexpectedField(name.into())))
                                }
                            } else {
                                Some(Err(bad_kind(&input)))
                            }
                        }
                    }
                }
            }
            OrdinalStructState::HeaderInitNested => {
                if matches!(&input, ReadEvent::EndRecord) {
                    *state = OrdinalStructState::HeaderEnd;
                    None
                } else {
                    if header_kind.has_body() {
                        *state = OrdinalStructState::HeaderBodyItemNested;
                        if let Some(i) = select_index(OrdinalFieldKey::HeaderBody) {
                            *index = i;
                        } else {
                            return Some(Err(ReadError::InconsistentState));
                        }
                        if let Err(e) = select_recog(fields, *index, input)? {
                            Some(Err(e))
                        } else {
                            *state = OrdinalStructState::HeaderBetween;
                            None
                        }
                    } else {
                        if let ReadEvent::TextValue(name) = input {
                            if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                                if progress.get(i).unwrap_or(false) {
                                    Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                } else {
                                    *index = i;
                                    *state = OrdinalStructState::HeaderExpectingSlotNested;
                                    None
                                }
                            } else {
                                Some(Err(ReadError::UnexpectedField(name.into())))
                            }
                        } else {
                            Some(Err(bad_kind(&input)))
                        }
                    }
                }
            }
            OrdinalStructState::HeaderBodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetween;
                    None
                }
            }
            OrdinalStructState::HeaderBodyItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetweenNested;
                    None
                }
            }
            OrdinalStructState::HeaderBetween => match input {
                ReadEvent::EndAttribute => {
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = OrdinalStructState::HeaderExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            OrdinalStructState::HeaderBetweenNested => match input {
                ReadEvent::EndRecord => {
                    *state = OrdinalStructState::HeaderEnd;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = OrdinalStructState::HeaderExpectingSlotNested;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            OrdinalStructState::HeaderExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = OrdinalStructState::HeaderItem;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            OrdinalStructState::HeaderExpectingSlotNested => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = OrdinalStructState::HeaderItemNested;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            OrdinalStructState::HeaderItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetween;
                    None
                }
            }
            OrdinalStructState::HeaderItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetweenNested;
                    None
                }
            }
            OrdinalStructState::HeaderEnd => {
                if matches!(&input, ReadEvent::EndAttribute) {
                    *state = OrdinalStructState::AttrBetween;
                    None
                } else {
                    Some(Err(input.kind_error()))
                }
            }
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
                _ => Some(Err(bad_kind(&input))),
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

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(T::make_recognizer(), Arc::new)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        MappedRecognizer::new(T::make_attr_recognizer(), Arc::new)
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
        if self.seen_extant && matches!(&input, ReadEvent::EndAttribute) {
            Some(Ok(None))
        } else if !self.seen_extant && matches!(&input, ReadEvent::Extant) {
            self.seen_extant = true;
            None
        } else {
            Some(Err(bad_kind(&input)))
        }
    }

    fn reset(&mut self) {
        self.seen_extant = false;
    }
}

/// Runs another recognizer and then transforms its result by applying a function to it.
pub struct MappedRecognizer<R, F> {
    inner: R,
    f: F,
}

impl<R, F> MappedRecognizer<R, F> {
    fn new(inner: R, f: F) -> Self {
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

    fn make_recognizer() -> Self::Rec {
        OptionRecognizer::new(T::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        FirstOf::new(
            EmptyAttrRecognizer::default(),
            MappedRecognizer::new(T::make_attr_recognizer(), Option::Some),
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

    fn make_recognizer() -> Self::Rec {
        ValueMaterializer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        AttrBodyMaterializer::default()
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

    fn make_recognizer() -> Self::Rec {
        HashMapRecognizer::new(K::make_recognizer(), V::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        HashMapRecognizer::new_attr(K::make_recognizer(), V::make_recognizer())
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
                    Some(Err(bad_kind(&input)))
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
                    Some(Err(bad_kind(&input)))
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
    HeaderInit,
    HeaderInitNested,
    HeaderBodyItem,
    HeaderBodyItemNested,
    HeaderBetween,
    HeaderBetweenNested,
    HeaderExpectingSlot,
    HeaderExpectingSlotNested,
    HeaderItem,
    HeaderItemNested,
    HeaderEnd,
    AttrBetween,
    AttrItem,
    DelegatedSimple,
    DelegatedComplex,
    Done,
}

/// This type is used to encode the standard encoding of Rust structs, where the body of the record
/// is determined by one of the fields, generated by the derivation macro for
/// [`RecognizerReadable`]. It should not generally be necessary to use this type explicitly.
pub struct DelegateStructRecognizer<T, Flds> {
    tag: TagSpec,
    header_kind: HeaderKind,
    state: DelegateStructState,
    fields: Flds,
    progress: Bitset,
    select_index: for<'a> fn(OrdinalFieldKey<'a>) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
    simple_body: bool,
}

impl<T, Flds> DelegateStructRecognizer<T, Flds> {
    /// #Arguments
    /// * `tag` - The expected name of the first attribute or an inidcation that it should be used
    /// to populate a field.
    /// * `header_kind` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute and whether there are slots in the header.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    /// * `simple_body` - Indicates that the delegate field is represented by a singe value which
    /// will need to be wrapped in a record.
    pub fn new(
        tag: TagSpec,
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
        simple_body: bool,
    ) -> Self {
        DelegateStructRecognizer {
            tag,
            header_kind,
            state: DelegateStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
            simple_body,
        }
    }

    /// This constructor is used for enumeration variants. The primary different is that the tag
    /// of the record has already been read before this recognizers is called so the initial state
    /// is skipped.
    ///
    /// #Arguments
    /// * `header_kind` - Inidcates that one of the fields has been promoted to the body of the
    /// tag attribute and whether there are slots in the header.
    /// * `fields` - The state of the recognizer state machine (specified by the macro).
    /// * `num_fields` - The total numer of (non-skipped) fields that the recognizer expects.
    /// * `vtable` - Functions that are generated by the macro that determine how incoming events
    /// modify the state.
    /// * `simple_body` - Indicates that the delegate field is represented by a singe value which
    /// will need to be wrapped in a record.
    pub fn variant(
        header_kind: HeaderKind,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
        simple_body: bool,
    ) -> Self {
        DelegateStructRecognizer {
            tag: TagSpec::Fixed(""),
            header_kind,
            state: DelegateStructState::HeaderInit,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
            simple_body,
        }
    }
}

impl<T, Flds> Recognizer for DelegateStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let DelegateStructRecognizer {
            tag,
            header_kind,
            state,
            fields,
            progress,
            select_index,
            index,
            select_recog,
            on_done,
            simple_body,
            ..
        } = self;

        match state {
            DelegateStructState::Init => match input {
                ReadEvent::StartAttribute(name) => match tag {
                    TagSpec::Fixed(tag) => {
                        if name == *tag {
                            *state = DelegateStructState::HeaderInit;
                            None
                        } else {
                            Some(Err(bad_kind(&ReadEvent::StartAttribute(name))))
                        }
                    }
                    TagSpec::Field => {
                        if let Some(i) = select_index(OrdinalFieldKey::Tag) {
                            if let Err(e) = select_recog(fields, i, ReadEvent::TextValue(name))? {
                                Some(Err(e))
                            } else {
                                *state = DelegateStructState::HeaderInit;
                                None
                            }
                        } else {
                            Some(Err(ReadError::InconsistentState))
                        }
                    }
                },
                ow => Some(Err(bad_kind(&ow))),
            },
            DelegateStructState::HeaderInit => {
                match &input {
                    ReadEvent::EndAttribute => {
                        *state = DelegateStructState::AttrBetween;
                        None
                    }
                    ReadEvent::StartBody if *header_kind != HeaderKind::BodyOnly => {
                        *state = DelegateStructState::HeaderInitNested;
                        None
                    }
                    _ => {
                        if header_kind.has_body() {
                            *state = DelegateStructState::HeaderBodyItem;
                            if let Some(i) = select_index(OrdinalFieldKey::HeaderBody) {
                                *index = i;
                            } else {
                                return Some(Err(ReadError::InconsistentState));
                            }
                            if let Err(e) = select_recog(fields, *index, input)? {
                                Some(Err(e))
                            } else {
                                *state = DelegateStructState::HeaderBetween;
                                None
                            }
                        } else {
                            if let ReadEvent::TextValue(name) = input {
                                if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                                    if progress.get(i).unwrap_or(false) {
                                        Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                    } else {
                                        *index = i;
                                        *state = DelegateStructState::HeaderExpectingSlot;
                                        None
                                    }
                                } else {
                                    Some(Err(ReadError::UnexpectedField(name.into())))
                                }
                            } else {
                                Some(Err(bad_kind(&input)))
                            }
                        }
                    }
                }
            }
            DelegateStructState::HeaderInitNested => {
                if matches!(&input, ReadEvent::EndRecord) {
                    *state = DelegateStructState::HeaderEnd;
                    None
                } else {
                    if header_kind.has_body() {
                        *state = DelegateStructState::HeaderBodyItemNested;
                        if let Some(i) = select_index(OrdinalFieldKey::HeaderBody) {
                            *index = i;
                        } else {
                            return Some(Err(ReadError::InconsistentState));
                        }
                        if let Err(e) = select_recog(fields, *index, input)? {
                            Some(Err(e))
                        } else {
                            *state = DelegateStructState::HeaderBetween;
                            None
                        }
                    } else {
                        if let ReadEvent::TextValue(name) = input {
                            if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                                if progress.get(i).unwrap_or(false) {
                                    Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                                } else {
                                    *index = i;
                                    *state = DelegateStructState::HeaderExpectingSlotNested;
                                    None
                                }
                            } else {
                                Some(Err(ReadError::UnexpectedField(name.into())))
                            }
                        } else {
                            Some(Err(bad_kind(&input)))
                        }
                    }
                }
            }
            DelegateStructState::HeaderBodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = DelegateStructState::HeaderBetween;
                    None
                }
            }
            DelegateStructState::HeaderBodyItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = DelegateStructState::HeaderBetweenNested;
                    None
                }
            }
            DelegateStructState::HeaderBetween => match input {
                ReadEvent::EndAttribute => {
                    *state = DelegateStructState::AttrBetween;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = DelegateStructState::HeaderExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            DelegateStructState::HeaderBetweenNested => match input {
                ReadEvent::EndRecord => {
                    *state = DelegateStructState::HeaderEnd;
                    None
                }
                ReadEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = DelegateStructState::HeaderExpectingSlotNested;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
            },
            DelegateStructState::HeaderExpectingSlot => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = DelegateStructState::HeaderItem;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            DelegateStructState::HeaderExpectingSlotNested => {
                if matches!(&input, ReadEvent::Slot) {
                    *state = DelegateStructState::HeaderItemNested;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            DelegateStructState::HeaderItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = DelegateStructState::HeaderBetween;
                    None
                }
            }
            DelegateStructState::HeaderItemNested => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = DelegateStructState::HeaderBetweenNested;
                    None
                }
            }
            DelegateStructState::HeaderEnd => {
                if matches!(&input, ReadEvent::EndAttribute) {
                    *state = DelegateStructState::AttrBetween;
                    None
                } else {
                    Some(Err(input.kind_error()))
                }
            }
            DelegateStructState::AttrBetween => match input {
                ReadEvent::StartBody => {
                    if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                        if *simple_body {
                            *state = DelegateStructState::DelegatedSimple;
                            None
                        } else {
                            *state = DelegateStructState::DelegatedComplex;
                            if let Err(e) = select_recog(fields, *index, ReadEvent::StartBody)? {
                                Some(Err(e))
                            } else {
                                *state = DelegateStructState::Done;
                                None
                            }
                        }
                    } else {
                        *state = DelegateStructState::Done;
                        None
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
                        if *simple_body {
                            Some(Err(ReadError::UnexpectedField(name.into())))
                        } else {
                            *state = DelegateStructState::DelegatedComplex;
                            if let Err(e) =
                                select_recog(fields, *index, ReadEvent::StartAttribute(name))?
                            {
                                Some(Err(e))
                            } else {
                                *state = DelegateStructState::Done;
                                None
                            }
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(name.into())))
                    }
                }
                ow => Some(Err(bad_kind(&ow))),
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
            DelegateStructState::DelegatedSimple => match input {
                ReadEvent::EndRecord => {
                    if let Err(e) = select_recog(fields, *index, ReadEvent::Extant)? {
                        Some(Err(e))
                    } else {
                        Some(on_done(fields))
                    }
                }
                ev @ ReadEvent::Slot
                | ev @ ReadEvent::StartBody
                | ev @ ReadEvent::StartAttribute(_)
                | ev @ ReadEvent::EndAttribute => Some(Err(bad_kind(&ev))),
                ow => {
                    if let Err(e) = select_recog(fields, *index, ow)? {
                        Some(Err(e))
                    } else {
                        *state = DelegateStructState::Done;
                        None
                    }
                }
            },
            DelegateStructState::DelegatedComplex => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    Some(on_done(fields))
                }
            }
            DelegateStructState::Done => {
                if matches!(&input, ReadEvent::EndRecord) {
                    Some(on_done(fields))
                } else {
                    Some(Err(bad_kind(&input)))
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
                ow => Some(Err(bad_kind(&ow))),
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
        match (input, *state) {
            (ReadEvent::StartAttribute(name), UnitStructState::Init) => {
                if name == *tag {
                    *state = UnitStructState::Tag;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedAttribute(name.into())))
                }
            }
            (ReadEvent::Extant, UnitStructState::Tag) => {
                *state = UnitStructState::SeenExtant;
                None
            }
            (ReadEvent::EndAttribute, UnitStructState::Tag) => {
                *state = UnitStructState::AfterTag;
                None
            }
            (ReadEvent::EndAttribute, UnitStructState::SeenExtant) => {
                *state = UnitStructState::AfterTag;
                None
            }
            (ReadEvent::StartBody, UnitStructState::AfterTag) => {
                *state = UnitStructState::Body;
                None
            }
            (ReadEvent::EndRecord, UnitStructState::Body) => Some(Ok(on_done())),
            (ow, _) => Some(Err(bad_kind(&ow))),
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

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        Some(Err(bad_kind(&input)))
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
            T::try_from_str(txt.borrow()).map_err(|message| ReadError::Malformatted {
                text: txt.into(),
                message,
            })
        } else {
            Err(bad_kind(&input))
        };
        Some(result)
    }

    fn reset(&mut self) {}
}
