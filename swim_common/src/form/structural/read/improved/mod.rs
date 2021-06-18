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

pub mod primitive;
#[cfg(test)]
mod tests;

use crate::form::structural::generic::coproduct::{CCons, CNil, Unify};
use crate::form::structural::read::materializers::value::ValueMaterializer;
use crate::form::structural::read::parser::{NumericLiteral, ParseEvent};
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use crate::model::{Value, ValueKind};
use num_bigint::{BigInt, BigUint};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::option::Option::None;
use std::sync::Arc;
use utilities::iteratee::Iteratee;

pub trait RecognizerReadable: Sized {
    type Rec: Recognizer<Target = Self>;
    type AttrRec: Recognizer<Target = Self>;

    fn make_recognizer() -> Self::Rec;
    fn make_attr_recognizer() -> Self::AttrRec;

    fn on_absent() -> Option<Self> {
        None
    }

    fn is_simple() -> bool {
        false
    }
}

pub trait Recognizer {
    type Target;

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>>;

    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        None
    }

    fn reset(&mut self);

    fn as_iteratee(&mut self) -> RecognizerIteratee<'_, Self>
    where
        Self: Sized,
    {
        RecognizerIteratee(self)
    }
}

pub struct RecognizerIteratee<'b, R>(&'b mut R);

impl<'a, 'b, R> Iteratee<ParseEvent<'a>> for RecognizerIteratee<'b, R>
where
    R: Recognizer,
{
    type Item = Result<R::Target, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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

fn bad_kind(event: &ParseEvent<'_>) -> ReadError {
    match event {
        ParseEvent::Number(NumericLiteral::Int(_)) => ReadError::UnexpectedKind(ValueKind::Int64),
        ParseEvent::Number(NumericLiteral::UInt(_)) => ReadError::UnexpectedKind(ValueKind::UInt64),
        ParseEvent::Number(NumericLiteral::BigInt(_)) => {
            ReadError::UnexpectedKind(ValueKind::BigInt)
        }
        ParseEvent::Number(NumericLiteral::BigUint(_)) => {
            ReadError::UnexpectedKind(ValueKind::BigUint)
        }
        ParseEvent::Number(NumericLiteral::Float(_)) => {
            ReadError::UnexpectedKind(ValueKind::Float64)
        }
        ParseEvent::Boolean(_) => ReadError::UnexpectedKind(ValueKind::Boolean),
        ParseEvent::TextValue(_) => ReadError::UnexpectedKind(ValueKind::Text),
        ParseEvent::Extant => ReadError::UnexpectedKind(ValueKind::Extant),
        ParseEvent::Blob(_) => ReadError::UnexpectedKind(ValueKind::Data),
        ParseEvent::StartBody | ParseEvent::StartAttribute(_) => {
            ReadError::UnexpectedKind(ValueKind::Record)
        }
        _ => ReadError::InconsistentState,
    }
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
simple_readable!(f64, F64Recognizer);
simple_readable!(BigInt, BigIntRecognizer);
simple_readable!(BigUint, BigUintRecognizer);
simple_readable!(String, StringRecognizer);
simple_readable!(Text, TextRecognizer);
simple_readable!(Vec<u8>, DataRecognizer);
simple_readable!(bool, BoolRecognizer);

pub struct VecRecognizer<T, R> {
    is_attr_body: bool,
    stage: BodyStage,
    vector: Vec<T>,
    rec: R,
}

impl<T, R: Recognizer<Target = T>> Recognizer for VecRecognizer<T, R> {
    type Target = Vec<T>;

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            BodyStage::Init => {
                if matches!(&input, ParseEvent::StartBody) {
                    self.stage = BodyStage::Between;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            BodyStage::Between => match &input {
                ParseEvent::EndRecord if !self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.vector)))
                }
                ParseEvent::EndAttribute if self.is_attr_body => {
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

type Selector<Flds> = for<'a> fn(&mut Flds, u32, ParseEvent<'a>) -> Option<Result<(), ReadError>>;

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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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
                if matches!(input, ParseEvent::StartBody) {
                    *state = BodyFieldState::Between;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            BodyFieldState::Between => match input {
                ParseEvent::EndRecord if !*is_attr_body => Some(on_done(fields)),
                ParseEvent::EndAttribute if *is_attr_body => Some(on_done(fields)),
                ParseEvent::TextValue(name) => {
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
                if matches!(input, ParseEvent::Slot) {
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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
            if matches!(input, ParseEvent::StartBody) {
                *state = BodyStage::Between;
                None
            } else {
                Some(Err(bad_kind(&input)))
            }
        } else if matches!(state, BodyStage::Between)
            && ((!*is_attr_body && matches!(&input, ParseEvent::EndRecord))
                || (*is_attr_body && matches!(&input, ParseEvent::EndAttribute)))
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if self.after_content {
            if matches!(&input, ParseEvent::EndAttribute) {
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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

#[derive(Clone, Copy)]
pub enum LabelledFieldKey<'a> {
    HeaderBody,
    HeaderSlot(&'a str),
    Attr(&'a str),
    Item(&'a str),
}

#[derive(Clone, Copy)]
enum LabelledStructState {
    Init,
    HeaderInit,
    HeaderBodyItem,
    HeaderBetween,
    HeaderExpectingSlot,
    HeaderItem,
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
    HeaderBodyItem,
    HeaderBetween,
    HeaderExpectingSlot,
    HeaderItem,
    AttrBetween,
    AttrItem,
    BodyBetween,
    BodyItem,
}

#[derive(Clone, Copy)]
pub enum OrdinalFieldKey<'a> {
    HeaderBody,
    HeaderSlot(&'a str),
    Attr(&'a str),
    FirstItem,
}

pub struct LabelledStructRecognizer<T, Flds> {
    tag: &'static str,
    has_header_body: bool,
    state: LabelledStructState,
    fields: Flds,
    progress: Bitset,
    select_index: for<'a> fn(LabelledFieldKey<'a>) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

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
    pub fn new(
        tag: &'static str,
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: LabelledVTable<T, Flds>,
    ) -> Self {
        LabelledStructRecognizer {
            tag,
            has_header_body,
            state: LabelledStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }

    pub fn variant(
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: LabelledVTable<T, Flds>,
    ) -> Self {
        LabelledStructRecognizer {
            tag: "",
            has_header_body,
            state: LabelledStructState::HeaderInit,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }
}

impl<T, Flds> OrdinalStructRecognizer<T, Flds> {
    pub fn new(
        tag: &'static str,
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        OrdinalStructRecognizer {
            tag,
            has_header_body,
            state: OrdinalStructState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }

    pub fn variant(
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
    ) -> Self {
        OrdinalStructRecognizer {
            tag: "",
            has_header_body,
            state: OrdinalStructState::HeaderInit,
            fields,
            progress: Bitset::new(num_fields),
            select_index: vtable.select_index,
            index: 0,
            select_recog: vtable.select_recog,
            on_done: vtable.on_done,
            reset: vtable.reset,
        }
    }
}

impl<T, Flds> Recognizer for LabelledStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let LabelledStructRecognizer {
            tag,
            has_header_body,
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
            LabelledStructState::Init => match &input {
                ParseEvent::StartAttribute(name) if name == *tag => {
                    if *has_header_body {
                        *state = LabelledStructState::HeaderInit;
                    } else {
                        *state = LabelledStructState::HeaderBetween;
                    }
                    None
                }
                _ => Some(Err(bad_kind(&input))),
            },
            LabelledStructState::HeaderInit => {
                if matches!(&input, ParseEvent::EndAttribute) {
                    *state = LabelledStructState::AttrBetween;
                    None
                } else {
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
            LabelledStructState::HeaderBetween => match input {
                ParseEvent::EndAttribute => {
                    *state = LabelledStructState::AttrBetween;
                    None
                }
                ParseEvent::TextValue(name) => {
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
            LabelledStructState::HeaderExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = LabelledStructState::HeaderItem;
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
            LabelledStructState::AttrBetween => match input {
                ParseEvent::StartBody => {
                    *state = LabelledStructState::BodyBetween;
                    None
                }
                ParseEvent::StartAttribute(name) => {
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
                ParseEvent::EndRecord => Some(on_done(fields)),
                ParseEvent::TextValue(name) => {
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
                if matches!(&input, ParseEvent::Slot) {
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
        (self.reset)(&mut self.fields);
    }
}

pub struct OrdinalStructRecognizer<T, Flds> {
    tag: &'static str,
    has_header_body: bool,
    state: OrdinalStructState,
    fields: Flds,
    progress: Bitset,
    select_index: for<'a> fn(OrdinalFieldKey<'a>) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    on_done: fn(&mut Flds) -> Result<T, ReadError>,
    reset: fn(&mut Flds),
}

impl<T, Flds> Recognizer for OrdinalStructRecognizer<T, Flds> {
    type Target = T;

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let OrdinalStructRecognizer {
            tag,
            has_header_body,
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
            OrdinalStructState::Init => match &input {
                ParseEvent::StartAttribute(name) if name == *tag => {
                    if *has_header_body {
                        *state = OrdinalStructState::HeaderInit;
                    } else {
                        *state = OrdinalStructState::HeaderBetween;
                    }
                    None
                }
                _ => Some(Err(bad_kind(&input))),
            },
            OrdinalStructState::HeaderInit => {
                if matches!(&input, ParseEvent::EndAttribute) {
                    *state = OrdinalStructState::AttrBetween;
                    None
                } else {
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
            OrdinalStructState::HeaderBetween => match &input {
                ParseEvent::EndAttribute => {
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
                ParseEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = OrdinalStructState::HeaderExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                    }
                }
                _ => Some(Err(bad_kind(&input))),
            },
            OrdinalStructState::HeaderExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = OrdinalStructState::HeaderItem;
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
            OrdinalStructState::AttrBetween => match &input {
                ParseEvent::StartBody => {
                    *state = OrdinalStructState::BodyBetween;
                    if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                    }
                    None
                }
                ParseEvent::StartAttribute(name) => {
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
                ParseEvent::EndRecord => Some(on_done(fields)),
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
        (self.reset)(&mut self.fields);
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let OptionRecognizer {
            inner,
            reading_value,
        } = self;
        if *reading_value {
            inner.feed_event(input).map(|r| r.map(Option::Some))
        } else {
            match &input {
                ParseEvent::Extant => {
                    let r = inner.feed_event(ParseEvent::Extant);
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if self.seen_extant && matches!(&input, ParseEvent::EndAttribute) {
            Some(Ok(None))
        } else if !self.seen_extant && matches!(&input, ParseEvent::Extant) {
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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
    type AttrRec = ValueMaterializer; //TODO Not quite correct.

    fn make_recognizer() -> Self::Rec {
        ValueMaterializer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        ValueMaterializer::default()
    }
}

enum MapStage {
    Init,
    Between,
    Key,
    Slot,
    Value,
}

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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match self.stage {
            MapStage::Init => {
                if matches!(&input, ParseEvent::StartBody) {
                    self.stage = MapStage::Between;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            MapStage::Between => match &input {
                ParseEvent::EndRecord if !self.is_attr_body => {
                    Some(Ok(std::mem::take(&mut self.map)))
                }
                ParseEvent::EndAttribute if self.is_attr_body => {
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
                if matches!(&input, ParseEvent::Slot) {
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
    event: ParseEvent<'_>,
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
    HeaderBodyItem,
    HeaderBetween,
    HeaderExpectingSlot,
    HeaderItem,
    AttrBetween,
    AttrItem,
    DelegatedSimple,
    DelegatedComplex,
    Done,
}

pub struct DelegateStructRecognizer<T, Flds> {
    tag: &'static str,
    has_header_body: bool,
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
    pub fn new(
        tag: &'static str,
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
        simple_body: bool,
    ) -> Self {
        DelegateStructRecognizer {
            tag,
            has_header_body,
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

    pub fn variant(
        has_header_body: bool,
        fields: Flds,
        num_fields: u32,
        vtable: OrdinalVTable<T, Flds>,
        simple_body: bool,
    ) -> Self {
        DelegateStructRecognizer {
            tag: "",
            has_header_body,
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let DelegateStructRecognizer {
            tag,
            has_header_body,
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
            DelegateStructState::Init => match &input {
                ParseEvent::StartAttribute(name) if name == *tag => {
                    if *has_header_body {
                        *state = DelegateStructState::HeaderInit;
                    } else {
                        *state = DelegateStructState::HeaderBetween;
                    }
                    None
                }
                _ => Some(Err(bad_kind(&input))),
            },
            DelegateStructState::HeaderInit => {
                if matches!(&input, ParseEvent::EndAttribute) {
                    *state = DelegateStructState::AttrBetween;
                    None
                } else {
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
            DelegateStructState::HeaderBetween => match &input {
                ParseEvent::EndAttribute => {
                    *state = DelegateStructState::AttrBetween;
                    None
                }
                ParseEvent::TextValue(name) => {
                    if let Some(i) = select_index(OrdinalFieldKey::HeaderSlot(name.borrow())) {
                        if progress.get(i).unwrap_or(false) {
                            Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                        } else {
                            *index = i;
                            *state = DelegateStructState::HeaderExpectingSlot;
                            None
                        }
                    } else {
                        Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                    }
                }
                _ => Some(Err(bad_kind(&input))),
            },
            DelegateStructState::HeaderExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = DelegateStructState::HeaderItem;
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
            DelegateStructState::AttrBetween => match input {
                ParseEvent::StartBody => {
                    if let Some(i) = select_index(OrdinalFieldKey::FirstItem) {
                        *index = i;
                        if *simple_body {
                            *state = DelegateStructState::DelegatedSimple;
                            None
                        } else {
                            *state = DelegateStructState::DelegatedComplex;
                            if let Err(e) = select_recog(fields, *index, ParseEvent::StartBody)? {
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
                ParseEvent::StartAttribute(name) => {
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
                                select_recog(fields, *index, ParseEvent::StartAttribute(name))?
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
                ParseEvent::EndRecord => {
                    if let Err(e) = select_recog(fields, *index, ParseEvent::Extant)? {
                        Some(Err(e))
                    } else {
                        Some(on_done(fields))
                    }
                }
                ev @ ParseEvent::Slot
                | ev @ ParseEvent::StartBody
                | ev @ ParseEvent::StartAttribute(_)
                | ev @ ParseEvent::EndAttribute => Some(Err(bad_kind(&ev))),
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
                if matches!(&input, ParseEvent::EndRecord) {
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

    fn feed_event(&mut self, _input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
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

pub struct TaggedEnumRecognizer<Var> {
    select_var: fn(&str) -> Option<Var>,
    variant: Option<Var>,
}

impl<Var> TaggedEnumRecognizer<Var> {
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

    fn feed_event(&mut self, input: ParseEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let TaggedEnumRecognizer {
            select_var,
            variant,
        } = self;
        match variant {
            None => match input {
                ParseEvent::StartAttribute(name) => {
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
