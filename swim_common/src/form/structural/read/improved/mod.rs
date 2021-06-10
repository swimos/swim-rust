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

use crate::form::structural::read::parser::{ParseEvent, NumericLiteral};
use utilities::iteratee::Iteratee;
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use crate::model::{ValueKind, Value};
use std::borrow::Borrow;
use std::marker::PhantomData;
use num_bigint::{BigInt, BigUint};
use std::sync::Arc;
use std::option::Option::None;
use crate::form::structural::read::materializers::value::ValueMaterializer;
use std::collections::HashMap;
use std::hash::Hash;

pub trait RecognizerReadable: Sized {
    type Rec: Recognizer<Self>;
    type AttrRec: Recognizer<Self>;

    fn make_recognizer() -> Self::Rec;
    fn make_attr_recognizer() -> Self::AttrRec;

    fn on_absent() -> Option<Self> {
        None
    }
}

pub trait Recognizer<R>: for<'a> Iteratee<ParseEvent<'a>, Item = Result<R, ReadError>> {

    fn reset(&mut self);

}

fn bad_kind(event: &ParseEvent<'_>) -> ReadError {
    match event {
        ParseEvent::Number(NumericLiteral::Int(_)) => ReadError::UnexpectedKind(ValueKind::Int64),
        ParseEvent::Number(NumericLiteral::UInt(_)) => ReadError::UnexpectedKind(ValueKind::UInt64),
        ParseEvent::Number(NumericLiteral::BigInt(_)) => ReadError::UnexpectedKind(ValueKind::BigInt),
        ParseEvent::Number(NumericLiteral::BigUint(_)) => ReadError::UnexpectedKind(ValueKind::BigUint),
        ParseEvent::Number(NumericLiteral::Float(_)) => ReadError::UnexpectedKind(ValueKind::Float64),
        ParseEvent::Boolean(_) => ReadError::UnexpectedKind(ValueKind::Boolean),
        ParseEvent::TextValue(_) => ReadError::UnexpectedKind(ValueKind::Text),
        ParseEvent::Extant => ReadError::UnexpectedKind(ValueKind::Extant),
        ParseEvent::Blob(_) => ReadError::UnexpectedKind(ValueKind::Data),
        ParseEvent::StartBody| ParseEvent::StartAttribute(_) => ReadError::UnexpectedKind(ValueKind::Record),
        _ => {
            ReadError::InconsistentState
        },
    }
}

macro_rules! simple_readable {
    ($target:ty, $recog:ident) => {
        impl RecognizerReadable for $target {
            type Rec = primitive::$recog;
            type AttrRec = SimpleAttrBody<$target, primitive::$recog>;

            fn make_recognizer() -> Self::Rec {
                primitive::$recog
            }

            fn make_attr_recognizer() -> Self::AttrRec {
                SimpleAttrBody::new(primitive::$recog)
            }

        }
    }
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

impl<'a, T, R: Recognizer<T>> Iteratee<ParseEvent<'a>> for VecRecognizer<T, R> {
    type Item = Result<Vec<T>, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match self.stage {
            BodyStage::Init => {
                if matches!(&input, ParseEvent::StartBody) {
                    self.stage = BodyStage::Between;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            BodyStage::Between => {
                match &input {
                    ParseEvent::EndRecord if !self.is_attr_body => {
                        Some(Ok(std::mem::take(&mut self.vector)))
                    }
                    ParseEvent::EndAttribute if self.is_attr_body => {
                        Some(Ok(std::mem::take(&mut self.vector)))
                    }
                    _ => {
                        self.stage = BodyStage::Item;
                        match self.rec.feed(input)? {
                            Ok(t) => {
                                self.vector.push(t);
                                self.rec.reset();
                                self.stage = BodyStage::Between;
                                None
                            }
                            Err(e) => {
                                Some(Err(e))
                            }
                        }
                    }
                }
            }
            BodyStage::Item => {
                match self.rec.feed(input)? {
                    Ok(t) => {
                        self.vector.push(t);
                        self.rec.reset();
                        self.stage = BodyStage::Between;
                        None
                    }
                    Err(e) => {
                        Some(Err(e))
                    }
                }
            }
        }
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

impl<T, R: Recognizer<T>> Recognizer<Vec<T>> for VecRecognizer<T, R> {
    fn reset(&mut self) {
        self.stage = BodyStage::Init;
        self.vector.clear();
        self.rec.reset();
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

    pub fn new(fields: Flds, select_index: fn(&str) -> Option<u32>,
               num_fields: u32,
               select_recog: Selector<Flds>,
               on_done: fn(&mut Flds) -> Result<T, ReadError>,
               reset: fn(&mut Flds)) -> Self {
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

    pub fn new_attr(fields: Flds, select_index: fn(&str) -> Option<u32>,
                    num_fields: u32,
                    select_recog: Selector<Flds>,
                    on_done: fn(&mut Flds) -> Result<T, ReadError>,
                    reset: fn(&mut Flds)) -> Self {
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

impl<'a, T, Flds> Iteratee<ParseEvent<'a>> for NamedFieldsRecognizer<T, Flds> {
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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
            BodyFieldState::Between => {
                match input {
                    ParseEvent::EndRecord if !*is_attr_body => {
                        Some(on_done(fields))
                    }
                    ParseEvent::EndAttribute if *is_attr_body => {
                        Some(on_done(fields))
                    }
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
                    ow => {
                        Some(Err(bad_kind(&ow)))
                    }
                }
            }
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
}

impl<T, Flds> Recognizer<T> for NamedFieldsRecognizer<T, Flds> {
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

    pub fn new(fields: Flds,
               num_fields: u32,
               select_recog: Selector<Flds>,
               on_done: fn(&mut Flds) -> Result<T, ReadError>,
               reset: fn(&mut Flds)) -> Self {
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

    pub fn new_attr(fields: Flds,
                    num_fields: u32,
                    select_recog: Selector<Flds>,
                    on_done: fn(&mut Flds) -> Result<T, ReadError>,
                    reset: fn(&mut Flds)) -> Self {
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


impl<'a, T, Flds> Iteratee<ParseEvent<'a>> for OrdinalFieldsRecognizer<T, Flds> {
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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
        } else if matches!(state, BodyStage::Between) && ((!*is_attr_body && matches!(&input, ParseEvent::EndRecord)) ||
            (*is_attr_body && matches!(&input, ParseEvent::EndAttribute))) {
            Some(on_done(fields))
        } else {
            if *index == *num_fields {
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

    }
}

impl<T, Flds> Recognizer<T> for OrdinalFieldsRecognizer<T, Flds> {
    fn reset(&mut self) {
        self.index = 0;
        self.state = BodyStage::Init;
        (self.reset)(&mut self.fields)
    }
}

pub struct SimpleAttrBody<T, R: Recognizer<T>> {
    after_content: bool,
    value: Option<T>,
    delegate: R,
}

impl<T, R: Recognizer<T>> SimpleAttrBody<T, R> {

    pub fn new(rec: R) -> Self {
        SimpleAttrBody {
            after_content: false,
            value: None,
            delegate: rec,
        }
    }

}

impl<'a, T, R: Recognizer<T>> Iteratee<ParseEvent<'a>> for SimpleAttrBody<T, R> {
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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
            let r = self.delegate.feed(input)?;
            self.after_content = true;
            match r {
                Ok(t) => {
                    self.value = Some(t);
                    None
                }
                Err(e) => {
                    Some(Err(e))
                }
            }
        }
    }
}


impl<T, R: Recognizer<T>> Recognizer<T> for SimpleAttrBody<T, R> {
    fn reset(&mut self) {
        self.after_content = false;
        self.value = None;
        self.delegate.reset();
    }
}


pub struct FirstOf<T, R1, R2> {
    _type: PhantomData<fn() -> T>,
    first_active: bool,
    second_active: bool,
    recognizer1: R1,
    recognizer2: R2,
}

impl<T, R1, R2> FirstOf<T, R1, R2>
    where
        R1: Recognizer<T>,
        R2: Recognizer<T>,
{

    pub fn new(recognizer1: R1, recognizer2: R2) -> Self {
        FirstOf {
            _type: PhantomData,
            first_active: true,
            second_active: true,
            recognizer1,
            recognizer2,
        }
    }

}

impl<'a, T, R1, R2> Iteratee<ParseEvent<'a>> for FirstOf<T, R1, R2>
    where
        R1: Recognizer<T>,
        R2: Recognizer<T>,
{
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let FirstOf {
            first_active,
            second_active,
            recognizer1,
            recognizer2,
            ..
        } = self;
        if *first_active && *second_active {
            match recognizer1.feed(input.clone()) {
                r@Some(Ok(_)) => {
                    *first_active = false;
                    *second_active = false;
                    return r;
                }
                Some(Err(_)) => {
                    *first_active = false;
                }
                _ => {},
            }
            match recognizer2.feed(input) {
                r@Some(Ok(_)) => {
                    *first_active = false;
                    *second_active = false;
                    r
                }
                r@Some(Err(_)) => {
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
            let r = recognizer1.feed(input.clone())?;
            *first_active = false;
            Some(r)
        } else if *second_active {
            let r = recognizer2.feed(input.clone())?;
            *second_active = false;
            Some(r)
        } else {
            Some(Err(ReadError::InconsistentState))
        }
    }
}

impl<T, R1, R2> Recognizer<T> for FirstOf<T, R1, R2>
    where
        R1: Recognizer<T>,
        R2: Recognizer<T>,
{
    fn reset(&mut self) {
        self.first_active = true;
        self.second_active = true;
        self.recognizer1.reset();
        self.recognizer2.reset()
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
enum OrdinalFieldKey<'a> {
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

impl<T, Flds> LabelledStructRecognizer<T, Flds> {

    pub fn new(tag: &'static str,
               has_header_body: bool,
               fields: Flds,
               num_fields: u32,
               select_index: for<'a> fn(LabelledFieldKey<'a>) -> Option<u32>,
               select_recog: Selector<Flds>,
               on_done: fn(&mut Flds) -> Result<T, ReadError>,
               reset: fn(&mut Flds)) -> Self {
        LabelledStructRecognizer {
            tag,
            has_header_body,
            state: LabelledStructState::Init,
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

impl<'a, T, Flds> Iteratee<ParseEvent<'a>> for LabelledStructRecognizer<T, Flds> {
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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
            LabelledStructState::Init => {
                match &input {
                    ParseEvent::StartAttribute(name) if name == *tag => {
                        if *has_header_body {
                            *state = LabelledStructState::HeaderInit;
                        } else {
                            *state = LabelledStructState::HeaderBetween;
                        }
                        None
                    }
                    _ => {
                        Some(Err(bad_kind(&input)))
                    }
                }
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
                        return Some(Err(ReadError::InconsistentState))
                    }
                    if let Err(e) = select_recog(fields, *index, input)? {
                        Some(Err(e))
                    } else {
                        *state = LabelledStructState::HeaderBetween;
                        None
                    }
                }
            },
            LabelledStructState::HeaderBodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetween;
                    None
                }
            }
            LabelledStructState::HeaderBetween => {
                match &input {
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
                            Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                        }
                    }
                    _ => Some(Err(bad_kind(&input))),
                }
            },
            LabelledStructState::HeaderExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = LabelledStructState::HeaderItem;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            },
            LabelledStructState::HeaderItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::HeaderBetween;
                    None
                }
            },
            LabelledStructState::AttrBetween => {
                match &input {
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
                            Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                        }
                    }
                    _ => Some(Err(bad_kind(&input)))
                }
            },
            LabelledStructState::AttrItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::AttrBetween;
                    None
                }
            },
            LabelledStructState::BodyBetween => {
                match &input {
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
                            Some(Err(ReadError::UnexpectedField(Text::new(name.borrow()))))
                        }
                    }
                    _ => Some(Err(bad_kind(&input))),
                }
            },
            LabelledStructState::BodyExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = LabelledStructState::BodyItem;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            },
            LabelledStructState::BodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = LabelledStructState::BodyBetween;
                    None
                }
            },
        }
    }
}

impl<T, Flds> Recognizer<T> for LabelledStructRecognizer<T, Flds> {
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

impl<'a, T, Flds> Iteratee<ParseEvent<'a>> for OrdinalStructRecognizer<T, Flds> {
    type Item = Result<T, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
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
            OrdinalStructState::Init => {
                match &input {
                    ParseEvent::StartAttribute(name) if name == *tag => {
                        if *has_header_body {
                            *state = OrdinalStructState::HeaderInit;
                        } else {
                            *state = OrdinalStructState::HeaderBetween;
                        }
                        None
                    }
                    _ => {
                        Some(Err(bad_kind(&input)))
                    }
                }
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
                        return Some(Err(ReadError::InconsistentState))
                    }
                    if let Err(e) = select_recog(fields, *index, input)? {
                        Some(Err(e))
                    } else {
                        *state = OrdinalStructState::HeaderBetween;
                        None
                    }
                }
            },
            OrdinalStructState::HeaderBodyItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetween;
                    None
                }
            }
            OrdinalStructState::HeaderBetween => {
                match &input {
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
                }
            },
            OrdinalStructState::HeaderExpectingSlot => {
                if matches!(&input, ParseEvent::Slot) {
                    *state = OrdinalStructState::HeaderItem;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            },
            OrdinalStructState::HeaderItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::HeaderBetween;
                    None
                }
            },
            OrdinalStructState::AttrBetween => {
                match &input {
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
                    _ => Some(Err(bad_kind(&input)))
                }
            },
            OrdinalStructState::AttrItem => {
                if let Err(e) = select_recog(fields, *index, input)? {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = OrdinalStructState::AttrBetween;
                    None
                }
            },
            OrdinalStructState::BodyBetween => {
                match &input {
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
            },
        }
    }
}

impl<T, Flds> Recognizer<T> for OrdinalStructRecognizer<T, Flds> {
    fn reset(&mut self) {
        self.state = OrdinalStructState::Init;
        self.progress.clear();
        self.index = 0;
        (self.reset)(&mut self.fields);
    }
}

impl<T: RecognizerReadable> RecognizerReadable for Arc<T> {
    type Rec = MappedRecognizer<T, T::Rec, fn(T) -> Arc<T>>;
    type AttrRec = MappedRecognizer<T, T::AttrRec, fn(T) -> Arc<T>>;

    fn make_recognizer() -> Self::Rec {
        MappedRecognizer::new(T::make_recognizer(), Arc::new)
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        MappedRecognizer::new(T::make_attr_recognizer(), Arc::new)
    }
}

pub struct OptionRecognizer<T, R> {
    inner: R,
    reading_value: bool,
    _type: PhantomData<fn() -> T>
}

impl<T, R> OptionRecognizer<T, R> {

    fn new(inner: R) -> Self {
        OptionRecognizer {
            inner,
            reading_value: false,
            _type: PhantomData,
        }
    }

}

impl<'a, T, R: Recognizer<T>> Iteratee<ParseEvent<'a>> for OptionRecognizer<T, R> {
    type Item = Result<Option<T>, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let OptionRecognizer { inner, reading_value, _type } = self;
        if *reading_value {
            inner.feed(input).map(|r| r.map(Option::Some))
        } else {
            match &input {
                ParseEvent::Extant => {
                    let r = inner.feed(ParseEvent::Extant);
                    if matches!(&r, Some(Err(_))) {
                        Some(Ok(None))
                    } else {
                        r.map(|r| r.map(Option::Some))
                    }
                }
                _ => {
                    *reading_value = true;
                    inner.feed(input).map(|r| r.map(Option::Some))
                }
            }
        }
    }
}

impl<'a, T, R: Recognizer<T>> Recognizer<Option<T>> for OptionRecognizer<T, R> {
    fn reset(&mut self) {
        self.reading_value = false;
        self.inner.reset();
    }
}

pub struct EmptyAttrRecognizer<T> {
    seen_extant: bool,
    _type: PhantomData<fn() -> Option<T>>
}

impl<T> Default for EmptyAttrRecognizer<T> {
    fn default() -> Self {
        EmptyAttrRecognizer {
            seen_extant: false,
            _type: PhantomData,
        }
    }
}

impl<'a, T> Iteratee<ParseEvent<'a>> for EmptyAttrRecognizer<T> {
    type Item = Result<Option<T>, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        if self.seen_extant && matches!(&input, ParseEvent::EndAttribute) {
            Some(Ok(None))
        } else if !self.seen_extant && matches!(&input, ParseEvent::Extant) {
            self.seen_extant = true;
            None
        } else {
            Some(Err(bad_kind(&input)))
        }
    }
}

impl<T> Recognizer<Option<T>> for EmptyAttrRecognizer<T> {
    fn reset(&mut self) {
        self.seen_extant = false;
    }
}

pub struct MappedRecognizer<T, R, F> {
    inner: R,
    f: F,
    _type: PhantomData<fn() -> T>
}

impl<T, R, F> MappedRecognizer<T, R, F> {

    fn new(inner: R, f: F) -> Self {
        MappedRecognizer {
            inner,
            f,
            _type: PhantomData,
        }
    }

}

impl<'a, T, U, R, F> Iteratee<ParseEvent<'a>> for MappedRecognizer<T, R, F>
where
    R: Recognizer<T>,
    F: Fn(T) -> U,
{
    type Item = Result<U, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let MappedRecognizer { inner, f, .. } = self;
        inner.feed(input).map(|r| r.map(f))
    }
}

impl<T, U, R, F> Recognizer<U> for MappedRecognizer<T, R, F>
    where
        R: Recognizer<T>,
        F: Fn(T) -> U,
{
    fn reset(&mut self) {
        self.inner.reset()
    }
}
impl<T: RecognizerReadable> RecognizerReadable for Option<T> {
    type Rec = OptionRecognizer<T, T::Rec>;
    type AttrRec = FirstOf<Option<T>, EmptyAttrRecognizer<T>, MappedRecognizer<T, T::AttrRec, fn(T) -> Option<T>>>;

    fn make_recognizer() -> Self::Rec {
        OptionRecognizer::new(T::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        FirstOf::new(EmptyAttrRecognizer::default(), MappedRecognizer::new(T::make_attr_recognizer(), Option::Some))
    }

    fn on_absent() -> Option<Self> {
        Some(None)
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
    Value
}

pub struct HashMapRecognizer<K, V, RK, RV> {
    is_attr_body: bool,
    stage: MapStage,
    key: Option<K>,
    map: HashMap<K, V>,
    key_rec: RK,
    val_rec: RV,
}

impl<K, V, RK, RV> HashMapRecognizer<K, V, RK, RV> {

    fn new(key_rec: RK,
           val_rec: RV) -> Self {
        HashMapRecognizer {
            is_attr_body: false,
            stage: MapStage::Init,
            key: None,
            map: HashMap::new(),
            key_rec,
            val_rec,
        }
    }

    fn new_attr(key_rec: RK,
           val_rec: RV) -> Self {
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
    type Rec = HashMapRecognizer<K, V, K::Rec, V::Rec>;
    type AttrRec = HashMapRecognizer<K, V, K::Rec, V::Rec>;

    fn make_recognizer() -> Self::Rec {
        HashMapRecognizer::new(K::make_recognizer(), V::make_recognizer())
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        HashMapRecognizer::new_attr(K::make_recognizer(), V::make_recognizer())
    }
}

impl<'a, K, V, RK, RV> Iteratee<ParseEvent<'a>> for HashMapRecognizer<K, V, RK, RV>
where
    K: Eq + Hash,
    RK: Recognizer<K>,
    RV: Recognizer<V>,
{
    type Item = Result<HashMap<K, V>, ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match self.stage {
            MapStage::Init => {
                if matches!(&input, ParseEvent::StartBody) {
                    self.stage = MapStage::Between;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            MapStage::Between => {
                match &input {
                    ParseEvent::EndRecord if !self.is_attr_body => {
                        Some(Ok(std::mem::take(&mut self.map)))
                    }
                    ParseEvent::EndAttribute if self.is_attr_body => {
                        Some(Ok(std::mem::take(&mut self.map)))
                    }
                    _ => {
                        self.stage = MapStage::Key;
                        match self.key_rec.feed(input)? {
                            Ok(t) => {
                                self.key = Some(t);
                                self.key_rec.reset();
                                self.stage = MapStage::Slot;
                                None
                            }
                            Err(e) => {
                                Some(Err(e))
                            }
                        }
                    }
                }
            }
            MapStage::Key => {
                match self.key_rec.feed(input)? {
                    Ok(t) => {
                        self.key = Some(t);
                        self.key_rec.reset();
                        self.stage = MapStage::Slot;
                        None
                    }
                    Err(e) => {
                        Some(Err(e))
                    }
                }
            }
            MapStage::Slot => {
                if matches!(&input, ParseEvent::Slot) {
                    self.stage = MapStage::Value;
                    None
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            MapStage::Value => {
                match self.val_rec.feed(input)? {
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
                    Err(e) => {
                        Some(Err(e))
                    }
                }
            }
        }
    }
}

impl<K, V, RK, RV> Recognizer<HashMap<K, V>> for HashMapRecognizer<K, V, RK, RV>
    where
        K: Eq + Hash,
        RK: Recognizer<K>,
        RV: Recognizer<V>,
{
    fn reset(&mut self) {
        self.key = None;
        self.stage = MapStage::Init;
        self.key_rec.reset();
        self.val_rec.reset();
    }
}
