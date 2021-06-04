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

use crate::form::structural::read::parser::{ParseEvent, NumericLiteral};
use utilities::iteratee::Iteratee;
use crate::form::structural::read::ReadError;
use crate::model::text::Text;
use crate::model::ValueKind;
use std::borrow::Borrow;
use either::Either;
use std::marker::PhantomData;
use num_bigint::{BigInt, BigUint};

pub trait RecognizerReadableBase: Sized + 'static {

    type Builder: 'static;
    type AttrBuilder: 'static;

    fn make_builder() -> Self::Builder;
    fn make_attr_builder() -> Self::AttrBuilder;

    fn try_complete(builder: Self::Builder) -> Option<Self>;
    fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self>;

}

pub trait RecognizerReadableF<'v>: RecognizerReadableBase {

    type Rec: Recognizer<()> + 'v;
    type AttrRec: Recognizer<()> + 'v;

    fn make_recognizer(builder: &'v mut Self::Builder) -> Self::Rec;

    fn make_attr_recognizer(builder: &'v mut Self::AttrBuilder) -> Self::AttrRec;

}

pub trait RecognizerReadable: for<'v> RecognizerReadableF<'v> {}

impl<T> RecognizerReadable for T
where
    T: for<'v> RecognizerReadableF<'v>,
{}

pub trait Recognizer<R>: for<'a> Iteratee<ParseEvent<'a>, Item = Result<R, ReadError>> {

    fn reset(&mut self);

}

pub struct SetValue<'v, T, R> {
    target: &'v mut Option<T>,
    name: Option<&'static str>,
    rec: R,
}

impl<'a, 'v, T, R> Iteratee<ParseEvent<'a>> for SetValue<'v, T, R>
where
    R: Recognizer<T>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let SetValue { target, name, rec } = self;
        let result = rec.feed(input)?;
        let modified = match result {
            Ok(r) => {
                if target.is_some() {
                    if let Some(name) = name {
                        Err(ReadError::DuplicateField(Text::new(name)))
                    } else {
                        Err(ReadError::InconsistentState)
                    }
                } else {
                    **target = Some(r);
                    Ok(())
                }
            }
            Err(e) => Err(e),
        };
        Some(modified)
    }
}

impl<'v, T, R> Recognizer<()> for SetValue<'v, T, R>
    where
        R: Recognizer<T>,
{
    fn reset(&mut self) {
        self.rec.reset()
    }
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
        _ => ReadError::InconsistentState,
    }
}

macro_rules! simple_readable {
    ($target:ty, $recog:ident) => {
        impl RecognizerReadableBase for $target {
            type Builder = Option<$target>;
            type AttrBuilder = Option<$target>;

            fn make_builder() -> Self::Builder {
                None
            }

            fn make_attr_builder() -> Self::AttrBuilder {
                None
            }

            fn try_complete(builder: Self::Builder) -> Option<Self> {
                builder
            }

            fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self> {
                builder
            }
        }

        impl<'v> RecognizerReadableF<'v> for $target {
            type Rec = SetValue<'v, $target, primitive::$recog>;
            type AttrRec = SimpleAttrBody<SetValue<'v, $target, primitive::$recog>>;

            fn make_recognizer(builder: &'v mut Self::Builder) -> Self::Rec {
                SetValue {
                    target: builder,
                    name: None,
                    rec: primitive::$recog
                }
            }

            fn make_attr_recognizer(builder: &'v mut Self::AttrBuilder) -> Self::AttrRec {
                SimpleAttrBody::new(Self::make_recognizer(builder))
            }
        }
    }
}

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

impl<T: RecognizerReadableBase> RecognizerReadableBase for Vec<T> {
    type Builder = (T::Builder, Vec<T>);
    type AttrBuilder = (T::Builder, Vec<T>);

    fn make_builder() -> Self::Builder {
        (T::make_builder(), vec![])
    }

    fn make_attr_builder() -> Self::AttrBuilder {
        (T::make_builder(), vec![])
    }

    fn try_complete(builder: Self::Builder) -> Option<Self> {
        let (_, v) = builder;
        Some(v)
    }

    fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self> {
        let (_, v) = builder;
        Some(v)
    }
}
/*
pub struct VecRecognizer<'v, R: 'v, T> {
    is_attr_body: bool,
    recognizer: R,
    vector: &'v mut Vec<T>,
    stage: BodyStage,
}

impl<'a, T: RecognizerReadable> RecognizerReadableF<'a> for Vec<T> {
    type Rec = VecRecognizer<'a, T::Builder, T>;
    type AttrRec = VecRecognizer<'a, T::Builder, T>;

    fn make_recognizer(builder: &'a mut Self::Builder) -> Self::Rec {
        let (builder, v) = builder;
        VecRecognizer {
            is_attr_body: false,
            recognizer: T::make_recognizer(builder),
            vector: v,
            stage: BodyStage::Init
        }
    }

    fn make_attr_recognizer(builder: &'a mut Self::AttrBuilder) -> Self::AttrRec {
        let (builder, v) = builder;
        VecRecognizer {
            is_attr_body: true,
            recognizer: T::make_recognizer(builder),
            vector: v,
            stage: BodyStage::Init
        }
    }
}

impl<'a, 'v, T: RecognizerReadable> Iteratee<ParseEvent<'a>> for VecRecognizer<'v, <T as RecognizableReadable<'v>>::Rec, T> {
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let VecRecognizer { is_attr_body, recognizer, vector, stage } = self;
        match stage {
            BodyStage::Init => {
                if matches!(&input, ParseEvent::StartBody) {
                    *stage = BodyStage::Between;
                    None
                } else {
                    Some(bad_kind(&input))
                }
            }
            BodyStage::Between => {
                if (!*is_attr_body && matches!(&input, ParseEvent::EndRecord)) ||
                    (*is_attr_body && matches!(&input, ParseEvent::EndAttribute)) {
                    Some(Ok(()))
                } else {
                    let r = recognizer.feed(input)?;
                    if r.is_ok() {

                    }
                }
            }
            BodyStage::Item => {
                todo!()
            }
        }
    }
}

impl<'v, T: RecognizerReadable> Recognizer<()> for VecRecognizer<'v, <T as RecognizableReadable<'v>>::Rec, T> {
    fn reset(&mut self) {
        self.stage = BodyStage::Init;
        *self.builder = T::make_builder();
        self.vector.clear();
    }
}*/

impl<T, U> RecognizerReadableBase for (T, U)
where
    T: RecognizerReadableBase,
    U: RecognizerReadableBase,
{
    type Builder = (T::Builder, U::Builder);
    type AttrBuilder = ((T::Builder, U::Builder), (T::Builder, U::Builder));

    fn make_builder() -> Self::Builder {
        (T::make_builder(), U::make_builder())
    }

    fn make_attr_builder() -> Self::AttrBuilder {
        (Self::make_builder(), Self::make_builder())
    }

    fn try_complete(builder: Self::Builder) -> Option<Self> {
        let (builder1, builder2) = builder;
        let left = T::try_complete(builder1)?;
        let right = U::try_complete(builder2)?;
        Some((left, right))
    }

    fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self> {
        let (first, second) = builder;
        let r1 = Self::try_complete(first);
        if r1.is_some() {
            r1
        } else {
            Self::try_complete(second)
        }
    }
}

impl<'v, T, U> RecognizerReadableF<'v> for (T, U)
where
    T: RecognizerReadableF<'v>,
    U: RecognizerReadableF<'v>,
{
    type Rec = PairRecognizer<T::Rec, U::Rec>;
    type AttrRec = FirstOf<(), PairRecognizer<T::Rec, U::Rec>, SimpleAttrBody<PairRecognizer<T::Rec, U::Rec>>>;

    fn make_recognizer(builder: &'v mut Self::Builder) -> Self::Rec {
        let (left, right) = builder;
        PairRecognizer {
            is_attr: false,
            stage: BodyStage::Init,
            count: 0,
            left_recog: T::make_recognizer(left),
            right_recog: U::make_recognizer(right),
        }
    }

    fn make_attr_recognizer(builder: &'v mut Self::AttrBuilder) -> Self::AttrRec {
        let ((left1, right1), (left2, right2)) = builder;
        let option1 = PairRecognizer {
            is_attr: true,
            stage: BodyStage::Between,
            count: 0,
            left_recog: T::make_recognizer(left1),
            right_recog: U::make_recognizer(right1),
        };
        let option2 = SimpleAttrBody::new(PairRecognizer {
            is_attr: false,
            stage: BodyStage::Init,
            count: 0,
            left_recog: T::make_recognizer(left2),
            right_recog: U::make_recognizer(right2),
        });
        FirstOf::new(option1, option2)
    }
}

#[derive(Clone, Copy)]
enum BodyStage {
    Init,
    Item,
    Between,
}

pub struct PairRecognizer<RT, RU> {
    is_attr: bool,
    stage: BodyStage,
    count: usize,
    left_recog: RT,
    right_recog: RU,
}

impl<RT, RU> PairRecognizer<RT, RU>
    where
        RT: Recognizer<()>,
        RU: Recognizer<()>,
{

    fn delegate<'a>(&mut self, input: ParseEvent<'a>) -> Option<Result<(), ReadError>> {
        let PairRecognizer { stage, count, left_recog, right_recog, .. } = self;
        match *count {
            0 => {
                let r = left_recog.feed(input)?;
                if let Err(e) = r {
                    Some(Err(e))
                } else {
                    *stage = BodyStage::Between;
                    *count = 1;
                    None
                }
            },
            1 => {
                let r = right_recog.feed(input)?;
                if let Err(e) = r {
                    Some(Err(e))
                } else {
                    *stage = BodyStage::Between;
                    *count = 2;
                    None
                }
            },
            _ => return Some(Err(ReadError::UnexpectedItem)),
        }
    }

}

impl<'a, RT, RU> Iteratee<ParseEvent<'a>> for PairRecognizer<RT, RU>
where
    RT: Recognizer<()>,
    RU: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

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
            BodyStage::Item => {
                self.delegate(input)
            }
            BodyStage::Between => {
                if (!self.is_attr && matches!(&input, ParseEvent::EndRecord)) || (self.is_attr && matches!(&input, ParseEvent::EndAttribute)) {
                    Some(Ok(()))
                } else {
                    self.stage = BodyStage::Item;
                    self.delegate(input)
                }
            }
        }
    }
}

impl<RT, RU> Recognizer<()> for PairRecognizer<RT, RU>
where
    RT: Recognizer<()>,
    RU: Recognizer<()>,
{
    fn reset(&mut self) {
        self.stage = BodyStage::Init;
        self.count = 0;
        self.left_recog.reset();
        self.right_recog.reset();
    }
}

impl<T, U> RecognizerReadableBase for Either<T, U>
    where
        T: RecognizerReadableBase,
        U: RecognizerReadableBase,
{
    type Builder = (T::Builder, U::Builder, bool);
    type AttrBuilder = (T::Builder, U::Builder, bool);


    fn make_builder() -> Self::Builder {
        (T::make_builder(), U::make_builder(), true)
    }

    fn make_attr_builder() -> Self::AttrBuilder {
        Self::make_builder()
    }

    fn try_complete(builder: Self::Builder) -> Option<Self> {
        let (left, right, is_left) = builder;
        if is_left {
            Some(Either::Left(T::try_complete(left)?))
        } else {
            Some(Either::Right(U::try_complete(right)?))
        }
    }

    fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self> {
        Self::try_complete(builder)
    }
}

impl<'v, T, U> RecognizerReadableF<'v> for Either<T, U>
    where
        T: RecognizerReadableF<'v>,
        U: RecognizerReadableF<'v>,
{
    type Rec = EitherRecognizer<'v, T::Rec, U::Rec>;
    type AttrRec = SimpleAttrBody<EitherRecognizer<'v, T::Rec, U::Rec>>;

    fn make_recognizer(builder: &'v mut Self::Builder) -> Self::Rec {
        let (left, right, is_left) = builder;
        EitherRecognizer {
            stage: Some(BodyStage::Init),
            is_left,
            left: T::make_recognizer(left),
            right: U::make_recognizer(right),
        }
    }

    fn make_attr_recognizer(builder: &'v mut Self::AttrBuilder) -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer(builder))
    }
}

pub struct EitherRecognizer<'v, RT, RU> {
    stage: Option<BodyStage>,
    is_left: &'v mut bool,
    left: RT,
    right: RU,
}

impl<'a, 'v, RT, RU> Iteratee<ParseEvent<'a>> for EitherRecognizer<'v, RT, RU>
where
    RT: Recognizer<()>,
    RU: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        match self.stage {
            Some(BodyStage::Init) => {
                if let ParseEvent::StartAttribute(name) = input {
                    match name.borrow() {
                        "left" => {
                            self.stage = Some(BodyStage::Item);
                            *self.is_left = true;
                            None
                        },
                        "right" => {
                            self.stage = Some(BodyStage::Item);
                            *self.is_left = false;
                            None
                        },
                        ow => Some(Err(ReadError::UnexpectedAttribute(Text::new(ow)))),
                    }
                } else {
                    Some(Err(bad_kind(&input)))
                }
            }
            Some(BodyStage::Item) => {
                let r = if *self.is_left {
                    self.left.feed(input)?
                } else {
                    self.right.feed(input)?
                };
                if let Err(e) = r {
                    Some(Err(e))
                } else {
                    self.stage = Some(BodyStage::Between);
                    None
                }
            }
            Some(BodyStage::Between) => {
                if matches!(&input, ParseEvent::EndAttribute) {
                    self.stage = None;
                    None
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
            _ => {
                if matches!(&input, ParseEvent::EndRecord) {
                    Some(Ok(()))
                } else {
                    Some(Err(ReadError::UnexpectedItem))
                }
            }
        }
    }
}

impl<'v, RT, RU> Recognizer<()> for EitherRecognizer<'v, RT, RU>
    where
        RT: Recognizer<()>,
        RU: Recognizer<()>,
{
    fn reset(&mut self) {
        self.stage = Some(BodyStage::Init);
        self.left.reset();
        self.right.reset();
    }
}

type Selector<Flds> = for<'a> fn(&mut Flds, u32, ParseEvent<'a>) -> Option<Result<(), ReadError>>;

struct Bitset(u32, u64);

impl Bitset {

    fn new(cap: u32) -> Self {
        Bitset(cap, 0)
    }

    fn capacity(&self) -> u32 {
        self.0
    }

    fn filled(&self) -> u32 {
        u64::count_ones(self.1)
    }

    fn is_full(&self) -> bool {
        self.filled() == self.0
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

pub struct NamedFieldsRecognizer<Flds> {
    is_attr_body: bool,
    state: BodyFieldState,
    fields: Flds,
    progress: Bitset,
    select_index: fn(&str) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
}

pub struct OrdinalFieldsRecognizer<Flds> {
    is_attr_body: bool,
    state: BodyStage,
    fields: Flds,
    num_fields: u32,
    index: u32,
    select_recog: Selector<Flds>,
}

impl<Flds> NamedFieldsRecognizer<Flds> {

    pub fn new(fields: Flds,
               num_fields: u32,
               select_index: fn(&str) -> Option<u32>,
               select_recog: Selector<Flds>) -> Self {
        NamedFieldsRecognizer {
            is_attr_body: false,
            state: BodyFieldState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index,
            index: 0,
            select_recog
        }
    }

    pub fn new_attr(fields: Flds,
                    num_fields: u32,
                    select_index: fn(&str) -> Option<u32>,
                    select_recog: Selector<Flds>) -> Self {
        NamedFieldsRecognizer {
            is_attr_body: true,
            state: BodyFieldState::Between,
            fields,
            progress: Bitset::new(num_fields),
            select_index,
            index: 0,
            select_recog
        }
    }

}

impl<Flds> OrdinalFieldsRecognizer<Flds> {

    pub fn new(fields: Flds,
               num_fields: u32,
               select_recog: Selector<Flds>) -> Self {
        OrdinalFieldsRecognizer {
            is_attr_body: false,
            state: BodyStage::Init,
            fields,
            num_fields,
            index: 0,
            select_recog
        }
    }

    pub fn new_attr(fields: Flds,
               num_fields: u32,
               select_recog: Selector<Flds>) -> Self {
        OrdinalFieldsRecognizer {
            is_attr_body: true,
            state: BodyStage::Init,
            fields,
            num_fields,
            index: 0,
            select_recog
        }
    }

}

enum AttrFieldState {
    Init,
    Between,
    AttrValue,
    Body,
}

pub struct AttrFieldsRecognizer<Flds, Body> {
    state: AttrFieldState,
    fields: Flds,
    progress: Bitset,
    select_index: fn(&str) -> Option<u32>,
    index: u32,
    select_recog: Selector<Flds>,
    body: Body,
}

impl<Flds, Body> AttrFieldsRecognizer<Flds, Body> {

    pub fn new(fields: Flds,
               num_fields: u32,
               select_index: fn(&str) -> Option<u32>,
               select_recog: Selector<Flds>,
               body: Body) -> Self {
        AttrFieldsRecognizer {
            state: AttrFieldState::Init,
            fields,
            progress: Bitset::new(num_fields),
            select_index,
            index: 0,
            select_recog,
            body,
        }
    }

}

impl<'a, Flds> Iteratee<ParseEvent<'a>> for NamedFieldsRecognizer<Flds> {
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let NamedFieldsRecognizer {
            is_attr_body,
            state,
            fields,
            progress,
            select_index,
            index,
            select_recog
        } = self;
        match state {
            BodyFieldState::Init => {
                debug_assert!(!*is_attr_body);
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
                        Some(Ok(()))
                    }
                    ParseEvent::EndAttribute if *is_attr_body => {
                        Some(Ok(()))
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

impl<Flds> Recognizer<()> for NamedFieldsRecognizer<Flds> {
    fn reset(&mut self) {
        self.progress.clear();
        self.state = BodyFieldState::Init;
    }
}

impl<'a, Flds> Iteratee<ParseEvent<'a>> for OrdinalFieldsRecognizer<Flds> {
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let OrdinalFieldsRecognizer {
            is_attr_body,
            state,
            fields,
            num_fields,
            index,
            select_recog
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
            Some(Ok(()))
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

impl<Flds> Recognizer<()> for OrdinalFieldsRecognizer<Flds> {
    fn reset(&mut self) {
        self.index = 0;
        self.state = BodyStage::Init;
    }
}

impl<'a, Flds, Body> Iteratee<ParseEvent<'a>> for AttrFieldsRecognizer<Flds, Body>
where
    Body: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let AttrFieldsRecognizer {
            state,
            fields,
            progress,
            select_index,
            index,
            select_recog,
            body,
        } = self;
        match state {
            AttrFieldState::Init => {
                match &input {
                    ParseEvent::StartAttribute(name) => {
                        if let Some(i) = select_index(name.borrow()) {
                            if progress.get(i).unwrap_or(false) {
                                Some(Err(ReadError::DuplicateField(Text::new(name.borrow()))))
                            } else {
                                *index = i;
                                *state = AttrFieldState::AttrValue;
                                None
                            }
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(Text::new(name.borrow()))))
                        }
                    }
                    ParseEvent::StartBody => {
                        let r = body.feed(input);
                        if r.is_none() {
                            *state = AttrFieldState::Body;
                        }
                        r
                    }
                    ow => Some(Err(bad_kind(ow))),
                }
            }
            AttrFieldState::Between => {
                match &input {
                    ParseEvent::StartAttribute(name) => {
                        if let Some(i) = select_index(name.borrow()) {
                            *index = i;
                            *state = AttrFieldState::AttrValue;
                            None
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(Text::new(name.borrow()))))
                        }
                    }
                    ParseEvent::StartBody => {
                        let r = body.feed(input);
                        if r.is_none() {
                            *state = AttrFieldState::Body;
                        }
                        r
                    }
                    ow => {
                        Some(Err(bad_kind(ow)))
                    }
                }
            }
            AttrFieldState::AttrValue => {
                let r = select_recog(fields, *index, input)?;
                if let Err(e) = r {
                    Some(Err(e))
                } else {
                    progress.set(*index);
                    *state = AttrFieldState::Between;
                    None
                }
            }
            AttrFieldState::Body => {
                body.feed(input)
            }
        }
    }
}

impl<Flds, Body> Recognizer<()> for AttrFieldsRecognizer<Flds, Body>
    where
        Body: Recognizer<()>,
{
    fn reset(&mut self) {
        self.progress.clear();
        self.state = AttrFieldState::Init;
        self.body.reset();
    }
}

enum TaggedStage {
    Init,
    ReadingTag,
    Delegate
}

pub struct TaggedStructRecognizer<Header, Delegate> {
    name: &'static str,
    stage: TaggedStage,
    header: Header,
    delegate: Delegate,
}

impl<Header, Delegate> TaggedStructRecognizer<Header, Delegate> {

    pub fn new(name: &'static str,
               header: Header,
               delegate: Delegate) -> Self {
        TaggedStructRecognizer {
            name,
            stage: TaggedStage::Init,
            header,
            delegate
        }
    }

}

impl<'a, Header, Delegate> Iteratee<ParseEvent<'a>> for TaggedStructRecognizer<Header, Delegate>
where
    Header: Recognizer<()>,
    Delegate: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let TaggedStructRecognizer {
            name,
            stage,
            header,
            delegate
        } = self;
        match stage {
            TaggedStage::Init => {
                match &input {
                    ParseEvent::StartAttribute(attr_name) => {
                        if attr_name == name {
                            *stage = TaggedStage::ReadingTag;
                            None
                        } else {
                            Some(Err(ReadError::UnexpectedAttribute(Text::new(attr_name.borrow()))))
                        }
                    }
                    _ => Some(Err(bad_kind(&input))),
                }
            },
            TaggedStage::ReadingTag => {
                match header.feed(input) {
                    Some(Ok(_)) => {
                        *stage = TaggedStage::Delegate;
                        None
                    }
                    Some(Err(e)) => {
                        Some(Err(e))
                    }
                    _ => None,
                }
            }
            TaggedStage::Delegate => delegate.feed(input)
        }
    }
}

impl<Header, Delegate> Recognizer<()> for TaggedStructRecognizer<Header, Delegate>
    where
        Header: Recognizer<()>,
        Delegate: Recognizer<()>,
{
    fn reset(&mut self) {
        self.stage = TaggedStage::Init;
        self.header.reset();
        self.delegate.reset();
    }
}

pub struct SimpleAttrBody<Delegate> {
    after_content: bool,
    delegate: Delegate,
}

impl<Delegate> SimpleAttrBody<Delegate> {

    pub fn new(delegate: Delegate) -> Self {
        SimpleAttrBody {
            after_content: false,
            delegate,
        }
    }

}

impl<'a, Delegate> Iteratee<ParseEvent<'a>> for SimpleAttrBody<Delegate>
where
    Delegate: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        if self.after_content {
            if matches!(&input, ParseEvent::EndAttribute) {
                Some(Ok(()))
            } else {
                Some(Err(bad_kind(&input)))
            }
        } else {
            let r = self.delegate.feed(input);
            if r.is_some() {
                self.after_content = true;
            }
            r
        }
    }
}


impl<Delegate> Recognizer<()> for SimpleAttrBody<Delegate>
    where
        Delegate: Recognizer<()>,
{
    fn reset(&mut self) {
        self.after_content = false;
        self.delegate.reset();
    }
}

pub struct HeaderRecognizer<Body, Remainder> {
    after_body: bool,
    body: Body,
    remainder: Remainder,
}

impl<Body, Remainder> HeaderRecognizer<Body, Remainder> {

    pub fn new(body: Body, remainder: Remainder) -> Self {
        HeaderRecognizer {
            after_body: false,
            body,
            remainder,
        }
    }

}

impl<'a, Body, Remainder> Iteratee<ParseEvent<'a>> for HeaderRecognizer<Body, Remainder>
    where
        Body: Recognizer<()>,
        Remainder: Recognizer<()>,
{
    type Item = Result<(), ReadError>;

    fn feed(&mut self, input: ParseEvent<'a>) -> Option<Self::Item> {
        let HeaderRecognizer { after_body, body, remainder } = self;
        if *after_body {
            remainder.feed(input)
        } else {
            let result = body.feed(input)?;
            *after_body = true;
            Some(result)
        }
    }
}

impl<Body, Remainder> Recognizer<()> for HeaderRecognizer<Body, Remainder>
    where
        Body: Recognizer<()>,
        Remainder: Recognizer<()>,
{
    fn reset(&mut self) {
        self.after_body = false;
        self.body.reset();
        self.remainder.reset();
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

pub type ComplexAttrBody<R> = FirstOf<(), SimpleAttrBody<R>, R>;

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

#[cfg(test)]
mod tests {

    use super::*;

    fn read<'a, It, T>(it: It) -> Result<T, ReadError>
    where
        It: Iterator<Item = ParseEvent<'a>> + 'a,
        T: RecognizerReadable,
    {
        let mut builder = T::make_builder();
        let mut recognizer = T::make_recognizer(&mut builder);

        for event in it {
            if let Some(r) = recognizer.feed(event) {
                r?;
                break;
            }
        }
        drop(recognizer);
        T::try_complete(builder).ok_or(ReadError::IncompleteRecord)
    }

    #[test]
    fn examples() {
        let events = vec![ParseEvent::Number(NumericLiteral::Int(1))];
        let r: Result<i32, ReadError> = read(events.into_iter());
        assert_eq!(r, Ok(1));
    }

    struct Example<S, T> {
        first: S,
        second: T,
    }

    impl<S: RecognizerReadable, T: RecognizerReadable> RecognizerReadableBase for Example<S, T> {
        type Builder = (S::Builder, T::Builder);
        type AttrBuilder = ((S::Builder, T::Builder), (S::Builder, T::Builder));

        fn make_builder() -> Self::Builder {
            (S::make_builder(), T::make_builder())
        }

        fn make_attr_builder() -> Self::AttrBuilder {
            (Self::make_builder(), Self::make_builder())
        }

        fn try_complete(builder: Self::Builder) -> Option<Self> {
            let (first_builder, second_builder) = builder;
            let first = S::try_complete(first_builder)?;
            let second = T::try_complete(second_builder)?;
            Some(Example {
                first,
                second,
            })
        }

        fn try_complete_attr(builder: Self::AttrBuilder) -> Option<Self> {
            let (first, second) = builder;
            let r1 = Self::try_complete(first);
            if r1.is_some() {
                r1
            } else {
                Self::try_complete(second)
            }
        }
    }

    fn example_select_field(name: &str) -> Option<u32> {
        match name {
            "first" => Some(0),
            "second" => Some(1),
            _ => None,
        }
    }

    fn example_select<'a, R1: Recognizer<()>, R2: Recognizer<()>>(state: &mut (R1, R2), index: u32, input: ParseEvent<'a>) -> Option<Result<(), ReadError>> {
        let (first, second) = state;
        match index {
            0 => first.feed(input),
            1 => second.feed(input),
            _ => Some(Err(ReadError::InconsistentState)),
        }
    }

    impl<'a, S: RecognizerReadable, T: RecognizerReadable> RecognizerReadableF<'a> for Example<S, T> {
        type Rec = NamedFieldsRecognizer<(<S as RecognizerReadableF<'a>>::Rec, <T as RecognizerReadableF<'a>>::Rec)>;
        type AttrRec = ComplexAttrBody<NamedFieldsRecognizer<(<S as RecognizerReadableF<'a>>::Rec, <T as RecognizerReadableF<'a>>::Rec)>>;

        fn make_recognizer(builder: &'a mut Self::Builder) -> Self::Rec {
            let (first_builder, second_builder) = builder;
            let first_rec = <S as RecognizerReadableF<'a>>::make_recognizer(first_builder);
            let second_rec = <T as RecognizerReadableF<'a>>::make_recognizer(second_builder);
            NamedFieldsRecognizer::new((first_rec, second_rec), 2, example_select_field, example_select)
        }

        fn make_attr_recognizer(builder: &'a mut Self::AttrBuilder) -> Self::AttrRec {
            let ((first_builder1, second_builder1), (first_builder2, second_builder2)) = builder;
            let first_rec1 = <S as RecognizerReadableF<'a>>::make_recognizer(first_builder1);
            let second_rec1 = <T as RecognizerReadableF<'a>>::make_recognizer(second_builder1);
            let option2 = NamedFieldsRecognizer::new_attr((first_rec1, second_rec1), 2, example_select_field, example_select);

            let first_rec2 = <S as RecognizerReadableF<'a>>::make_recognizer(first_builder2);
            let second_rec2 = <T as RecognizerReadableF<'a>>::make_recognizer(second_builder2);
            let option1 = SimpleAttrBody::new(NamedFieldsRecognizer::new((first_rec2, second_rec2), 2, example_select_field, example_select));
            FirstOf::new(option1, option2)
        }
    }
}
