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

mod msgpack;

use crate::form::structural::read::improved::{
    FirstOf, NamedFieldsRecognizer, Recognizer, RecognizerReadable, SimpleAttrBody,
};
use crate::form::structural::read::parser::ParseEvent;
use crate::form::structural::read::ReadError;
use crate::form::structural::write::{BodyWriter, HeaderWriter};
use crate::form::structural::write::{
    PrimitiveWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use crate::model::text::Text;
use std::borrow::Cow;
use std::prelude::v1::Result::Err;

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct GeneralType<S, T> {
    first: S,
    second: T,
}

impl<S, T> GeneralType<S, T> {
    pub fn new(first: S, second: T) -> Self {
        GeneralType { first, second }
    }
}

type GeneralFields<S, T> = (
    Option<S>,
    Option<T>,
    <S as RecognizerReadable>::Rec,
    <T as RecognizerReadable>::Rec,
);
type GeneralRec<S, T> = NamedFieldsRecognizer<GeneralType<S, T>, GeneralFields<S, T>>;
type GeneralAttrRec<S, T> = FirstOf<
    GeneralType<S, T>,
    GeneralRec<S, T>,
    SimpleAttrBody<GeneralType<S, T>, GeneralRec<S, T>>,
>;

fn general_select_field(name: &str) -> Option<u32> {
    match name {
        "first" => Some(0),
        "second" => Some(1),
        _ => None,
    }
}

fn general_select<'a, S, RS: Recognizer<S>, T, RT: Recognizer<T>>(
    state: &mut (Option<S>, Option<T>, RS, RT),
    index: u32,
    input: ParseEvent<'a>,
) -> Option<Result<(), ReadError>> {
    let (first, second, first_rec, second_rec) = state;
    match index {
        0 => {
            if first.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("first"))))
            } else {
                let r = first_rec.feed(input)?;
                match r {
                    Ok(s) => {
                        *first = Some(s);
                        Some(Ok(()))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
        1 => {
            if second.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("second"))))
            } else {
                let r = second_rec.feed(input)?;
                match r {
                    Ok(t) => {
                        *second = Some(t);
                        Some(Ok(()))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
        _ => Some(Err(ReadError::InconsistentState)),
    }
}

fn general_construct<
    S: RecognizerReadable,
    RS: Recognizer<S>,
    T: RecognizerReadable,
    RT: Recognizer<T>,
>(
    state: &mut (Option<S>, Option<T>, RS, RT),
) -> Result<GeneralType<S, T>, ReadError> {
    let (first, second, first_rec, second_rec) = state;
    first_rec.reset();
    second_rec.reset();
    match (
        first.take().or_else(|| S::on_absent()),
        second.take().or_else(|| T::on_absent()),
    ) {
        (Some(first), Some(second)) => Ok(GeneralType { first, second }),
        (Some(_), _) => Err(ReadError::MissingFields(vec![Text::new("second")])),
        (_, Some(_)) => Err(ReadError::MissingFields(vec![Text::new("first")])),
        _ => Err(ReadError::MissingFields(vec![
            Text::new("first"),
            Text::new("second"),
        ])),
    }
}

fn general_reset<
    S: RecognizerReadable,
    RS: Recognizer<S>,
    T: RecognizerReadable,
    RT: Recognizer<T>,
>(
    state: &mut (Option<S>, Option<T>, RS, RT),
) {
    let (first, second, first_rec, second_rec) = state;
    *first = None;
    *second = None;
    first_rec.reset();
    second_rec.reset();
}

impl<S: RecognizerReadable, T: RecognizerReadable> RecognizerReadable for GeneralType<S, T> {
    type Rec = GeneralRec<S, T>;
    type AttrRec = GeneralAttrRec<S, T>;

    fn make_recognizer() -> Self::Rec {
        NamedFieldsRecognizer::new(
            (None, None, S::make_recognizer(), T::make_recognizer()),
            general_select_field,
            2,
            general_select,
            general_construct,
            general_reset,
        )
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        let option1 = NamedFieldsRecognizer::new_attr(
            (None, None, S::make_recognizer(), T::make_recognizer()),
            general_select_field,
            2,
            general_select,
            general_construct,
            general_reset,
        );

        let option2 = SimpleAttrBody::new(Self::make_recognizer());
        FirstOf::new(option1, option2)
    }
}

impl<S: StructuralWritable, T: StructuralWritable> StructuralWritable for GeneralType<S, T> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let GeneralType { first, second } = self;
        let mut rec_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::MapLike, 2)?;
        if !first.omit_as_field() {
            rec_writer = rec_writer.write_slot(&"first", first)?;
        }
        if !second.omit_as_field() {
            rec_writer = rec_writer.write_slot(&"second", second)?;
        }
        rec_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let GeneralType { first, second } = self;
        let mut rec_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::MapLike, 2)?;
        if !first.omit_as_field() {
            rec_writer = rec_writer.write_slot_into("first", first)?;
        }
        if !second.omit_as_field() {
            rec_writer = rec_writer.write_slot_into("second", second)?;
        }
        rec_writer.done()
    }
}

pub struct HeaderView<T>(pub T);

struct WithHeaderBody<T> {
    header: T,
    attr: T,
    slot: T,
}

impl<T: StructuralWritable> StructuralWritable for WithHeaderBody<T> {
    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let mut rec_writer = writer.record(2)?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("StructuralWritable"), &self.header)?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("attr"), &self.attr)?;
        let mut body_writer = rec_writer.complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot(&"slot", &self.slot)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let mut rec_writer = writer.record(2)?;
        rec_writer = rec_writer.write_attr_into("StructuralWritable", self.header)?;
        rec_writer = rec_writer.write_attr_into("attr", self.attr)?;
        let mut body_writer = rec_writer.complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot_into("slot", self.slot)?;
        body_writer.done()
    }
}

struct WithHeaderField<T> {
    header: T,
    attr: T,
    slot: T,
}

impl<T: StructuralWritable> StructuralWritable for HeaderView<&WithHeaderField<T>> {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let mut body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot(&"header", &self.0.header)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        self.write_with(writer)
    }
}

impl<T: StructuralWritable> StructuralWritable for WithHeaderField<T> {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let mut rec_writer = writer.record(2)?;
        rec_writer = rec_writer.write_attr_into("StructuralWritable", HeaderView(self))?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("attr"), &self.attr)?;
        let mut body_writer = rec_writer.complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot_into("slot", &self.slot)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        self.write_with(writer)
    }
}

/*
#[derive(Debug, Eq, PartialEq)]
struct OneOfEach<T> {
    header: T,
    header_slot: T,
    attr: T,
    slot1: T,
    slot2: T,
}

type OneOfEachFields<T> = (Option<T>, Option<T>, Option<T>, Option<T>, Option<T>);

impl<T> ValueReadable for OneOfEach<T> {}

impl<T: StructuralReadable> StructuralReadable for OneOfEach<T> {
    type Reader = Builder<OneOfEach<T>, OneOfEachFields<T>>;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        Ok(Default::default())
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        let Builder { mut state, .. } = reader;

        let mut missing = vec![];
        if state.0.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("header"));
            } else {
                state.0 = on_missing;
            }
        }
        if state.1.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("header_slot"));
            } else {
                state.1 = on_missing;
            }
        }
        if state.2.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("attr"));
            } else {
                state.2 = on_missing;
            }
        }
        if state.3.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("slot1"));
            } else {
                state.3 = on_missing;
            }
        }
        if state.4.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("slot2"));
            } else {
                state.4 = on_missing;
            }
        }
        if let (Some(header), Some(header_slot), Some(attr), Some(slot1), Some(slot2), ..) = state {
            Ok(OneOfEach {
                header,
                header_slot,
                attr,
                slot1,
                slot2,
            })
        } else {
            Err(ReadError::MissingFields(missing))
        }
    }
}

impl<T: StructuralReadable> HeaderReader for Builder<OneOfEach<T>, OneOfEachFields<T>> {
    type Body = Self;
    type Delegate = CCons<
        HeaderBuilder<OneOfEach<T>, OneOfEachFields<T>>,
        CCons<Wrapped<Self, AttrReader<T>>, CNil>,
    >;

    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        if !self.read_tag {
            if name == "OneOfEach" {
                let builder = HeaderBuilder::new(self, true);
                Ok(CCons::Head(builder))
            } else {
                Err(ReadError::MissingTag)
            }
        } else {
            match name.borrow() {
                "attr" => {
                    let wrapped = Wrapped {
                        payload: self,
                        reader: AttrReader::default(),
                    };
                    Ok(CCons::Tail(CCons::Head(wrapped)))
                }
                ow => Err(ReadError::UnexpectedField(ow.into())),
            }
        }
    }

    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError> {
        match delegate {
            CCons::Head(HeaderBuilder { mut inner, .. }) => {
                inner.read_tag = true;
                Ok(inner)
            }
            CCons::Tail(CCons::Head(Wrapped {
                mut payload,
                reader,
            })) => {
                if payload.state.2.is_some() {
                    return Err(ReadError::DuplicateField(Text::new("attr")));
                } else {
                    payload.state.2 = Some(reader.try_get_value()?);
                }
                Ok(payload)
            }
            CCons::Tail(CCons::Tail(nil)) => nil.explode(),
        }
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        Ok(self)
    }
}

impl<T: StructuralReadable> Builder<OneOfEach<T>, OneOfEachFields<T>> {
    fn has_more(&self) -> bool {
        let (_, _, _, v1, v2) = &self.state;
        v1.is_none() || v2.is_none()
    }

    fn apply<P: PushPrimValue>(&mut self, push: P) -> Result<bool, ReadError> {
        if self.reading_slot {
            self.reading_slot = false;
            match self
                .current_field
                .take()
                .ok_or_else(|| ReadError::UnexpectedKind(push.kind()))?
            {
                1 => push.apply(&mut self.state.3, "slot1"),
                2 => push.apply(&mut self.state.4, "slot2"),
                _ => Err(ReadError::InconsistentState),
            }?;
            Ok(self.has_more())
        } else {
            if let Some(name) = push.text() {
                match name {
                    "slot1" => {
                        self.current_field = Some(1);
                        Ok(true)
                    }
                    "slot2" => {
                        self.current_field = Some(2);
                        Ok(true)
                    }
                    name => Err(ReadError::UnexpectedField(Text::new(name))),
                }
            } else {
                Err(ReadError::UnexpectedKind(push.kind()))
            }
        }
    }

    fn apply_header<P: PushPrimValue>(&mut self, push: P) -> Result<bool, ReadError> {
        if self.reading_slot {
            self.reading_slot = false;
            match self
                .current_field
                .take()
                .ok_or_else(|| ReadError::UnexpectedKind(push.kind()))?
            {
                0 => push.apply(&mut self.state.1, "header_slot"),
                _ => Err(ReadError::InconsistentState),
            }?;
            Ok(self.has_more())
        } else {
            if let Some(name) = push.text() {
                match name {
                    "header_slot" => {
                        self.current_field = Some(0);
                        Ok(true)
                    }
                    name => Err(ReadError::UnexpectedField(Text::new(name))),
                }
            } else {
                Err(ReadError::UnexpectedKind(push.kind()))
            }
        }
    }
}

impl<T: StructuralReadable> BodyReader for Builder<OneOfEach<T>, OneOfEachFields<T>> {
    type Delegate = CCons<Wrapped<Self, T::Reader>, CCons<Wrapped<Self, T::Reader>, CNil>>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.apply(())
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        if self.reading_slot {
            Err(ReadError::DoubleSlot)
        } else {
            self.reading_slot = true;
            Ok(())
        }
    }

    fn push_record(mut self) -> Result<Self::Delegate, ReadError> {
        if self.reading_slot {
            match self.current_field.take() {
                Some(1) => {
                    let wrapper = Wrapped {
                        payload: self,
                        reader: <T as StructuralReadable>::record_reader()?,
                    };
                    Ok(CCons::Head(wrapper))
                }
                Some(2) => {
                    let wrapper = Wrapped {
                        payload: self,
                        reader: <T as StructuralReadable>::record_reader()?,
                    };
                    Ok(CCons::Tail(CCons::Head(wrapper)))
                }
                _ => Err(ReadError::InconsistentState),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Record))
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        let mut payload = match delegate {
            CCons::Head(Wrapped {
                mut payload,
                reader,
            }) => {
                if payload.state.3.is_some() {
                    return Err(ReadError::DuplicateField(Text::new("slot1")));
                } else {
                    payload.state.3 = Some(<T as StructuralReadable>::try_terminate(reader)?);
                }
                payload
            }
            CCons::Tail(CCons::Head(Wrapped {
                mut payload,
                reader,
            })) => {
                if payload.state.4.is_some() {
                    return Err(ReadError::DuplicateField(Text::new("slot2")));
                } else {
                    payload.state.4 = Some(<T as StructuralReadable>::try_terminate(reader)?);
                }
                payload
            }
            CCons::Tail(CCons::Tail(nil)) => nil.explode(),
        };
        payload.reading_slot = false;
        Ok(payload)
    }
}

impl<T: StructuralReadable> HeaderBuilder<OneOfEach<T>, OneOfEachFields<T>> {
    fn apply<P: PushPrimValue>(&mut self, push: P) -> Result<bool, ReadError> {
        if self.after_body {
            self.inner.apply_header(push)
        } else {
            push.apply(&mut self.inner.state.0, "header")?;
            self.after_body = true;
            Ok(self.inner.has_more())
        }
    }
}

impl<T: StructuralReadable> BodyReader for HeaderBuilder<OneOfEach<T>, OneOfEachFields<T>> {
    type Delegate = CCons<Wrapped<Self, T::Reader>, CCons<Wrapped<Self, T::Reader>, CNil>>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.apply(())
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        if !self.after_body {
            Err(ReadError::UnexpectedSlot)
        } else if self.inner.reading_slot {
            Err(ReadError::DoubleSlot)
        } else {
            self.inner.reading_slot = true;
            Ok(())
        }
    }

    fn push_record(mut self) -> Result<Self::Delegate, ReadError> {
        if !self.after_body {
            let wrapper = Wrapped {
                payload: self,
                reader: <T as StructuralReadable>::record_reader()?,
            };
            Ok(CCons::Head(wrapper))
        } else if self.inner.reading_slot {
            match self.inner.current_field.take() {
                Some(0) => {
                    let wrapper = Wrapped {
                        payload: self,
                        reader: <T as StructuralReadable>::record_reader()?,
                    };
                    Ok(CCons::Tail(CCons::Head(wrapper)))
                }
                _ => Err(ReadError::InconsistentState),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Record))
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            CCons::Head(Wrapped {
                mut payload,
                reader,
            }) => {
                if payload.inner.state.0.is_some() {
                    return Err(ReadError::InconsistentState);
                } else {
                    payload.inner.state.0 = Some(<T as StructuralReadable>::try_terminate(reader)?);
                }
                payload.after_body = true;
                Ok(payload)
            }
            CCons::Tail(CCons::Head(Wrapped {
                mut payload,
                reader,
            })) => {
                if payload.inner.state.1.is_some() {
                    return Err(ReadError::DuplicateField(Text::new("header_slot")));
                } else {
                    payload.inner.state.1 = Some(<T as StructuralReadable>::try_terminate(reader)?);
                }
                payload.inner.reading_slot = false;
                Ok(payload)
            }
            CCons::Tail(CCons::Tail(nil)) => nil.explode(),
        }
    }
}

#[test]
fn one_of_each() {
    use crate::form::structural::read::parser;
    let recon = "@OneOfEach(5, header_slot: 3)@attr(6) { slot1: 1, slot2: 2 }";
    let span = parser::Span::new(recon);
    let result = parser::parse_from_str::<OneOfEach<i32>>(span);
    match result {
        Ok(v) => assert_eq!(
            v,
            OneOfEach {
                header: 5,
                header_slot: 3,
                attr: 6,
                slot1: 1,
                slot2: 2,
            }
        ),
        Err(e) => panic!("{:?}", e),
    }
}

struct TupleStruct<T, U>(T, U);

impl<T: ValueReadable, U: ValueReadable> ValueReadable for TupleStruct<T, U> {}

type TupleStructFields<T, U> = (Option<T>, Option<U>);

impl<T: StructuralReadable, U: StructuralReadable> StructuralReadable for TupleStruct<T, U> {
    type Reader = Builder<TupleStruct<T, U>, TupleStructFields<T, U>>;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        Ok(Builder::default())
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        let Builder { mut state, .. } = reader;

        let mut missing = vec![];
        if state.0.is_none() {
            let on_missing = <T as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("header"));
            } else {
                state.0 = on_missing;
            }
        }
        if state.1.is_none() {
            let on_missing = <U as StructuralReadable>::on_absent();
            if on_missing.is_none() {
                missing.push(Text::new("header_slot"));
            } else {
                state.1 = on_missing;
            }
        }

        if let (Some(first), Some(second)) = state {
            Ok(TupleStruct(first, second))
        } else {
            Err(ReadError::MissingFields(missing))
        }
    }
}

impl<T: StructuralReadable, U: StructuralReadable> HeaderReader
    for Builder<TupleStruct<T, U>, TupleStructFields<T, U>>
{
    type Body = Self;
    type Delegate = HeaderBuilder<TupleStruct<T, U>, TupleStructFields<T, U>>;

    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        match name.borrow() {
            "TupleStruct" => Ok(HeaderBuilder::new(self, false)),
            ow => Err(ReadError::UnexpectedField(ow.into())),
        }
    }

    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError> {
        let HeaderBuilder { inner, .. } = delegate;
        Ok(inner)
    }

    fn start_body(mut self) -> Result<Self::Body, ReadError> {
        self.current_field = Some(0);
        Ok(self)
    }
}

impl<T: StructuralReadable, U: StructuralReadable> BodyReader
    for HeaderBuilder<TupleStruct<T, U>, TupleStructFields<T, U>>
{
    type Delegate = Never;

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        Err(ReadError::UnexpectedItem)
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        delegate.explode()
    }
}

impl<T: StructuralReadable, U: StructuralReadable>
    Builder<TupleStruct<T, U>, TupleStructFields<T, U>>
{
    fn apply<P: PushPrimValue>(&mut self, push: P) -> Result<bool, ReadError> {
        match self
            .current_field
            .take()
            .ok_or_else(|| ReadError::UnexpectedKind(push.kind()))?
        {
            0 => {
                push.apply(&mut self.state.0, "part1")?;
                self.current_field = Some(1);
                Ok(true)
            }
            1 => {
                push.apply(&mut self.state.1, "part2")?;
                self.current_field = None;
                Ok(false)
            }
            _ => Err(ReadError::UnexpectedItem),
        }
    }
}

impl<T: StructuralReadable, U: StructuralReadable> BodyReader
    for Builder<TupleStruct<T, U>, TupleStructFields<T, U>>
{
    type Delegate = CCons<Wrapped<Self, T::Reader>, CCons<Wrapped<Self, U::Reader>, CNil>>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.apply(())
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.apply(value)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        Err(ReadError::UnexpectedSlot)
    }

    fn push_record(mut self) -> Result<Self::Delegate, ReadError> {
        match self.current_field.take() {
            Some(0) => {
                let wrapper = Wrapped {
                    payload: self,
                    reader: <T as StructuralReadable>::record_reader()?,
                };
                Ok(CCons::Head(wrapper))
            }
            Some(1) => {
                let wrapper = Wrapped {
                    payload: self,
                    reader: <U as StructuralReadable>::record_reader()?,
                };
                Ok(CCons::Tail(CCons::Head(wrapper)))
            }
            _ => Err(ReadError::InconsistentState),
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            CCons::Head(Wrapped {
                mut payload,
                reader,
            }) => {
                if payload.state.0.is_some() {
                    return Err(ReadError::InconsistentState);
                } else {
                    payload.state.0 = Some(<T as StructuralReadable>::try_terminate(reader)?);
                }
                payload.current_field = Some(1);
                Ok(payload)
            }
            CCons::Tail(CCons::Head(Wrapped {
                mut payload,
                reader,
            })) => {
                if payload.state.1.is_some() {
                    return Err(ReadError::InconsistentState);
                } else {
                    payload.state.1 = Some(<U as StructuralReadable>::try_terminate(reader)?);
                }
                payload.current_field = None;
                Ok(payload)
            }
            CCons::Tail(CCons::Tail(nil)) => nil.explode(),
        }
    }
}
*/
