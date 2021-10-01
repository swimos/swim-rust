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

use crate::structural::read::event::ReadEvent;
use crate::structural::read::recognizer::{
    FirstOf, NamedFieldsRecognizer, Recognizer, RecognizerReadable, SimpleAttrBody,
};
use crate::structural::read::ReadError;
use crate::structural::write::{BodyWriter, HeaderWriter};
use crate::structural::write::{
    PrimitiveWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use std::borrow::Cow;
use std::result::Result::Err;
use swim_model::Text;

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
type GeneralAttrRec<S, T> = FirstOf<GeneralRec<S, T>, SimpleAttrBody<GeneralRec<S, T>>>;

fn general_select_field(name: &str) -> Option<u32> {
    match name {
        "first" => Some(0),
        "second" => Some(1),
        _ => None,
    }
}

fn general_select<'a, S, RS: Recognizer<Target = S>, T, RT: Recognizer<Target = T>>(
    state: &mut (Option<S>, Option<T>, RS, RT),
    index: u32,
    input: ReadEvent<'a>,
) -> Option<Result<(), ReadError>> {
    let (first, second, first_rec, second_rec) = state;
    match index {
        0 => {
            if first.is_some() {
                Some(Err(ReadError::DuplicateField(Text::new("first"))))
            } else {
                let r = first_rec.feed_event(input)?;
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
                let r = second_rec.feed_event(input)?;
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
    RS: Recognizer<Target = S>,
    T: RecognizerReadable,
    RT: Recognizer<Target = T>,
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
    RS: Recognizer<Target = S>,
    T: RecognizerReadable,
    RT: Recognizer<Target = T>,
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
    type BodyRec = Self::Rec;

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

    fn make_body_recognizer() -> Self::BodyRec {
        Self::make_recognizer()
    }
}

impl<S: StructuralWritable, T: StructuralWritable> StructuralWritable for GeneralType<S, T> {
    fn num_attributes(&self) -> usize {
        0
    }

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
    fn num_attributes(&self) -> usize {
        2
    }

    fn write_with<W: StructuralWriter>(&self, writer: W) -> Result<W::Repr, W::Error> {
        let mut rec_writer = writer.record(self.num_attributes())?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("StructuralWritable"), &self.header)?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("attr"), &self.attr)?;
        let mut body_writer = rec_writer.complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot(&"slot", &self.slot)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(self, writer: W) -> Result<W::Repr, W::Error> {
        let mut rec_writer = writer.record(self.num_attributes())?;
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
    fn num_attributes(&self) -> usize {
        0
    }

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
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let mut rec_writer = writer.record(self.num_attributes())?;
        rec_writer = rec_writer.write_attr_into("StructuralWritable", HeaderView(self))?;
        rec_writer = rec_writer.write_attr(Cow::Borrowed("attr"), &self.attr)?;
        let mut body_writer = rec_writer.complete_header(RecordBodyKind::MapLike, 1)?;
        body_writer = body_writer.write_slot(&"slot", &self.slot)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        self.write_with(writer)
    }
}
