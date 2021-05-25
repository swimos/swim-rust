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

use crate::form::structural::generic::coproduct::{CCons, CNil};
use crate::form::structural::read::builder::prim::PushPrimValue;
use crate::form::structural::read::builder::{
    AttrReader, Builder, HeaderBuilder, NoAttributes, Wrapped,
};
use crate::form::structural::read::{
    BodyReader, HeaderReader, ReadError, StructuralReadable, ValueReadable,
};
use crate::form::structural::write::{
    BodyWriter, HeaderWriter, PrimitiveWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use crate::model::text::Text;
use crate::model::ValueKind;
use either::Either;
use num_bigint::{BigInt, BigUint};
use std::borrow::Borrow;
use std::borrow::Cow;

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

type GeneralTypeFields<S, T> = (Option<S>, Option<T>);

impl<S: StructuralReadable, T: StructuralReadable> ValueReadable for GeneralType<S, T> {}

impl<S: StructuralReadable, T: StructuralReadable> StructuralReadable for GeneralType<S, T> {
    type Reader = Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>;

    fn record_reader() -> Result<Self::Reader, ReadError> {
        Ok(Builder::default())
    }

    fn try_terminate(reader: <Self::Reader as HeaderReader>::Body) -> Result<Self, ReadError> {
        let Builder { state, .. } = reader;
        match state {
            (Some(first), Some(second), ..) => Ok(GeneralType { first, second }),
            (_, Some(second), ..) => {
                if let Some(first) = S::on_absent() {
                    Ok(GeneralType { first, second })
                } else {
                    Err(ReadError::MissingFields(vec![Text::new("first")]))
                }
            }
            (Some(first), ..) => {
                if let Some(second) = T::on_absent() {
                    Ok(GeneralType { first, second })
                } else {
                    Err(ReadError::MissingFields(vec![Text::new("second")]))
                }
            }
            _ => S::on_absent()
                .and_then(|first| T::on_absent().map(|second| GeneralType { first, second }))
                .ok_or_else(|| {
                    ReadError::MissingFields(vec![Text::new("first"), Text::new("second")])
                }),
        }
    }
}

impl<S, T> NoAttributes for Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
where
    S: StructuralReadable,
    T: StructuralReadable,
{
}

impl<S: StructuralReadable, T: StructuralReadable>
    Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
{
    fn push_prim<V, F1, F2>(
        &mut self,
        f1: F1,
        f2: F2,
        value: V,
        kind: ValueKind,
    ) -> Result<bool, ReadError>
    where
        F1: FnOnce(V) -> Result<S, ReadError>,
        F2: FnOnce(V) -> Result<T, ReadError>,
    {
        let (first, second) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = f1(value)?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = f2(value)?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                Some(_) => Err(ReadError::UnexpectedSlot),
                _ => Err(ReadError::UnexpectedKind(ValueKind::Extant)),
            }
        } else {
            Err(ReadError::UnexpectedKind(kind))
        }
    }
}

impl<S, T> BodyReader for Builder<GeneralType<S, T>, GeneralTypeFields<S, T>>
where
    S: StructuralReadable,
    T: StructuralReadable,
{
    type Delegate = Either<Wrapped<Self, S::Reader>, Wrapped<Self, T::Reader>>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        let (first, second) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = S::read_extant()?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = T::read_extant()?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                Some(_) => Err(ReadError::UnexpectedSlot),
                _ => Err(ReadError::UnexpectedKind(ValueKind::Extant)),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Extant))
        }
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.push_prim(S::read_i32, T::read_i32, value, ValueKind::Int32)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.push_prim(S::read_i64, T::read_i64, value, ValueKind::Int64)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.push_prim(S::read_u32, T::read_u32, value, ValueKind::UInt32)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.push_prim(S::read_u64, T::read_u64, value, ValueKind::UInt64)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.push_prim(S::read_f64, T::read_f64, value, ValueKind::Float64)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.push_prim(S::read_bool, T::read_bool, value, ValueKind::Boolean)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.push_prim(S::read_big_int, T::read_big_int, value, ValueKind::BigInt)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.push_prim(
            S::read_big_uint,
            T::read_big_uint,
            value,
            ValueKind::BigUint,
        )
    }

    fn push_text<'a>(&mut self, value: Cow<'a, str>) -> Result<bool, ReadError> {
        let (first, second) = &mut self.state;

        if self.reading_slot {
            self.reading_slot = false;
            match self.current_field.take() {
                Some(0) => {
                    let value = S::read_text(value)?;
                    if first.is_some() {
                        Err(ReadError::DuplicateField(Text::new("first")))
                    } else {
                        *first = Some(value);
                        Ok(second.is_none())
                    }
                }
                Some(1) => {
                    let value = T::read_text(value)?;
                    if second.is_some() {
                        Err(ReadError::DuplicateField(Text::new("second")))
                    } else {
                        *second = Some(value);
                        Ok(first.is_none())
                    }
                }
                _ => Err(ReadError::UnexpectedSlot),
            }
        } else {
            match value.as_ref() {
                "first" => {
                    self.current_field = Some(0);
                    Ok(first.is_none())
                }
                "second" => {
                    self.current_field = Some(1);
                    Ok(second.is_none())
                }
                _ => Err(ReadError::UnexpectedKind(ValueKind::Text)),
            }
        }
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.push_prim(S::read_blob, T::read_blob, value, ValueKind::Data)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        self.reading_slot = true;
        Ok(())
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        if self.reading_slot {
            match &self.current_field {
                Some(0) => Ok(Either::Left(Wrapped {
                    payload: self,
                    reader: S::record_reader()?,
                })),
                Some(1) => Ok(Either::Right(Wrapped {
                    payload: self,
                    reader: T::record_reader()?,
                })),
                _ => Err(ReadError::UnexpectedSlot),
            }
        } else {
            Err(ReadError::UnexpectedKind(ValueKind::Record))
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            Either::Left(Wrapped {
                mut payload,
                reader,
            }) => {
                let (first, _) = &mut payload.state;
                *first = Some(S::try_terminate(reader)?);
                Ok(payload)
            }
            Either::Right(Wrapped {
                mut payload,
                reader,
            }) => {
                let (_, second) = &mut payload.state;
                *second = Some(T::try_terminate(reader)?);
                Ok(payload)
            }
        }
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
