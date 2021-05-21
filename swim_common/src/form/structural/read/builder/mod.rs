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

use crate::form::structural::read::{
    BodyOf, BodyReader, HeaderReader, ReadError, ReaderOf, StructuralReadable,
};
use crate::model::ValueKind;
use either::Either;
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::marker::PhantomData;
use utilities::never::Never;

/// Utility type used in the automatic derivation of [`StructuralReadable`] instances.
/// It should never be necessary to use this type directly. This wraps a generic
/// state type, built from tuples and [`Option`] so that it can be used to produce
/// a unique implementation of [`HeaderReader`] for a type without having to generate
/// new structs.
pub struct Builder<T, State> {
    _type: PhantomData<fn(State) -> T>,
    pub read_tag: bool,
    pub state: State,
    pub current_field: Option<usize>,
    pub reading_slot: bool,
}

impl<T, State: Default> Default for Builder<T, State> {
    fn default() -> Self {
        Builder {
            _type: PhantomData,
            read_tag: false,
            state: Default::default(),
            current_field: None,
            reading_slot: false,
        }
    }
}

/// Extension to [`Builder`] for reading types with a complex tag attribute.
pub struct HeaderBuilder<T, State> {
    pub inner: Builder<T, State>,
    /// Rercords when the tag body has been observed (this will be the first item in the record).
    pub after_body: bool,
}

impl<T, State> HeaderBuilder<T, State> {
    pub(crate) fn new(inner: Builder<T, State>, has_body: bool) -> Self {
        HeaderBuilder {
            inner,
            after_body: !has_body,
        }
    }
}

/// Many types do not require attributes. Implementing this empty trait will prove
/// a default implementation of [`HeaderReader`] that will return an error if any
/// attributes are read.
pub trait NoAttributes {}

impl<B: NoAttributes + BodyReader> HeaderReader for B {
    type Body = Self;
    type Delegate = Never;

    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        Err(ReadError::UnexpectedAttribute(name.into()))
    }

    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError> {
        delegate.explode()
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        Ok(self)
    }
}

/// This type wraps instances of [`HeaderReader`] and [`BodyReader`] with an additional
/// payload (to be extracted after they complete). This is to facilitate nested, stateful
/// readers where the type of the reader changes as the depth increases.
pub struct Wrapped<Payload, Reader> {
    pub payload: Payload,
    pub reader: Reader,
}

impl<Payload, Reader> Wrapped<Payload, Reader> {
    fn try_map<T, F>(self, f: F) -> Result<Wrapped<Payload, T>, ReadError>
    where
        F: FnOnce(Reader) -> Result<T, ReadError>,
    {
        let Wrapped { payload, reader } = self;
        let sub_reader = f(reader)?;
        Ok(Wrapped {
            payload,
            reader: sub_reader,
        })
    }
}

impl<Payload, Reader> HeaderReader for Wrapped<Payload, Reader>
where
    Reader: HeaderReader,
{
    type Body = Wrapped<Payload, Reader::Body>;
    type Delegate = Wrapped<Payload, Reader::Delegate>;

    fn read_attribute(self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        self.try_map(move |reader| reader.read_attribute(name))
    }

    fn restore(delegate: Self::Delegate) -> Result<Self, ReadError> {
        delegate.try_map(Reader::restore)
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        self.try_map(HeaderReader::start_body)
    }
}

impl<Payload, Reader> BodyReader for Wrapped<Payload, Reader>
where
    Reader: BodyReader,
{
    type Delegate = Wrapped<Payload, Reader::Delegate>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.reader.push_extant()
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.reader.push_i32(value)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.reader.push_i64(value)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.reader.push_u32(value)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.reader.push_u64(value)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.reader.push_f64(value)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.reader.push_bool(value)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.reader.push_big_int(value)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.reader.push_big_uint(value)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        self.reader.push_text(value)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.reader.push_blob(value)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        self.reader.start_slot()
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        self.try_map(BodyReader::push_record)
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        delegate.try_map(BodyReader::restore)
    }
}

pub enum AttrReader<T: StructuralReadable> {
    Init(Option<T>),
    Record(BodyOf<T>),
}

impl<T: StructuralReadable> Default for AttrReader<T> {
    fn default() -> Self {
        AttrReader::Init(None)
    }
}

impl<T: StructuralReadable> AttrReader<T> {
    fn read_prim_attr<U, F, G>(&mut self, v: U, f: F, g: G) -> Result<bool, ReadError>
    where
        F: FnOnce(U) -> Result<T, ReadError>,
        G: FnOnce(&mut BodyOf<T>, U) -> Result<bool, ReadError>,
    {
        match self {
            AttrReader::Init(value @ None) => {
                *value = Some(f(v)?);
                Ok(false)
            }
            AttrReader::Init(_) => Err(ReadError::InconsistentState),
            AttrReader::Record(body_reader) => g(body_reader, v),
        }
    }

    pub fn try_get_value(self) -> Result<T, ReadError> {
        match self {
            AttrReader::Init(Some(v)) => Ok(v),
            AttrReader::Init(_) => Err(ReadError::UnexpectedKind(ValueKind::Extant)),
            AttrReader::Record(reader) => T::try_terminate(reader),
        }
    }
}

impl<T: StructuralReadable> BodyReader for AttrReader<T> {
    type Delegate = Either<ReaderOf<T>, <BodyOf<T> as BodyReader>::Delegate>;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.read_prim_attr(
            (),
            |_| T::read_extant(),
            |reader, _| <BodyOf<T>>::push_extant(reader),
        )
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_i32, <BodyOf<T>>::push_i32)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_i64, <BodyOf<T>>::push_i64)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_u32, <BodyOf<T>>::push_u32)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_u64, <BodyOf<T>>::push_u64)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_f64, <BodyOf<T>>::push_f64)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_bool, <BodyOf<T>>::push_bool)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_big_int, <BodyOf<T>>::push_big_int)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_big_uint, <BodyOf<T>>::push_big_uint)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        match self {
            AttrReader::Init(output @ None) => {
                let r = T::read_text(value.clone());
                match r {
                    Ok(v) => {
                        *output = Some(v);
                        Ok(false)
                    }
                    Err(ReadError::UnexpectedKind(_)) => {
                        let mut body_reader = T::record_reader()?.start_body()?;
                        let p = body_reader.push_text(value)?;
                        *self = AttrReader::Record(body_reader);
                        Ok(p)
                    }
                    Err(e) => Err(e),
                }
            }
            AttrReader::Init(_) => Err(ReadError::UnexpectedKind(ValueKind::Record)),
            AttrReader::Record(body_reader) => body_reader.push_text(value),
        }
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.read_prim_attr(value, T::read_blob, <BodyOf<T>>::push_blob)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        if let AttrReader::Record(body_reader) = self {
            <BodyOf<T>>::start_slot(body_reader)
        } else {
            Err(ReadError::UnexpectedSlot)
        }
    }

    fn push_record(self) -> Result<Self::Delegate, ReadError> {
        match self {
            AttrReader::Init(None) => Ok(Either::Left(T::record_reader()?)),
            AttrReader::Init(_) => Err(ReadError::UnexpectedKind(ValueKind::Record)),
            AttrReader::Record(body_reader) => Ok(Either::Right(body_reader.push_record()?)),
        }
    }

    fn restore(delegate: <Self::Delegate as HeaderReader>::Body) -> Result<Self, ReadError> {
        match delegate {
            Either::Left(reader) => Ok(AttrReader::Init(Some(T::try_terminate(reader)?))),
            Either::Right(body_reader) => {
                Ok(AttrReader::Record(<BodyOf<T>>::restore(body_reader)?))
            }
        }
    }
}

pub mod prim {
    use crate::form::structural::read::{ReadError, ValueReadable};
    use crate::model::text::Text;
    use crate::model::ValueKind;
    use num_bigint::{BigInt, BigUint};
    use std::borrow::{Borrow, Cow};

    /// This trait aids in the macro derivation fo [`StructuralReadable`]. It should not
    /// generally need to be referred to in normal code.
    pub trait PushPrimValue: Sized {
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError>;

        #[inline]
        fn apply<T: ValueReadable>(
            self,
            location: &mut Option<T>,
            name: &'static str,
        ) -> Result<(), ReadError> {
            let value = self.try_to_value()?;
            if location.is_some() {
                Err(ReadError::DuplicateField(Text::new(name)))
            } else {
                *location = Some(value);
                Ok(())
            }
        }

        fn kind(&self) -> ValueKind;

        #[inline]
        fn text(&self) -> Option<&str> {
            None
        }
    }

    impl PushPrimValue for () {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_extant()
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Extant
        }
    }

    impl PushPrimValue for i32 {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_i32(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Int32
        }
    }

    impl PushPrimValue for i64 {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_i64(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Int64
        }
    }

    impl PushPrimValue for u32 {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_u32(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::UInt32
        }
    }

    impl PushPrimValue for u64 {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_u64(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::UInt64
        }
    }

    impl PushPrimValue for f64 {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_f64(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Float64
        }
    }

    impl PushPrimValue for bool {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_bool(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Boolean
        }
    }

    impl PushPrimValue for BigInt {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_big_int(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::BigInt
        }
    }

    impl PushPrimValue for BigUint {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_big_uint(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::BigUint
        }
    }

    impl PushPrimValue for Vec<u8> {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_blob(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Data
        }
    }

    impl<'a> PushPrimValue for Cow<'a, str> {
        #[inline]
        fn try_to_value<T: ValueReadable>(self) -> Result<T, ReadError> {
            T::read_text(self)
        }

        #[inline]
        fn kind(&self) -> ValueKind {
            ValueKind::Text
        }

        #[inline]
        fn text(&self) -> Option<&str> {
            Some(self.borrow())
        }
    }
}
