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

use crate::form::structural::read::{BodyReader, HeaderReader, Never, ReadError};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::marker::PhantomData;

/// Utility type used in the automatic derivation of [`StructuralReadable`] instances.
/// It should never be necessary to use this type directly. This wraps a generic
/// state type, built from tuples and [`Option`] so that it can be used to produce
/// a unique implementation of [`HeaderReader`] for a type without having to generate
/// new structs.
pub struct Builder<T, State> {
    _type: PhantomData<fn(State) -> T>,
    pub state: State,
    pub current_field: Option<usize>,
    pub reading_slot: bool,
}

impl<T, State: Default> Default for Builder<T, State> {
    fn default() -> Self {
        Builder {
            _type: PhantomData,
            state: State::default(),
            current_field: None,
            reading_slot: false,
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

    fn read_attribute<'a>(self, name: Cow<'a, str>) -> Result<Self::Delegate, ReadError> {
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
/// payload (to be extracted after the complete). This is to facilitate nested, stateful
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

    fn read_attribute<'a>(self, name: Cow<'a, str>) -> Result<Self::Delegate, ReadError> {
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

    fn push_text<'a>(&mut self, value: Cow<'a, str>) -> Result<bool, ReadError> {
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
