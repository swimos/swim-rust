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

use crate::form::structural::write::{
    BodyWriter, HeaderWriter, PrimitiveWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};

/// Slots that have been lifted to the header of a record.
pub struct HeaderSlots<T, Tail> {
    key: &'static str,
    value: T,
    tail: Tail,
}

/// Base case for the HList where there are no slots.
pub struct NoSlots;

impl NoSlots {
    /// Prepend an additional slot to the HList.
    pub fn prepend<U>(self, key: &'static str, value: U) -> HeaderSlots<U, Self> {
        HeaderSlots {
            key,
            value,
            tail: self,
        }
    }
}

impl<T, Tail> HeaderSlots<T, Tail> {
    /// Prepend an additional slot to the HList.
    pub fn prepend<U>(self, key: &'static str, value: U) -> HeaderSlots<U, Self> {
        HeaderSlots {
            key,
            value,
            tail: self,
        }
    }

    /// Complete the header with an initial value.
    pub fn with_body<U>(self, body: U) -> HeaderWithBody<U, Self> {
        HeaderWithBody { body, slots: self }
    }

    /// Complete a header consisting only of slots.
    pub fn simple(self) -> SimpleHeader<Self> {
        SimpleHeader(self)
    }
}

/// Header consisting only of slots.
pub struct SimpleHeader<A>(A);

/// A header with an intial value followed by a number of slots. If there were no slots, the
/// initial value could form the body of the header on its own and this structure would not be
/// necessary.
pub struct HeaderWithBody<T, S> {
    body: T,
    slots: S,
}

impl<T, S: AppendHeaders> HeaderWithBody<T, S> {
    fn len(&self) -> usize {
        S::LEN + 1
    }
}

/// This trait allows all of the types in this crate to be used with a unified interface
/// in the the macro-derived implementations.
pub trait AppendHeaders {
    /// The number of items that will be written into the attribute body.
    const LEN: usize;

    /// Write the values into the attribute body.
    fn append<B: BodyWriter>(&self, writer: B) -> Result<B, B::Error>;

    /// Write the values into the attribute body, potentially consuming them.
    fn append_into<B: BodyWriter>(self, writer: B) -> Result<B, B::Error>;
}

impl AppendHeaders for NoSlots {
    const LEN: usize = 0;

    fn append<B: BodyWriter>(&self, writer: B) -> Result<B, B::Error> {
        Ok(writer)
    }

    fn append_into<B: BodyWriter>(self, writer: B) -> Result<B, <B as BodyWriter>::Error> {
        Ok(writer)
    }
}

impl<T, Tail> AppendHeaders for HeaderSlots<T, Tail>
where
    T: StructuralWritable,
    Tail: AppendHeaders,
{
    const LEN: usize = Tail::LEN + 1;

    fn append<B: BodyWriter>(&self, mut writer: B) -> Result<B, <B as BodyWriter>::Error> {
        let HeaderSlots { key, value, tail } = self;
        writer = writer.write_slot(key, value)?;
        tail.append(writer)
    }

    fn append_into<B: BodyWriter>(self, mut writer: B) -> Result<B, <B as BodyWriter>::Error> {
        let HeaderSlots { key, value, tail } = self;
        writer = writer.write_slot_into(key, value)?;
        tail.append_into(writer)
    }
}

impl<T, S> StructuralWritable for HeaderWithBody<T, S>
where
    T: StructuralWritable,
    S: AppendHeaders,
{
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let HeaderWithBody { body, slots } = self;

        let mut body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::Mixed, self.len())?;

        body_writer = body_writer.write_value(body)?;
        body_writer = slots.append(body_writer)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let len = self.len();

        let HeaderWithBody { body, slots } = self;

        let mut body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::Mixed, len)?;

        body_writer = body_writer.write_value_into(body)?;
        body_writer = slots.append_into(body_writer)?;
        body_writer.done()
    }
}

impl<A> StructuralWritable for SimpleHeader<A>
where
    A: AppendHeaders,
{
    fn num_attributes(&self) -> usize {
        0
    }

    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let SimpleHeader(inner) = self;
        let body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::MapLike, A::LEN)?;
        inner.append(body_writer)?.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let SimpleHeader(inner) = self;
        let body_writer = writer
            .record(0)?
            .complete_header(RecordBodyKind::MapLike, A::LEN)?;
        inner.append_into(body_writer)?.done()
    }
}
