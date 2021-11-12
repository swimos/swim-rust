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

#[cfg(test)]
mod tests;

use crate::structural::read::event::{NumericValue, ReadEvent};
use crate::structural::read::recognizer::Recognizer;
use crate::structural::read::ReadError;
use crate::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use std::borrow::Cow;
use swim_model::bigint::{BigInt, BigUint};

/// Bridge to forward writes to a [`StructuralWriter`] instance to the builder methods
/// on a [`StructuralReadable`] type.
pub struct RecognizerBridge<R>(R);

impl<R> RecognizerBridge<R> {
    pub fn new(rec: R) -> Self {
        RecognizerBridge(rec)
    }
}

struct SubRecognizerBridge<'a, R>(&'a mut R);

impl<'a, R> SubRecognizerBridge<'a, R> {
    fn new(rec: &'a mut R) -> Self {
        SubRecognizerBridge(rec)
    }
}

impl<R: Recognizer> RecognizerBridge<R> {
    fn feed_single(self, event: ReadEvent<'_>) -> Result<R::Target, ReadError> {
        let RecognizerBridge(mut rec) = self;
        rec.feed_event(event)
            .or_else(move || rec.try_flush())
            .unwrap_or(Err(ReadError::IncompleteRecord))
    }
}

impl<'a, R: Recognizer> SubRecognizerBridge<'a, R> {
    fn feed_single(self, event: ReadEvent<'_>) -> Result<(), ReadError> {
        let SubRecognizerBridge(rec) = self;
        if let Some(Err(e)) = rec.feed_event(event) {
            Err(e)
        } else {
            Ok(())
        }
    }
}

impl<R: Recognizer> PrimitiveWriter for RecognizerBridge<R> {
    type Repr = R::Target;
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Extant)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Int(value.into())))
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Int(value)))
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::UInt(value.into())))
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::UInt(value)))
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Float(value)))
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Boolean(value))
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::BigInt(value)))
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::BigUint(value)))
    }

    fn write_text<L: Label>(self, value: L) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::TextValue(Cow::Borrowed(value.as_ref())))
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Blob(blob))
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Blob(value.to_vec()))
    }
}

impl<'a, R: Recognizer> PrimitiveWriter for SubRecognizerBridge<'a, R> {
    type Repr = ();
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Extant)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Int(value.into())))
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Int(value)))
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::UInt(value.into())))
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::UInt(value)))
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::Float(value)))
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Boolean(value))
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::BigInt(value)))
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Number(NumericValue::BigUint(value)))
    }

    fn write_text<L: Label>(self, value: L) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::TextValue(Cow::Borrowed(value.as_ref())))
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Blob(blob))
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ReadEvent::Blob(value.to_vec()))
    }
}

impl<R: Recognizer> StructuralWriter for RecognizerBridge<R> {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<R: Recognizer> HeaderWriter for RecognizerBridge<R> {
    type Repr = R::Target;
    type Error = ReadError;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec) = &mut self;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::StartAttribute(name)) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(rec);
        value.write_with(delegate)?;
        match rec.feed_event(ReadEvent::EndAttribute) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        mut self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec) = &mut self;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::StartAttribute(Cow::Owned(name.into()))) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(rec);
        value.write_into(delegate)?;
        match rec.feed_event(ReadEvent::EndAttribute) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(
        mut self,
        _kind: RecordBodyKind,
        _num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let RecognizerBridge(rec) = &mut self;
        match rec.feed_event(ReadEvent::StartBody) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }
}

impl<R: Recognizer> BodyWriter for RecognizerBridge<R> {
    type Repr = R::Target;
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let RecognizerBridge(mut rec) = self;
        let delegate = SubRecognizerBridge::new(&mut rec);
        key.write_with(delegate)?;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::Slot) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(&mut rec);
        value.write_with(delegate)?;
        Ok(RecognizerBridge::new(rec))
    }

    fn write_value_into<V: StructuralWritable>(mut self, value: V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let RecognizerBridge(mut rec) = self;
        let delegate = SubRecognizerBridge::new(&mut rec);
        key.write_into(delegate)?;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::Slot) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(&mut rec);
        value.write_into(delegate)?;
        Ok(RecognizerBridge::new(rec))
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let RecognizerBridge(mut rec) = self;
        match rec
            .feed_event(ReadEvent::EndRecord)
            .or_else(move || rec.try_flush())
        {
            Some(r) => r,
            _ => Err(ReadError::IncompleteRecord),
        }
    }
}

impl<'a, R: Recognizer> StructuralWriter for SubRecognizerBridge<'a, R> {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<'a, R: Recognizer> HeaderWriter for SubRecognizerBridge<'a, R> {
    type Repr = ();
    type Error = ReadError;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::StartAttribute(name)) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        match rec.feed_event(ReadEvent::EndAttribute) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        mut self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::StartAttribute(Cow::Owned(name.into()))) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        match rec.feed_event(ReadEvent::EndAttribute) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(
        mut self,
        _kind: RecordBodyKind,
        _num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        match rec.feed_event(ReadEvent::StartBody) {
            Some(Err(e)) => Err(e),
            _ => Ok(self),
        }
    }
}

impl<'a, R: Recognizer> BodyWriter for SubRecognizerBridge<'a, R> {
    type Repr = ();
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        key.write_with(delegate)?;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::Slot) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(mut self, value: V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        key.write_into(delegate)?;
        if let Some(Err(e)) = rec.feed_event(ReadEvent::Slot) {
            return Err(e);
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let SubRecognizerBridge(rec) = self;
        match rec.feed_event(ReadEvent::EndRecord) {
            Some(Err(e)) => Err(e),
            _ => Ok(()),
        }
    }
}
