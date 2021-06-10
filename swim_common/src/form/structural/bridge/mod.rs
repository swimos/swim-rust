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

use crate::form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::marker::PhantomData;
use crate::form::structural::read::improved::Recognizer;
use crate::form::structural::read::parser::{ParseEvent, NumericLiteral};
use crate::model::ValueKind;
use crate::form::structural::read::ReadError;

/// Bridge to forward writes to a [`StructuralWriter`] instance to the builder methods
/// on a [`StructuralReadable`] type.
pub struct RecognizerBridge<T, R>(R, PhantomData<fn() -> T>);

impl<T, R> RecognizerBridge<T, R> {

    pub fn new(rec: R) -> Self {
        RecognizerBridge(rec, PhantomData)
    }

}

struct SubRecognizerBridge<'a, T, R>(&'a mut R, PhantomData<fn() -> T>);

impl<'a, T, R> SubRecognizerBridge<'a, T, R> {

    fn new(rec: &'a mut R) -> Self {
        SubRecognizerBridge(rec, PhantomData)
    }

}

impl<T, R: Recognizer<T>> RecognizerBridge<T, R> {

    fn feed_single(self, event: ParseEvent<'_>, kind: ValueKind) -> Result<T, ReadError> {
        let RecognizerBridge(mut rec, _) = self;
        rec.feed(event).or_else(move || rec.flush())
            .unwrap_or(Err(ReadError::UnexpectedKind(kind)))
    }

}

impl<'a, T, R: Recognizer<T>> SubRecognizerBridge<'a, T, R> {

    fn feed_single(self, event: ParseEvent<'_>) -> Result<(), ReadError> {
        let SubRecognizerBridge(rec, _) = self;
        if let Some(Err(e)) = rec.feed(event) {
            Err(e)
        } else {
            Ok(())
        }
    }

}

impl<T, R: Recognizer<T>> PrimitiveWriter for RecognizerBridge<T, R> {
    type Repr = T;
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Extant, ValueKind::Extant)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Int(value.into())), ValueKind::Int32)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Int(value)), ValueKind::Int64)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::UInt(value.into())), ValueKind::UInt32)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::UInt(value)), ValueKind::UInt64)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Float(value)), ValueKind::Float64)
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Boolean(value), ValueKind::Boolean)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::BigInt(value)), ValueKind::BigInt)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::BigUint(value)), ValueKind::BigUint)
    }

    fn write_text<L: Label>(self, value: L) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::TextValue(Cow::Borrowed(value.as_ref())), ValueKind::Text)
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Blob(blob), ValueKind::Data)
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Blob(value.to_vec()), ValueKind::Data)
    }
}

impl<'a, T, R: Recognizer<T>> PrimitiveWriter for SubRecognizerBridge<'a, T, R> {
    type Repr = ();
    type Error = ReadError;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Extant)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Int(value.into())))
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Int(value)))
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::UInt(value.into())))
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::UInt(value)))
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::Float(value)))
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Boolean(value))
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::BigInt(value)))
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Number(NumericLiteral::BigUint(value)))
    }

    fn write_text<L: Label>(self, value: L) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::TextValue(Cow::Borrowed(value.as_ref())))
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Blob(blob))
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        self.feed_single(ParseEvent::Blob(value.to_vec()))
    }
}

impl<T, R: Recognizer<T>> StructuralWriter for RecognizerBridge<T, R> {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<T, R: Recognizer<T>> HeaderWriter for RecognizerBridge<T, R> {
    type Repr = T;
    type Error = ReadError;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(mut self, name: Cow<'_, str>, value: &V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartAttribute(name)) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(rec);
        value.write_with(delegate)?;
        match rec.feed(ParseEvent::EndAttribute) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(mut self, name: L, value: V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartAttribute(Cow::Owned(name.into()))) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(rec);
        value.write_into(delegate)?;
        match rec.feed(ParseEvent::EndAttribute) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(mut self, _kind: RecordBodyKind, _num_items: usize) -> Result<Self::Body, Self::Error> {
        let RecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartBody) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }
}

impl<T, R: Recognizer<T>> BodyWriter for RecognizerBridge<T, R> {
    type Repr = T;
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(self, key: &K, value: &V) -> Result<Self, Self::Error> {
        let RecognizerBridge(mut rec, _) = self;
        let delegate = SubRecognizerBridge::new(&mut rec);
        key.write_with(delegate)?;
        match rec.feed(ParseEvent::Slot) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(&mut rec);
        value.write_with(delegate)?;
        Ok(RecognizerBridge::new(rec))
    }

    fn write_value_into<V: StructuralWritable>(mut self, value: V) -> Result<Self, Self::Error> {
        let RecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(self, key: K, value: V) -> Result<Self, Self::Error> {
        let RecognizerBridge(mut rec, _) = self;
        let delegate = SubRecognizerBridge::new(&mut rec);
        key.write_into(delegate)?;
        match rec.feed(ParseEvent::Slot) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(&mut rec);
        value.write_into(delegate)?;
        Ok(RecognizerBridge::new(rec))
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let RecognizerBridge(mut rec, _) = self;
        match rec.feed(ParseEvent::EndRecord).or_else(move || rec.flush()) {
            Some(r) => r,
            _ => {
                Err(ReadError::IncompleteRecord)
            }
        }
    }
}

impl<'a, T, R: Recognizer<T>> StructuralWriter for SubRecognizerBridge<'a, T, R> {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<'a, T, R: Recognizer<T>> HeaderWriter for SubRecognizerBridge<'a, T, R> {
    type Repr = ();
    type Error = ReadError;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(mut self, name: Cow<'_, str>, value: &V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartAttribute(name)) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        match rec.feed(ParseEvent::EndAttribute) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self)
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(mut self, name: L, value: V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartAttribute(Cow::Owned(name.into()))) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        match rec.feed(ParseEvent::EndAttribute) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_into(self)
    }

    fn complete_header(mut self, _kind: RecordBodyKind, _num_items: usize) -> Result<Self::Body, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        match rec.feed(ParseEvent::StartBody) {
            Some(Err(e)) => {
                Err(e)
            }
            _ => Ok(self)
        }
    }
}

impl<'a, T, R: Recognizer<T>> BodyWriter for SubRecognizerBridge<'a, T, R> {
    type Repr = ();
    type Error = ReadError;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(mut self, key: &K, value: &V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        key.write_with(delegate)?;
        match rec.feed(ParseEvent::Slot) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_with(delegate)?;
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(mut self, value: V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(mut self, key: K, value: V) -> Result<Self, Self::Error> {
        let SubRecognizerBridge(rec, _) = &mut self;
        let delegate = SubRecognizerBridge::new(*rec);
        key.write_into(delegate)?;
        match rec.feed(ParseEvent::Slot) {
            Some(Err(e)) => {
                return Err(e);
            }
            _ => {}
        }
        let delegate = SubRecognizerBridge::new(*rec);
        value.write_into(delegate)?;
        Ok(self)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let SubRecognizerBridge(rec, _) = self;
        match rec.feed(ParseEvent::EndRecord) {
            Some(Err(e)) => Err(e),
            _ => Ok(())
        }
    }
}
