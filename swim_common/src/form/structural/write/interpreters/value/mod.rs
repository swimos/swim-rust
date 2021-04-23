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
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, StructuralWritable, StructuralWriter,
};
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Attr, Item, Value};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::convert::Infallible;

/// [`StructuralWriter`] that constructs [`Value`] instances representing the
/// strucuture that is described.
#[derive(Default)]
pub struct ValueInterpreter(Option<RecordBuilder>);

impl ValueInterpreter {
    fn get_attrs(&mut self) -> &mut Vec<Attr> {
        let ValueInterpreter(builder) = self;
        &mut builder.get_or_insert_with(RecordBuilder::default).attrs
    }

    fn get_items(&mut self) -> &mut Vec<Item> {
        let ValueInterpreter(builder) = self;
        &mut builder.get_or_insert_with(RecordBuilder::default).items
    }

    fn with_delegate_body(self, body: Value) -> Value {
        let ValueInterpreter(builder) = self;
        match builder {
            Some(RecordBuilder { mut attrs, .. }) if !attrs.is_empty() => {
                if let Value::Record(more_attrs, items) = body {
                    attrs.extend(more_attrs);
                    Value::Record(attrs, items)
                } else {
                    Value::Record(attrs, vec![Item::of(body)])
                }
            }
            _ => Value::record(vec![Item::of(body)]),
        }
    }

    fn build(self) -> Value {
        if let ValueInterpreter(Some(RecordBuilder { attrs, items })) = self {
            Value::Record(attrs, items)
        } else {
            Value::empty_record()
        }
    }
}

#[derive(Default)]
struct RecordBuilder {
    attrs: Vec<Attr>,
    items: Vec<Item>,
}

impl PrimitiveWriter for ValueInterpreter {
    type Repr = Value;
    type Error = Infallible;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Extant)
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Int32Value(value))
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Int64Value(value))
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        Ok(Value::UInt32Value(value))
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        Ok(Value::UInt64Value(value))
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Float64Value(value))
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        Ok(Value::BooleanValue(value))
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        Ok(Value::BigInt(value))
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        Ok(Value::BigUint(value))
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Text(value.into()))
    }

    fn write_blob_vec(self, value: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Data(Blob::from_vec(value)))
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        Ok(Value::Data(Blob::from_vec(value.into())))
    }
}

impl StructuralWriter for ValueInterpreter {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl HeaderWriter for ValueInterpreter {
    type Repr = Value;
    type Error = Infallible;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let interpreted = value.write_with(interpereter)?;
        let name: Text = name.into();
        self.get_attrs().push((name, interpreted).into());
        Ok(self)
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let body = value.write_with(interpereter)?;
        Ok(self.with_delegate_body(body))
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        mut self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let interpreted = value.write_into(interpereter)?;
        let name: Text = name.into();
        self.get_attrs().push((name, interpreted).into());
        Ok(self)
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let body = value.write_into(interpereter)?;
        Ok(self.with_delegate_body(body))
    }

    fn complete_header(self, _num_items: usize) -> Result<Self::Body, Self::Error> {
        Ok(self)
    }
}

impl BodyWriter for ValueInterpreter {
    type Repr = Value;
    type Error = Infallible;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let interpreted = value.write_with(interpereter)?;
        self.get_items().push(interpreted.into());
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let key_interp = ValueInterpreter::default();
        let interpreted_key = key.write_with(key_interp)?;
        let val_interp = ValueInterpreter::default();
        let interpreted_val = value.write_with(val_interp)?;
        self.get_items()
            .push((interpreted_key, interpreted_val).into());
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(mut self, value: V) -> Result<Self, Self::Error> {
        let interpereter = ValueInterpreter::default();
        let interpreted = value.write_into(interpereter)?;
        self.get_items().push(interpreted.into());
        Ok(self)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        let key_interp = ValueInterpreter::default();
        let interpreted_key = key.write_into(key_interp)?;
        let val_interp = ValueInterpreter::default();
        let interpreted_val = value.write_into(val_interp)?;
        self.get_items()
            .push((interpreted_key, interpreted_val).into());
        Ok(self)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        Ok(self.build())
    }
}
