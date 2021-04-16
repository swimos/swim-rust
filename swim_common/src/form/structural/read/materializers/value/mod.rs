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

use crate::form::structural::read::{BodyReader, HeaderReader, ReadError};
use crate::model::blob::Blob;
use crate::model::text::Text;
use crate::model::{Attr, Item, Value};
use num_bigint::{BigInt, BigUint};
use std::borrow::Cow;
use std::convert::TryFrom;

/// Implements [`HeaderReader`] and [`BodyReader`] to construct a [`Value`] instance.
#[derive(Default, Debug)]
pub struct ValueMaterializer {
    root: RecordBuilder,
    stack: Vec<RecordBuilder>,
    slot_key: Option<Value>,
}

#[derive(Debug)]
enum RecordKey {
    NoKey,
    Attr(Text),
    Slot(Value),
}

impl Default for RecordKey {
    fn default() -> Self {
        RecordKey::NoKey
    }
}

#[derive(Debug, Default)]
struct RecordBuilder {
    key: RecordKey,
    attrs: Vec<Attr>,
    items: Vec<Item>,
}

impl RecordBuilder {
    fn new(key: RecordKey) -> Self {
        RecordBuilder {
            key,
            attrs: Vec::new(),
            items: Vec::new(),
        }
    }
}

impl TryFrom<ValueMaterializer> for Value {
    type Error = ReadError;

    fn try_from(value: ValueMaterializer) -> Result<Self, Self::Error> {
        let ValueMaterializer {
            root: RecordBuilder { attrs, items, .. },
            stack,
            slot_key,
        } = value;
        if stack.is_empty() && slot_key.is_none() {
            Ok(Value::Record(attrs, items))
        } else {
            println!(
                "attrs: {:?}, items: {:?},stack: {:?}, slot_key: {:?}",
                attrs, items, stack, slot_key
            );
            Err(ReadError::IncompleteRecord)
        }
    }
}

impl ValueMaterializer {
    fn peek_mut(&mut self) -> &mut RecordBuilder {
        let ValueMaterializer { root, stack, .. } = self;
        stack.last_mut().unwrap_or(root)
    }

    fn new_item_frame(&mut self) {
        let frame = if let Some(key) = self.slot_key.take() {
            RecordBuilder::new(RecordKey::Slot(key))
        } else {
            RecordBuilder::new(RecordKey::NoKey)
        };
        self.stack.push(frame);
    }

    fn new_attr_frame(&mut self, name: Text) {
        self.stack.push(RecordBuilder::new(RecordKey::Attr(name)))
    }

    fn set_slot_key(&mut self) {
        let key = match self.peek_mut().items.pop() {
            Some(Item::ValueItem(value)) => value,
            _ => Value::Extant,
        };
        self.slot_key = Some(key);
    }

    fn pop(&mut self) -> Result<(), ReadError> {
        if let Some(RecordBuilder {
            key,
            attrs,
            mut items,
        }) = self.stack.pop()
        {
            match key {
                RecordKey::NoKey => {
                    let record = Value::Record(attrs, items);
                    self.add_value(record);
                }
                RecordKey::Slot(key) => {
                    let record = Value::Record(attrs, items);
                    self.add_slot(key, record);
                }
                RecordKey::Attr(name) => {
                    let body = if attrs.is_empty() && items.len() <= 1 {
                        match items.pop() {
                            Some(Item::ValueItem(value)) => value,
                            Some(slot @ Item::Slot(_, _)) => Value::Record(Vec::new(), vec![slot]),
                            _ => Value::Extant,
                        }
                    } else {
                        Value::Record(attrs, items)
                    };
                    self.add_attr(name, body);
                }
            }
            Ok(())
        } else {
            Err(ReadError::ReaderUnderflow)
        }
    }

    fn add_attr(&mut self, name: Text, value: Value) {
        self.peek_mut().attrs.push((name, value).into())
    }

    fn add_item(&mut self, value: Value) {
        if let Some(key) = self.slot_key.take() {
            self.add_slot(key, value);
        } else {
            self.add_value(value);
        }
    }

    fn add_slot(&mut self, key: Value, value: Value) {
        self.peek_mut().items.push((key, value).into());
    }

    fn add_value(&mut self, value: Value) {
        self.peek_mut().items.push(value.into());
    }
}

impl HeaderReader for ValueMaterializer {
    type Body = Self;
    type Delegate = Self;

    fn read_attribute(mut self, name: Cow<'_, str>) -> Result<Self::Delegate, ReadError> {
        self.new_attr_frame(name.into());
        Ok(self)
    }

    fn restore(mut delegate: Self::Delegate) -> Result<Self, ReadError> {
        delegate.pop()?;
        Ok(delegate)
    }

    fn start_body(self) -> Result<Self::Body, ReadError> {
        Ok(self)
    }
}

impl BodyReader for ValueMaterializer {
    type Delegate = Self;

    fn push_extant(&mut self) -> Result<bool, ReadError> {
        self.add_item(Value::Extant);
        Ok(true)
    }

    fn push_i32(&mut self, value: i32) -> Result<bool, ReadError> {
        self.add_item(Value::Int32Value(value));
        Ok(true)
    }

    fn push_i64(&mut self, value: i64) -> Result<bool, ReadError> {
        self.add_item(Value::Int64Value(value));
        Ok(true)
    }

    fn push_u32(&mut self, value: u32) -> Result<bool, ReadError> {
        self.add_item(Value::UInt32Value(value));
        Ok(true)
    }

    fn push_u64(&mut self, value: u64) -> Result<bool, ReadError> {
        self.add_item(Value::UInt64Value(value));
        Ok(true)
    }

    fn push_f64(&mut self, value: f64) -> Result<bool, ReadError> {
        self.add_item(Value::Float64Value(value));
        Ok(true)
    }

    fn push_bool(&mut self, value: bool) -> Result<bool, ReadError> {
        self.add_item(Value::BooleanValue(value));
        Ok(true)
    }

    fn push_big_int(&mut self, value: BigInt) -> Result<bool, ReadError> {
        self.add_item(Value::BigInt(value));
        Ok(true)
    }

    fn push_big_uint(&mut self, value: BigUint) -> Result<bool, ReadError> {
        self.add_item(Value::BigUint(value));
        Ok(true)
    }

    fn push_text(&mut self, value: Cow<'_, str>) -> Result<bool, ReadError> {
        self.add_item(Value::Text(value.into()));
        Ok(true)
    }

    fn push_blob(&mut self, value: Vec<u8>) -> Result<bool, ReadError> {
        self.add_item(Value::Data(Blob::from_vec(value)));
        Ok(true)
    }

    fn start_slot(&mut self) -> Result<(), ReadError> {
        self.set_slot_key();
        Ok(())
    }

    fn push_record(mut self) -> Result<Self::Delegate, ReadError> {
        self.new_item_frame();
        Ok(self)
    }

    fn restore(mut delegate: Self) -> Result<Self, ReadError> {
        delegate.pop()?;
        Ok(delegate)
    }
}
