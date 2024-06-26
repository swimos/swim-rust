// Copyright 2015-2024 Swim Inc.
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

use crate::structural::read::event::{NumericValue, ReadEvent};
use crate::structural::read::recognizer::Recognizer;
use crate::structural::read::ReadError;
use std::convert::TryFrom;
use std::option::Option::None;
use swimos_model::{Attr, Blob, Item, Text, Value};

#[derive(Debug, Default)]
enum RecordKey {
    #[default]
    NoKey,
    Attr(Text),
    Slot(Value),
}

#[derive(Debug, Default)]
struct RecordBuilder {
    key: RecordKey,
    attrs: Vec<Attr>,
    items: Vec<Item>,
    in_body: bool,
}

impl RecordBuilder {
    fn new(key: RecordKey, in_body: bool) -> Self {
        RecordBuilder {
            key,
            attrs: Vec::new(),
            items: Vec::new(),
            in_body,
        }
    }
}

/// [`Recognizer`] implementation for the [`Value`] type.
#[derive(Default, Debug)]
pub struct ValueMaterializer {
    stack: Vec<RecordBuilder>,
    slot_key: Option<Value>,
}

enum ItemEvent {
    Primitive(Value),
    RecordAtAttr(Text),
    RecordAtBody,
    EndAttr,
    EndRec,
    Slot,
}

impl ValueMaterializer {
    fn peek_mut(&mut self) -> Result<&mut RecordBuilder, ReadError> {
        self.stack.last_mut().ok_or(ReadError::ReaderUnderflow)
    }

    fn new_record_frame(&mut self, in_body: bool) -> &mut RecordBuilder {
        let frame = if let Some(key) = self.slot_key.take() {
            RecordBuilder::new(RecordKey::Slot(key), in_body)
        } else {
            RecordBuilder::new(RecordKey::NoKey, in_body)
        };
        let len = self.stack.len();
        self.stack.push(frame);
        &mut self.stack[len]
    }

    fn new_record_item(&mut self) -> Result<(), ReadError> {
        let top = self.peek_mut()?;
        if top.in_body {
            self.new_record_frame(true);
        } else {
            top.in_body = true;
        }
        Ok(())
    }

    fn new_attr_frame(&mut self, name: Text) {
        match self.stack.last_mut() {
            Some(top) if !top.in_body => {}
            _ => {
                self.new_record_frame(false);
            }
        }
        self.stack
            .push(RecordBuilder::new(RecordKey::Attr(name), true))
    }

    fn set_slot_key(&mut self) -> Result<(), ReadError> {
        let key = match self.peek_mut()?.items.pop() {
            Some(Item::ValueItem(value)) => value,
            _ => Value::Extant,
        };
        self.slot_key = Some(key);
        Ok(())
    }

    fn pop(&mut self, is_attr_end: bool) -> Result<Option<Value>, ReadError> {
        if let Some(RecordBuilder {
            key,
            attrs,
            mut items,
            ..
        }) = self.stack.pop()
        {
            match key {
                RecordKey::NoKey => {
                    if is_attr_end {
                        Err(ReadError::InconsistentState)
                    } else {
                        let record = Value::Record(attrs, items);
                        if self.stack.is_empty() {
                            Ok(Some(record))
                        } else {
                            self.add_value(record)?;
                            Ok(None)
                        }
                    }
                }
                RecordKey::Slot(key) => {
                    if is_attr_end {
                        Err(ReadError::InconsistentState)
                    } else {
                        let record = Value::Record(attrs, items);
                        self.add_slot(key, record)?;
                        Ok(None)
                    }
                }
                RecordKey::Attr(name) => {
                    if is_attr_end {
                        let body = if attrs.is_empty() && items.len() <= 1 {
                            match items.pop() {
                                Some(Item::ValueItem(value)) => value,
                                Some(slot @ Item::Slot(_, _)) => {
                                    Value::Record(Vec::new(), vec![slot])
                                }
                                _ => Value::Extant,
                            }
                        } else {
                            Value::Record(attrs, items)
                        };
                        self.add_attr(name, body)?;
                        Ok(None)
                    } else {
                        Err(ReadError::InconsistentState)
                    }
                }
            }
        } else {
            Err(ReadError::ReaderUnderflow)
        }
    }

    fn add_attr(&mut self, name: Text, value: Value) -> Result<(), ReadError> {
        self.peek_mut()?.attrs.push((name, value).into());
        Ok(())
    }

    fn add_item(&mut self, value: Value) -> Result<(), ReadError> {
        let slot_key = self.slot_key.take();
        let top = self.peek_mut()?;
        if top.in_body {
            if let Some(key) = slot_key {
                top.items.push((key, value).into());
            } else {
                top.items.push(value.into());
            }
            Ok(())
        } else {
            Err(ReadError::InconsistentState)
        }
    }

    fn add_slot(&mut self, key: Value, value: Value) -> Result<(), ReadError> {
        self.peek_mut()?.items.push((key, value).into());
        Ok(())
    }

    fn add_value(&mut self, value: Value) -> Result<(), ReadError> {
        self.peek_mut()?.items.push(value.into());
        Ok(())
    }
}

fn recognize_item(input: ReadEvent<'_>) -> ItemEvent {
    match input {
        ReadEvent::Extant => ItemEvent::Primitive(Value::Extant),
        ReadEvent::Number(NumericValue::Int(n)) => {
            ItemEvent::Primitive(if let Ok(m) = i32::try_from(n) {
                Value::Int32Value(m)
            } else {
                Value::Int64Value(n)
            })
        }
        ReadEvent::Number(NumericValue::UInt(n)) => {
            ItemEvent::Primitive(if let Ok(m) = i32::try_from(n) {
                Value::Int32Value(m)
            } else if let Ok(m) = i64::try_from(n) {
                Value::Int64Value(m)
            } else if let Ok(m) = u32::try_from(n) {
                Value::UInt32Value(m)
            } else {
                Value::UInt64Value(n)
            })
        }
        ReadEvent::Number(NumericValue::Float(x)) => ItemEvent::Primitive(Value::Float64Value(x)),
        ReadEvent::Number(NumericValue::BigInt(n)) => ItemEvent::Primitive(Value::BigInt(n)),
        ReadEvent::Number(NumericValue::BigUint(n)) => ItemEvent::Primitive(Value::BigUint(n)),
        ReadEvent::Boolean(p) => ItemEvent::Primitive(Value::BooleanValue(p)),
        ReadEvent::TextValue(txt) => ItemEvent::Primitive(Value::Text(txt.into())),
        ReadEvent::Blob(v) => ItemEvent::Primitive(Value::Data(Blob::from_vec(v))),
        ReadEvent::StartAttribute(name) => ItemEvent::RecordAtAttr(name.into()),
        ReadEvent::StartBody => ItemEvent::RecordAtBody,
        ReadEvent::EndAttribute => ItemEvent::EndAttr,
        ReadEvent::EndRecord => ItemEvent::EndRec,
        ReadEvent::Slot => ItemEvent::Slot,
    }
}

impl Recognizer for ValueMaterializer {
    type Target = Value;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        if self.stack.is_empty() {
            match recognize_item(input) {
                ItemEvent::Primitive(v) => Some(Ok(v)),
                ItemEvent::RecordAtAttr(name) => {
                    self.new_attr_frame(name);
                    None
                }
                ItemEvent::RecordAtBody => {
                    self.new_record_frame(true);
                    None
                }
                _ => Some(Err(ReadError::InconsistentState)),
            }
        } else {
            match recognize_item(input) {
                ItemEvent::Primitive(v) => {
                    if let Err(e) = self.add_item(v) {
                        Some(Err(e))
                    } else {
                        None
                    }
                }
                ItemEvent::RecordAtAttr(name) => {
                    self.new_attr_frame(name);
                    None
                }
                ItemEvent::RecordAtBody => {
                    if let Err(e) = self.new_record_item() {
                        Some(Err(e))
                    } else {
                        None
                    }
                }
                ItemEvent::Slot => {
                    if let Err(e) = self.set_slot_key() {
                        Some(Err(e))
                    } else {
                        None
                    }
                }
                ItemEvent::EndAttr => {
                    if let Err(e) = self.pop(true) {
                        Some(Err(e))
                    } else {
                        None
                    }
                }
                ItemEvent::EndRec => self.pop(false).transpose(),
            }
        }
    }

    fn try_flush(&mut self) -> Option<Result<Self::Target, ReadError>> {
        let ValueMaterializer { stack, .. } = self;
        if stack.len() > 1 {
            None
        } else if let Some(top) = stack.pop() {
            if top.in_body {
                None
            } else {
                let RecordBuilder { attrs, .. } = top;
                Some(Ok(Value::Record(attrs, vec![])))
            }
        } else {
            Some(Ok(Value::Extant))
        }
    }

    fn reset(&mut self) {
        self.slot_key = None;
        self.stack.clear();
    }
}

/// [`Recognizer`] implementation for the [`Value`] type when it occurs in an attribute body.
#[derive(Debug)]
pub struct AttrBodyMaterializer {
    inner: ValueMaterializer,
}

impl Default for AttrBodyMaterializer {
    fn default() -> Self {
        AttrBodyMaterializer {
            inner: ValueMaterializer {
                stack: vec![RecordBuilder::new(RecordKey::NoKey, true)],
                slot_key: None,
            },
        }
    }
}

impl Recognizer for AttrBodyMaterializer {
    type Target = Value;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let AttrBodyMaterializer { inner } = self;
        if inner.stack.len() == 1 {
            match input {
                ReadEvent::EndAttribute => {
                    let RecordBuilder { items, .. } = &mut inner.stack[0];
                    let value = if items.len() <= 1 {
                        if let Some(item) = items.pop() {
                            if let Item::ValueItem(v) = item {
                                v
                            } else {
                                Value::record(vec![item])
                            }
                        } else {
                            Value::Extant
                        }
                    } else {
                        Value::record(std::mem::take(items))
                    };
                    Some(Ok(value))
                }
                ReadEvent::EndRecord => Some(Err(ReadError::ReaderUnderflow)),
                ow => match inner.feed_event(ow)? {
                    Ok(_) => Some(Err(ReadError::InconsistentState)),
                    Err(e) => Some(Err(e)),
                },
            }
        } else {
            match inner.feed_event(input)? {
                Ok(_) => Some(Err(ReadError::InconsistentState)),
                Err(e) => Some(Err(e)),
            }
        }
    }

    fn reset(&mut self) {
        let AttrBodyMaterializer {
            inner: ValueMaterializer { stack, slot_key },
        } = self;
        *slot_key = None;
        stack.clear();
        stack.push(RecordBuilder::new(RecordKey::NoKey, true));
    }
}

/// [`Recognizer`] implementation for the [`Value`] type when it occurs as a delegated field.
#[derive(Debug, Default)]
pub struct DelegateBodyMaterializer {
    inner: ValueMaterializer,
}

impl Recognizer for DelegateBodyMaterializer {
    type Target = Value;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        let result = self.inner.feed_event(input)?;
        Some(result.map(|v| match v {
            Value::Record(attrs, mut items) if attrs.is_empty() && items.len() <= 1 => {
                if let Some(item) = items.pop() {
                    if let Item::ValueItem(single) = item {
                        single
                    } else {
                        Value::record(vec![item])
                    }
                } else {
                    Value::Extant
                }
            }
            ow => ow,
        }))
    }

    fn reset(&mut self) {
        self.inner.reset()
    }
}
