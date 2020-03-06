// Copyright 2015-2020 SWIM.AI inc.
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

use serde::de::{DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::Deserializer;

use crate::model::{Attr, Item, Value};
use crate::structure::form::{FormParseErr, Result};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ValueDeserializer<'s, 'de: 's> {
    current_state: State<'s>,
    stack: Vec<State<'s>>,
    input: &'de Value,
}

#[derive(Debug, Clone)]
pub struct State<'s> {
    deserializer_state: DeserializerState<'s>,
    value: Option<&'s Value>,
}

#[derive(Clone, Debug)]
pub enum DeserializerState<'i> {
    ReadingRecord { item_index: usize },
    ReadingItem(&'i Item),
    ReadingAttribute(&'i Attr),
    ReadingSingleValue,
    None,
}

impl<'s, 'de: 's> ValueDeserializer<'s, 'de> {
    pub fn for_values(input: &'de Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::None,
                value: Some(input),
            },
            stack: vec![],
            input,
        }
    }

    pub fn assert_stack_empty(&self) {
        assert_eq!(self.stack.len(), 0);
    }

    pub fn for_single_value(input: &'de Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::ReadingSingleValue,
                value: Some(input),
            },
            stack: vec![],
            input,
        }
    }

    pub fn push_record(&mut self, value: Option<&'s Value>) {
        self.push_state(State {
            deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
            value,
        });
    }

    pub fn push_state(&mut self, state: State<'s>) {
        self.stack.push(self.current_state.to_owned());
        self.current_state = state;
    }

    pub fn pop_state(&mut self) {
        if let Some(previous_state) = self.stack.pop() {
            self.current_state = previous_state;
        } else {
            self.current_state = State {
                deserializer_state: DeserializerState::None,
                value: None,
            };
        }
    }
}

impl<'s, 'de: 's, 'a> Deserializer<'de> for &'a mut ValueDeserializer<'s, 'de> {
    type Error = FormParseErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.current_state.value {
            Some(value) => match value {
                Value::Record(attrs, items) => panic!(),
                Value::Int32Value(_) => self.deserialize_i32(visitor),
                Value::Int64Value(_) => self.deserialize_i64(visitor),
                Value::Extant => self.deserialize_option(visitor),
                Value::Text(_) => self.deserialize_string(visitor),
                Value::Float64Value(_) => self.deserialize_f64(visitor),
                Value::BooleanValue(_) => self.deserialize_bool(visitor),
            },
            None => {
                if let DeserializerState::ReadingAttribute(a) =
                    self.current_state.deserializer_state
                {
                    visitor.visit_string(a.name.to_owned())
                } else {
                    panic!()
                }
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::BooleanValue(b)) = &self.current_state.value {
            visitor.visit_bool(b.to_owned())
        } else {
            Err(FormParseErr::Message(String::from("Unexpected type")))
        }
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: i8")))
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: i16")))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Int32Value(i)) = &self.current_state.value {
            visitor.visit_i32(i.to_owned())
        } else {
            Err(FormParseErr::Message(String::from("Unexpected type")))
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Int64Value(i)) = &self.current_state.value {
            visitor.visit_i64(i.to_owned())
        } else {
            Err(FormParseErr::Message(String::from("Unexpected type")))
        }
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: u8")))
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: u16")))
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: u32")))
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: u64")))
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: f32")))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Float64Value(f)) = &self.current_state.value {
            visitor.visit_f64(f.to_owned())
        } else {
            Err(FormParseErr::Message(String::from("Unexpected type")))
        }
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from(
            "Unsupported type: char",
        )))
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from("Unsupported type: str")))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Text(t)) = &self.current_state.value {
            visitor.visit_string(t.to_owned())
        } else {
            Err(FormParseErr::Message(String::from("Unexpected type")))
        }
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from(
            "Unsupported type: byte",
        )))
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(FormParseErr::Message(String::from(
            "Unsupported type: byte",
        )))
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.current_state.value {
            Some(Value::Extant) => visitor.visit_none(),
            Some(value @ Value::Record(_, _)) => visitor.visit_some(self),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Extracts the current value from what is being read.
    // For records this is the current item (indexed by 'item_index' that has been set by the previous method call.
    // For tuples, no state has been assigned to the deserializer and so the current value is pushed as a record.
    // For nested collections, the current value is extracted from the current item that is being deserialized.
    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match &self.current_state.deserializer_state {
            DeserializerState::ReadingRecord { ref item_index } => match self.current_state.value {
                Some(Value::Record(attrs, items)) => {
                    let value = match items.get(*item_index) {
                        Some(Item::ValueItem(value)) => value,
                        Some(Item::Slot(key, value)) => value,
                        _ => panic!("Value out of sync with state"),
                    };
                    self.push_record(Some(&value));
                }
                _ => panic!("Value out of sync with state"),
            },
            DeserializerState::ReadingItem(item) => {
                let value = match item {
                    Item::ValueItem(value) => value,
                    Item::Slot(key, value) => value,
                };
                self.push_record(Some(&value));
            }
            _ => self.push_record(self.current_state.value),
        }

        let result = Ok(visitor.visit_seq(RecordMap::new(&mut self))?);
        self.pop_state();

        result
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let state = {
            // If we're reading an item then we are reading a nested struct
            if let DeserializerState::ReadingItem(item) = self.current_state.deserializer_state {
                match item {
                    Item::ValueItem(value) => State {
                        deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
                        value: Some(&value),
                    },
                    Item::Slot(key, value) => State {
                        deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
                        value: Some(&value),
                    },
                }
            } else {
                State {
                    deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
                    value: self.current_state.value,
                }
            }
        };

        self.push_state(state);
        let value = visitor.visit_map(RecordMap::new(&mut self))?;
        self.pop_state();

        Ok(value)
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_struct");
        if let DeserializerState::None = &self.current_state.deserializer_state {
            self.current_state.value = Some(self.input);
        }

        match &self.current_state.value {
            Some(value) => {
                if let Value::Record(attrs, items) = value {
                    match attrs.first() {
                        Some(a) => {
                            if a.name == name {
                                self.deserialize_map(visitor)
                            } else {
                                Err(FormParseErr::Malformatted)
                            }
                        }
                        None => Err(FormParseErr::Message(String::from(
                            "Missing tag for struct",
                        ))),
                    }
                } else {
                    unimplemented!()
                }
            }
            None => Err(FormParseErr::Message(String::from("Missing record"))),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = visitor.visit_enum(Enum::new(self))?;
        Ok(value)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(_visitor)
    }
}

struct RecordMap<'a, 'de: 'a, 's> {
    de: &'a mut ValueDeserializer<'s, 'de>,
}

impl<'a, 'de, 's> RecordMap<'a, 'de, 's> {
    fn new(de: &'a mut ValueDeserializer<'s, 'de>) -> Self {
        if let Value::Record(_, _) = de.input {
            RecordMap { de }
        } else {
            // TODO Return an error
            unimplemented!()
        }
    }
}

impl<'de, 'a, 's> SeqAccess<'de> for RecordMap<'a, 'de, 's> {
    type Error = FormParseErr;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.de.current_state.value {
            Some(v) => {
                if let Value::Record(attrs, items) = v {
                    let result = {
                        match self.de.current_state.deserializer_state {
                            DeserializerState::ReadingRecord { item_index } => {
                                if item_index < items.len() {
                                    let item = items.get(item_index).unwrap();
                                    let value = match item {
                                        Item::ValueItem(value) => value,
                                        Item::Slot(_, _) => {
                                            return Err(FormParseErr::Message(String::from(
                                                "Malformatted record. Slot in place of ValueItem",
                                            )));
                                        }
                                    };

                                    self.de.push_state(State {
                                        deserializer_state: DeserializerState::ReadingItem(item),
                                        value: Some(value),
                                    });

                                    let result = seed.deserialize(&mut *self.de).map(Some);
                                    self.de.pop_state();

                                    result
                                } else {
                                    // println!("Record: Ok(None)");
                                    Ok(None)
                                }
                            }
                            DeserializerState::ReadingItem(item) => {
                                // println!("Item: Ok(None)");
                                Ok(None)
                            }

                            _ => unimplemented!("Illegal state"),
                        }
                    };

                    if let DeserializerState::ReadingRecord { item_index } =
                        &mut self.de.current_state.deserializer_state
                    {
                        *item_index += 1;
                    }

                    result
                } else {
                    seed.deserialize(&mut *self.de).map(Some)
                }
            }
            None => unimplemented!(),
        }
    }
}

impl<'de, 'a, 's> MapAccess<'de> for RecordMap<'a, 'de, 's> {
    type Error = FormParseErr;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if let DeserializerState::ReadingRecord { item_index } =
            self.de.current_state.deserializer_state
        {
            if let Some(value) = self.de.current_state.value {
                match value {
                    Value::Record(_, items) => {
                        if item_index < items.len() {
                            // Todo: remove unwrap
                            let item = items.get(item_index).unwrap();
                            let value = match item {
                                Item::Slot(key, value) => key,
                                Item::ValueItem(value) => value,
                            };

                            self.de.push_state(State {
                                deserializer_state: DeserializerState::ReadingItem(item),
                                value: Some(value),
                            });
                            seed.deserialize(&mut *self.de).map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                    _v => unimplemented!("Illegal state"),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        if let DeserializerState::ReadingItem(item) = self.de.current_state.deserializer_state {
            if let Item::Slot(_, value) = item {
                self.de.current_state.value = Some(value);
            } else {
                unimplemented!()
            }

            let result = seed.deserialize(&mut *self.de);
            self.de.pop_state();

            if let DeserializerState::ReadingRecord { ref mut item_index } =
                self.de.current_state.deserializer_state
            {
                *item_index += 1;
            }

            result
        } else {
            Err(FormParseErr::Message(String::from(
                "Attempted to read value when not reading an item",
            )))
        }
    }
}

struct Enum<'a, 'de: 'a, 's> {
    de: &'a mut ValueDeserializer<'s, 'de>,
}

impl<'a, 'de, 's> Enum<'a, 'de, 's> {
    fn new(de: &'a mut ValueDeserializer<'s, 'de>) -> Self {
        Enum { de }
    }
}

impl<'de, 'a, 's> EnumAccess<'de> for Enum<'a, 'de, 's> {
    type Error = FormParseErr;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        if let Some(Value::Record(attrs, items)) = &self.de.current_state.value {
            match attrs.first() {
                Some(a) => {
                    self.de.push_state(State {
                        deserializer_state: DeserializerState::ReadingAttribute(a),
                        value: None,
                    });

                    let val = seed.deserialize(&mut *self.de)?;
                    self.de.pop_state();

                    Ok((val, self))
                }
                None => Err(FormParseErr::Message(String::from(
                    "Enum missing variant name",
                ))),
            }
        } else {
            unimplemented!()
        }
    }
}

impl<'de, 'a, 's> VariantAccess<'de> for Enum<'a, 'de, 's> {
    type Error = FormParseErr;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Deserializer::deserialize_seq(self.de, visitor)
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Deserializer::deserialize_map(self.de, visitor)
    }
}
