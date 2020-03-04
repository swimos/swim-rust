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

use crate::model::{Item, Value};
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
    ReadingSingleValue,
    None,
}

impl<'s, 'de: 's> ValueDeserializer<'s, 'de> {
    pub fn for_values(input: &'de Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
                value: None,
            },
            stack: vec![],
            input,
        }
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
            None => panic!(),
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
        unimplemented!()
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
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
        unimplemented!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
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
        unimplemented!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
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
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // todo
        match &mut self.input {
            Value::Extant => visitor.visit_none(),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
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

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Tuple structs look just like sequences in JSON.
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
        let value = visitor.visit_map(RecordMap::new(&mut self))?;
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
        match &self.input {
            Value::Record(attrs, _) => match attrs.first() {
                Some(a) => {
                    if a.name == name {
                        let state = State {
                            deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
                            value: Some(&self.input),
                        };

                        self.push_state(state);
                        self.deserialize_map(visitor)
                    } else {
                        Err(FormParseErr::Malformatted)
                    }
                }
                None => Err(FormParseErr::Message(String::from(
                    "Missing tag for struct",
                ))),
            },
            v => unimplemented!("{:?}", v),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
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
        unimplemented!()
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

    fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        unimplemented!()
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
                                _ => panic!(),
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
                    _v => unimplemented!(),
                }
            } else {
                unimplemented!()
            }
        } else {
            unimplemented!()
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

    fn variant_seed<V>(self, _seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        unimplemented!()
    }
}

impl<'de, 'a, 's> VariantAccess<'de> for Enum<'a, 'de, 's> {
    type Error = FormParseErr;

    fn unit_variant(self) -> Result<()> {
        Err(FormParseErr::Message(String::from("Expected string")))
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
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
