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

use serde::Deserializer;

use common::model::{Item, Value};

use crate::enum_access::Enum;
use crate::map_access::RecordMap;
use crate::{DeserializerState, FormDeserializeErr, Result, State, ValueDeserializer};
use serde::de::Visitor;

impl<'de, 'a> Deserializer<'de> for &'a mut ValueDeserializer<'de> {
    type Error = FormDeserializeErr;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.current_state.value {
            Some(value) => match value {
                Value::Record(_attrs, _items) => {
                    unreachable!("Deserializer entered an illegal state")
                }
                Value::Int32Value(_) => self.deserialize_i32(visitor),
                Value::Int64Value(_) => self.deserialize_i64(visitor),
                Value::Extant => self.deserialize_option(visitor),
                Value::Text(_) => self.deserialize_string(visitor),
                Value::Float64Value(_) => self.deserialize_f64(visitor),
                Value::BooleanValue(_) => self.deserialize_bool(visitor),
                Value::BigInt(_) => self.deserialize_str(visitor),
                Value::BigUint(_) => self.deserialize_string(visitor),
            },
            None => {
                if let DeserializerState::ReadingAttribute(a) =
                    self.current_state.deserializer_state
                {
                    visitor.visit_string(a.name.to_owned())
                } else {
                    Err(FormDeserializeErr::Malformatted)
                }
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::BooleanValue(b)) = &self.current_state.value {
            visitor.visit_bool(*b)
        } else {
            self.err_incorrect_type("Value::BooleanValue", self.current_state.value)
        }
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("i8")
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("i16")
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Int32Value(i)) = &self.current_state.value {
            visitor.visit_i32(*i)
        } else {
            self.err_incorrect_type("Value::Int32Value", self.current_state.value)
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Int64Value(i)) = &self.current_state.value {
            visitor.visit_i64(*i)
        } else {
            self.err_incorrect_type("Value::Int64Value", self.current_state.value)
        }
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("u8")
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("u16")
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("u32")
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("u64")
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("f32")
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Float64Value(f)) = &self.current_state.value {
            visitor.visit_f64(*f)
        } else {
            self.err_incorrect_type("Value::Float64Value", self.current_state.value)
        }
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("char")
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(Value::Text(t)) = &self.current_state.value {
            visitor.visit_borrowed_str(t)
        } else {
            self.err_unsupported("str")
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match &self.current_state.value {
            Some(Value::Text(t)) => visitor.visit_string(t.to_owned()),
            Some(Value::BigInt(bi)) => visitor.visit_string(bi.to_string()),
            Some(Value::BigUint(bui)) => visitor.visit_string(bui.to_string()),
            _ => self.err_incorrect_type("Value::Text", self.current_state.value),
        }
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("byte")
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.err_unsupported("byte")
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.current_state.value {
            Some(Value::Extant) => visitor.visit_none(),
            Some(_value @ Value::Record(_, _)) => visitor.visit_some(self),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.check_struct_access(name)?;
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.check_struct_access(name)?;
        self.deserialize_seq(visitor)
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
                Some(Value::Record(_attrs, items)) => {
                    let value = match items.get(*item_index) {
                        Some(Item::ValueItem(value)) => value,
                        Some(Item::Slot(_key, value)) => value,
                        _ => panic!("Value out of sync with state"),
                    };
                    self.push_record(Some(&value));
                }
                _ => panic!("Value out of sync with state"),
            },
            DeserializerState::ReadingItem(item) => {
                let value = match item {
                    Item::ValueItem(value) => value,
                    Item::Slot(_key, value) => value,
                };
                self.push_record(Some(&value));
            }
            _ => self.push_record(self.current_state.value),
        }

        if let Some(Value::Record(_, _)) = self.current_state.value {
            let result = Ok(visitor.visit_seq(RecordMap::new(&mut self))?);
            self.pop_state();

            result
        } else {
            match self.current_state.value {
                Some(v) => match v {
                    Value::BigInt(_bi) => {
                        println!("big int");
                        unimplemented!()
                    }
                    _ => self.err_incorrect_type("Value::Record", Some(v)),
                },
                None => Err(FormDeserializeErr::Message(String::from("Missing value"))),
            }
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let DeserializerState::None = &self.current_state.deserializer_state {
            self.current_state.value = Some(self.input);
        }

        match &self.current_state.value {
            Some(value) => {
                if let Value::Record(attrs, _items) = value {
                    match attrs.first() {
                        Some(a) => {
                            if a.name == name {
                                self.deserialize_seq(visitor)
                            } else {
                                Err(FormDeserializeErr::Malformatted)
                            }
                        }
                        None => Err(FormDeserializeErr::Message(String::from("Missing tag"))),
                    }
                } else {
                    self.err_incorrect_type("Value::Record", Some(&Value::Extant))
                }
            }
            None => Err(FormDeserializeErr::Message(String::from("Missing record"))),
        }
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
                    Item::Slot(_key, value) => State {
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
        if let DeserializerState::None = &self.current_state.deserializer_state {
            self.current_state.value = Some(self.input);
        }

        match &self.current_state.value {
            Some(value) => {
                if let Value::Record(attrs, _items) = value {
                    match attrs.first() {
                        Some(a) => {
                            if a.name == name {
                                self.deserialize_map(visitor)
                            } else {
                                Err(FormDeserializeErr::Malformatted)
                            }
                        }
                        None => Err(FormDeserializeErr::Message(String::from("Missing tag"))),
                    }
                } else {
                    self.err_incorrect_type("Value::Record", Some(&Value::Extant))
                }
            }
            None => Err(FormDeserializeErr::Message(String::from("Missing record"))),
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

// pub struct BigIntDeserializer<'a, 'de: 'a> {
//     big_int: &'a BigInt,
//     de: &'a mut ValueDeserializer<'de>,
// }
//
// impl<'a, 'de> BigIntDeserializer<'a, 'de> {
//     pub fn new(de: &'a mut ValueDeserializer<'de>) -> Self {
//         BigIntDeserializer { de }
//     }
// }
//
// impl<'a, 'de> Deserialize for BigIntDeserializer<'a, 'de> {
//     fn deserialize<D>(deserializer: D) -> Result<Self>
//     where
//         D: Deserializer<'de>,
//     {
//         unimplemented!()
//     }
// }
