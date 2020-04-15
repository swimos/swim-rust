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

use serde::de::{DeserializeSeed, EnumAccess, VariantAccess, Visitor};
use serde::Deserializer;

use common::model::Value;

use crate::{DeserializerState, FormDeserializeErr, Result, State, ValueDeserializer};

pub struct Enum<'a, 'de: 'a> {
    de: &'a mut ValueDeserializer<'de>,
}

impl<'a, 'de> Enum<'a, 'de> {
    pub fn new(de: &'a mut ValueDeserializer<'de>) -> Self {
        Enum { de }
    }
}

impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Error = FormDeserializeErr;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        if let Some(Value::Record(attrs, _items)) = &self.de.current_state.value {
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
                None => Err(FormDeserializeErr::Message(String::from(
                    "Enum missing variant name",
                ))),
            }
        } else {
            unimplemented!()
        }
    }
}

impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = FormDeserializeErr;

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
