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

use crate::{DeserializerState, FormDeserializeErr, Result, State, ValueDeserializer};
use common::model::{Item, Value};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess};

pub struct RecordMap<'a, 'de: 'a> {
    de: &'a mut ValueDeserializer<'de>,
}

impl<'a, 'de> RecordMap<'a, 'de> {
    pub fn new(de: &'a mut ValueDeserializer<'de>) -> Self {
        RecordMap { de }
    }
}

impl<'de, 'a> SeqAccess<'de> for RecordMap<'a, 'de> {
    type Error = FormDeserializeErr;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.de.current_state.value {
            Some(v) => {
                if let Value::Record(_attrs, items) = v {
                    let result = {
                        match self.de.current_state.deserializer_state {
                            DeserializerState::ReadingRecord { item_index } => {
                                if item_index < items.len() {
                                    let item = items.get(item_index).unwrap();
                                    let value = match item {
                                        Item::ValueItem(value) => value,
                                        Item::Slot(_, _) => {
                                            return Err(FormDeserializeErr::Message(String::from(
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
                                    Ok(None)
                                }
                            }
                            DeserializerState::ReadingItem(_item) => Ok(None),

                            _ => unreachable!("Illegal state"),
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
            None => unreachable!(),
        }
    }
}

impl<'de, 'a> MapAccess<'de> for RecordMap<'a, 'de> {
    type Error = FormDeserializeErr;

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
                                Item::Slot(key, _value) => key,
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
                    _v => unreachable!("Illegal state"),
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
                unreachable!()
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
            Err(FormDeserializeErr::Message(String::from(
                "Attempted to read value when not reading an item",
            )))
        }
    }
}
