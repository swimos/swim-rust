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

use crate::model::{Item, Value};

#[derive(Debug)]
pub struct ValueDeserializer<'s, 'de> {
    current_state: State<'s>,
    stack: Vec<State<'s>>,
    input: &'de mut Value,
}

#[derive(Debug, Clone)]
pub struct State<'s> {
    deserializer_state: DeserializerState<'s>,
}

#[derive(Clone, Debug)]
pub enum DeserializerState<'i> {
    ReadingRecord { item_index: usize },
    ReadingItem(&'i Item),
}

impl<'s, 'de> ValueDeserializer<'s, 'de> {
    fn new(input: &'de mut Value) -> Self {
        ValueDeserializer {
            current_state: State {
                deserializer_state: DeserializerState::ReadingRecord { item_index: 0 },
            },
            stack: vec![],
            input,
        }
    }
}
