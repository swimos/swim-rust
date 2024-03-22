// Copyright 2015-2023 Swim Inc.
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

use swimos::model::time::Timestamp;
use swimos_form::Form;

#[derive(Debug, PartialEq, Eq, Hash, Form, Clone, Copy)]
#[form(tag = "message")]
pub struct Message {
    pub foo: i32,
    pub bar: i32,
    pub baz: i32,
}

#[derive(Debug, PartialEq, Eq, Hash, Form, Clone, Copy, Default)]
#[form(tag = "counter")]
pub struct Counter {
    pub count: usize,
}

#[derive(Debug, PartialEq, Eq, Hash, Form, Clone, Copy)]
#[form(tag = "item")]
pub struct HistoryItem {
    pub message: Message,
    pub timestamp: Timestamp,
}

impl HistoryItem {
    pub fn new(message: Message) -> Self {
        HistoryItem {
            message,
            timestamp: Timestamp::now(),
        }
    }
}
