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

use std::sync::Arc;
use swim_common::form::Form;
use swim_common::form::ValidatedForm;
use swim_common::model::Value;

/// Updates that can be applied to a map lane.
#[derive(Debug, PartialEq, Eq, Form, ValidatedForm, Clone)]
pub enum MapUpdate<K, V> {
    #[form(tag = "update")]
    Update(#[form(header, name = "key")] K, #[form(body)] Arc<V>),
    #[form(tag = "remove")]
    Remove(#[form(header, name = "key")] K),
    #[form(tag = "clear")]
    Clear,
    #[form(tag = "take")]
    Take(#[form(header_body)] usize),
    #[form(tag = "drop")]
    Drop(#[form(header_body)] usize),
}

impl<K: Form, V: Form> From<MapUpdate<K, V>> for Value {
    fn from(event: MapUpdate<K, V>) -> Self {
        event.into_value()
    }
}
