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

use crate::Form;
use crate::_common::model::{Item, Value};
use crate::_deserialize::FormDeserializeErr;
use crate::primitives::de_incorrect_type;

impl<T: Form> Form for Vec<T> {
    fn as_value(&self) -> Value {
        Value::record(self.iter().map(|t| Item::from(t.as_value())).collect())
    }

    fn try_from_value<'f>(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                let length = items.len();
                items
                    .iter()
                    .try_fold(
                        Vec::with_capacity(length),
                        |mut results: Vec<T>, item| match item {
                            Item::ValueItem(v) => {
                                let result = T::try_from_value(v)?;
                                results.push(result);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i.to_owned())),
                        },
                    )
            }
            v => de_incorrect_type("Vec<T>", v),
        }
    }
}
