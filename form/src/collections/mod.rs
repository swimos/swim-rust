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

use crate::_common::model::schema::StandardSchema;
use crate::_common::model::{Item, Value};
use crate::_deserialize::FormDeserializeErr;
use crate::primitives::de_incorrect_type;
use crate::{Form, ValidatedForm};
use im::OrdMap;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash};

#[cfg(test)]
mod tests;

impl<T: Form> Form for Vec<T> {
    fn as_value(&self) -> Value {
        Value::record(self.iter().map(|t| Item::from(t.as_value())).collect())
    }

    fn into_value(self) -> Value {
        Value::record(
            self.into_iter()
                .map(|t| Item::from(t.into_value()))
                .collect(),
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
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

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                let length = items.len();
                items.into_iter().try_fold(
                    Vec::with_capacity(length),
                    |mut results: Vec<T>, item| match item {
                        Item::ValueItem(v) => {
                            let result = T::try_convert(v)?;
                            results.push(result);
                            Ok(results)
                        }
                        i => Err(FormDeserializeErr::IllegalItem(i)),
                    },
                )
            }
            v => de_incorrect_type("Vec<T>", &v),
        }
    }
}

impl<T: ValidatedForm> ValidatedForm for Vec<T> {
    fn schema() -> StandardSchema {
        StandardSchema::array(<T as ValidatedForm>::schema()).and(StandardSchema::NumAttrs(0))
    }
}

impl<K: Hash + Eq + Form, V: Form, S: BuildHasher + Default> Form for HashMap<K, V, S> {
    fn as_value(&self) -> Value {
        Value::record(
            self.iter()
                .map(|(k, v)| Item::Slot(k.as_value(), v.as_value()))
                .collect(),
        )
    }

    fn into_value(self) -> Value {
        Value::record(
            self.into_iter()
                .map(|(k, v)| Item::Slot(k.into_value(), v.into_value()))
                .collect(),
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                items
                    .iter()
                    .try_fold(
                        HashMap::default(),
                        |mut results: HashMap<K, V, S>, item| match item {
                            Item::Slot(k, v) => {
                                let key = K::try_from_value(k)?;
                                let value = V::try_from_value(v)?;
                                results.insert(key, value);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i.to_owned())),
                        },
                    )
            }
            v => de_incorrect_type("HashMap<K, V>", v),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => items.into_iter().try_fold(
                HashMap::default(),
                |mut results: HashMap<K, V, S>, item| match item {
                    Item::Slot(k, v) => {
                        let key = K::try_convert(k)?;
                        let value = V::try_convert(v)?;
                        results.insert(key, value);
                        Ok(results)
                    }
                    i => Err(FormDeserializeErr::IllegalItem(i)),
                },
            ),
            v => de_incorrect_type("HashMap<K, V>", &v),
        }
    }
}

impl<K: Hash + Eq + ValidatedForm, V: ValidatedForm, S: BuildHasher + Default> ValidatedForm
    for HashMap<K, V, S>
{
    fn schema() -> StandardSchema {
        StandardSchema::map(
            <K as ValidatedForm>::schema(),
            <V as ValidatedForm>::schema(),
        )
        .and(StandardSchema::NumAttrs(0))
    }
}

impl<K: Ord + Form, V: Form> Form for BTreeMap<K, V> {
    fn as_value(&self) -> Value {
        Value::record(
            self.iter()
                .map(|(k, v)| Item::Slot(k.as_value(), v.as_value()))
                .collect(),
        )
    }

    fn into_value(self) -> Value {
        Value::record(
            self.into_iter()
                .map(|(k, v)| Item::Slot(k.into_value(), v.into_value()))
                .collect(),
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                items
                    .iter()
                    .try_fold(
                        BTreeMap::new(),
                        |mut results: BTreeMap<K, V>, item| match item {
                            Item::Slot(k, v) => {
                                let key = K::try_from_value(k)?;
                                let value = V::try_from_value(v)?;
                                results.insert(key, value);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i.to_owned())),
                        },
                    )
            }
            v => de_incorrect_type("BTreeMap<K, V>", v),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                items
                    .into_iter()
                    .try_fold(
                        BTreeMap::new(),
                        |mut results: BTreeMap<K, V>, item| match item {
                            Item::Slot(k, v) => {
                                let key = K::try_convert(k)?;
                                let value = V::try_convert(v)?;
                                results.insert(key, value);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i)),
                        },
                    )
            }
            v => de_incorrect_type("BTreeMap<K, V>", &v),
        }
    }
}

impl<K: Ord + ValidatedForm, V: ValidatedForm> ValidatedForm for BTreeMap<K, V> {
    fn schema() -> StandardSchema {
        StandardSchema::map(
            <K as ValidatedForm>::schema(),
            <V as ValidatedForm>::schema(),
        )
        .and(StandardSchema::NumAttrs(0))
    }
}

impl<K: Ord + Clone + Form, V: Clone + Form> Form for OrdMap<K, V> {
    fn as_value(&self) -> Value {
        Value::record(
            self.iter()
                .map(|(k, v)| Item::Slot(k.as_value(), v.as_value()))
                .collect(),
        )
    }

    fn into_value(self) -> Value {
        Value::record(
            self.into_iter()
                .map(|(k, v)| Item::Slot(k.into_value(), v.into_value()))
                .collect(),
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => items.iter().try_fold(
                OrdMap::new(),
                |mut results: OrdMap<K, V>, item| match item {
                    Item::Slot(k, v) => {
                        let key = K::try_from_value(k)?;
                        let value = V::try_from_value(v)?;
                        results.insert(key, value);
                        Ok(results)
                    }
                    i => Err(FormDeserializeErr::IllegalItem(i.to_owned())),
                },
            ),
            v => de_incorrect_type("OrdMap<K, V>", v),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                items
                    .into_iter()
                    .try_fold(
                        OrdMap::new(),
                        |mut results: OrdMap<K, V>, item| match item {
                            Item::Slot(k, v) => {
                                let key = K::try_convert(k)?;
                                let value = V::try_convert(v)?;
                                results.insert(key, value);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i)),
                        },
                    )
            }
            v => de_incorrect_type("OrdMap<K, V>", &v),
        }
    }
}

impl<K: Ord + Clone + ValidatedForm, V: Clone + ValidatedForm> ValidatedForm for OrdMap<K, V> {
    fn schema() -> StandardSchema {
        StandardSchema::map(
            <K as ValidatedForm>::schema(),
            <V as ValidatedForm>::schema(),
        )
        .and(StandardSchema::NumAttrs(0))
    }
}
