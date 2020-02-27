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
use crate::model::Item::ValueItem;
use crate::structure::form::form::{SerializerError, to_value};
use crate::structure::form::tests::assert_err;

#[cfg(test)]
mod valid_types {
    use super::*;

    use std::collections::{HashMap, BTreeMap};

    #[test]
    fn simple_map() {
        let mut map = BTreeMap::new();
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("c", 3);

        let parsed_value = to_value(&map).unwrap();

        let expected = Value::Record(Vec::new(), vec![
            Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
            Item::Slot(Value::Text(String::from("b")), Value::Int32Value(2)),
            Item::Slot(Value::Text(String::from("c")), Value::Int32Value(3)),

        ]);

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod invalid_types {
    use super::*;

}