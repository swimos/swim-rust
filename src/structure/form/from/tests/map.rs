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

use serde::Serialize;

use crate::model::{Item, Value};

#[cfg(test)]
mod valid_types {
    use std::collections::BTreeMap;

    use super::*;
    use crate::model::Attr;
    use crate::structure::form::from::to_value;

    #[test]
    fn simple_map() {
        let mut map = BTreeMap::new();
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("c", 3);

        let parsed_value = to_value(&map).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::Slot(Value::Text(String::from("a")), Value::Int32Value(1)),
                Item::Slot(Value::Text(String::from("b")), Value::Int32Value(2)),
                Item::Slot(Value::Text(String::from("c")), Value::Int32Value(3)),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn map_of_vecs() {
        let mut map = BTreeMap::new();
        map.insert("a", vec![1, 2, 3]);
        map.insert("b", vec![1, 2, 3]);

        let parsed_value = to_value(&map).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::Slot(
                    Value::Text(String::from("a")),
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(1)),
                            Item::ValueItem(Value::Int32Value(2)),
                            Item::ValueItem(Value::Int32Value(3)),
                        ],
                    ),
                ),
                Item::Slot(
                    Value::Text(String::from("b")),
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(1)),
                            Item::ValueItem(Value::Int32Value(2)),
                            Item::ValueItem(Value::Int32Value(3)),
                        ],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn map_of_tuples() {
        let mut map = BTreeMap::new();
        map.insert("a", (1, 2, 3, 4, 5));
        map.insert("b", (6, 7, 8, 9, 10));

        let parsed_value = to_value(&map).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::Slot(
                    Value::Text(String::from("a")),
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(1)),
                            Item::ValueItem(Value::Int32Value(2)),
                            Item::ValueItem(Value::Int32Value(3)),
                            Item::ValueItem(Value::Int32Value(4)),
                            Item::ValueItem(Value::Int32Value(5)),
                        ],
                    ),
                ),
                Item::Slot(
                    Value::Text(String::from("b")),
                    Value::Record(
                        Vec::new(),
                        vec![
                            Item::ValueItem(Value::Int32Value(6)),
                            Item::ValueItem(Value::Int32Value(7)),
                            Item::ValueItem(Value::Int32Value(8)),
                            Item::ValueItem(Value::Int32Value(9)),
                            Item::ValueItem(Value::Int32Value(10)),
                        ],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn map_of_structs() {
        #[derive(Serialize)]
        struct Test {
            a: f64,
        }

        let mut map = BTreeMap::new();
        map.insert("a", Test { a: 1.0 });
        map.insert("b", Test { a: 2.0 });

        let parsed_value = to_value(&map).unwrap();

        let expected = Value::Record(
            Vec::new(),
            vec![
                Item::Slot(
                    Value::Text(String::from("a")),
                    Value::Record(
                        vec![Attr::of("Test")],
                        vec![Item::Slot(
                            Value::Text(String::from("a")),
                            Value::Float64Value(1.0),
                        )],
                    ),
                ),
                Item::Slot(
                    Value::Text(String::from("b")),
                    Value::Record(
                        vec![Attr::of("Test")],
                        vec![Item::Slot(
                            Value::Text(String::from("a")),
                            Value::Float64Value(2.0),
                        )],
                    ),
                ),
            ],
        );

        assert_eq!(parsed_value, expected);
    }
}

#[cfg(test)]
mod invalid_types {
    use std::collections::BTreeMap;

    use crate::structure::form::from::tests::assert_err;
    use crate::structure::form::from::{to_value, SerializerError};

    #[test]
    fn invalid_nested_type() {
        let mut map: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
        map.insert("a", vec![1, 2, 3]);
        map.insert("b", vec![1, 2, 3]);

        let parsed_value = to_value(&map);
        assert_err(
            parsed_value,
            SerializerError::UnsupportedType(String::from("u32")),
        );
    }
}
