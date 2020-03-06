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

use serde::Deserialize;

use crate::model::{Item, Value};

#[cfg(test)]
mod valid_types {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::model::Attr;
    use crate::structure::form::Form;

    use super::*;

    #[test]
    fn set() {
        let mut set = BTreeSet::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);
        set.insert(4);
        set.insert(5);

        let record = Value::Record(
            Vec::new(),
            vec![
                Item::from(1),
                Item::from(2),
                Item::from(3),
                Item::from(4),
                Item::from(5),
            ],
        );

        let parsed_value = Form::default()
            .from_value::<BTreeSet<i32>>(&record)
            .unwrap();
        assert_eq!(parsed_value, set);
    }

    #[test]
    fn simple_map() {
        let mut map = BTreeMap::new();
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("c", 3);

        let record = Value::Record(
            Vec::new(),
            vec![Item::slot("a", 1), Item::slot("b", 2), Item::slot("c", 3)],
        );

        let parsed_value = Form::default()
            .from_value::<BTreeMap<&str, i32>>(&record)
            .unwrap();

        assert_eq!(parsed_value, map);
    }

    #[test]
    fn map_of_vecs() {
        let mut map = BTreeMap::new();
        map.insert("a", vec![1, 2, 3]);
        map.insert("b", vec![1, 2, 3]);

        let record = Value::Record(
            Vec::new(),
            vec![
                Item::Slot(
                    Value::from("a"),
                    Value::record(vec![Item::from(1), Item::from(2), Item::from(3)]),
                ),
                Item::Slot(
                    Value::from("b"),
                    Value::record(vec![Item::from(1), Item::from(2), Item::from(3)]),
                ),
            ],
        );

        let parsed_value = Form::default()
            .from_value::<BTreeMap<&str, Vec<i32>>>(&record)
            .unwrap();

        assert_eq!(parsed_value, map);
    }

    #[test]
    fn map_of_tuples() {
        let mut map = BTreeMap::new();
        map.insert("a", (1, 2, 3, 4, 5));
        map.insert("b", (6, 7, 8, 9, 10));

        let record = Value::record(vec![
            Item::slot(
                "a",
                Value::record(vec![
                    Item::from(1),
                    Item::from(2),
                    Item::from(3),
                    Item::from(4),
                    Item::from(5),
                ]),
            ),
            Item::slot(
                "b",
                Value::record(vec![
                    Item::from(6),
                    Item::from(7),
                    Item::from(8),
                    Item::from(9),
                    Item::from(10),
                ]),
            ),
        ]);

        let parsed_value = Form::default()
            .from_value::<BTreeMap<&str, (i32, i32, i32, i32, i32)>>(&record)
            .unwrap();

        assert_eq!(parsed_value, map);
    }

    #[test]
    fn map_of_structs() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: f64,
        }

        let mut map = BTreeMap::new();
        map.insert("a", Test { a: 1.0 });
        map.insert("b", Test { a: 2.0 });

        let record = Value::Record(
            Vec::new(),
            vec![
                Item::slot(
                    "a",
                    Value::Record(vec![Attr::of("Test")], vec![Item::slot("a", 1.0)]),
                ),
                Item::Slot(
                    Value::from("b"),
                    Value::Record(vec![Attr::of("Test")], vec![Item::slot("a", 2.0)]),
                ),
            ],
        );

        let parsed_value = Form::default()
            .from_value::<BTreeMap<&str, Test>>(&record)
            .unwrap();

        assert_eq!(parsed_value, map);
    }
}
