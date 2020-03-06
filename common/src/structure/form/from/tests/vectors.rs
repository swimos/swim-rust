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

use serde::{Deserialize};

use crate::model::{Attr, Item, Value};
use crate::structure::form::Form;




#[test]
fn nested_vectors() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Test {
        seq: Vec<Vec<i32>>,
    }

    let expected = Test {
        seq: vec![vec![1, 2], vec![3, 4]],
    };

    let record = Value::Record(
        vec![Attr::of("Test")],
        vec![Item::slot(
            "seq",
            Value::from_vec(vec![
                Item::from(Value::record(vec![Item::from(1), Item::from(2)])),
                Item::from(Value::record(vec![Item::from(3), Item::from(4)])),
            ]),
        )],
    );
    let parsed_value = Form::default().from_value::<Test>(&record).unwrap();

    assert_eq!(parsed_value, expected);
}

#[test]
fn vector_of_structs() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Parent {
        seq: Vec<Child>,
    }

    #[derive(Deserialize, PartialEq, Debug)]
    struct Child {
        id: i32,
    }

    let expected = Parent {
        seq: vec![Child { id: 1 }, Child { id: 2 }, Child { id: 3 }],
    };

    let record = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::slot(
            "seq",
            Value::from_vec(vec![
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 1)],
                )),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 2)],
                )),
                Item::ValueItem(Value::Record(
                    vec![Attr::of("Child")],
                    vec![Item::slot("id", 3)],
                )),
            ]),
        )],
    );

    let parsed_value = Form::default().from_value::<Parent>(&record).unwrap();

    assert_eq!(parsed_value, expected);
}

#[test]
fn vector_of_tuples() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Parent {
        seq: Vec<(i32, i32)>,
    }

    let expected = Parent { seq: vec![(1, 2), (3, 4), (5, 6)] };

    let record = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::slot(
            "seq",
            Value::from_vec(vec![
                Item::ValueItem(Value::record(vec![Item::from(1), Item::from(2)])),
                Item::ValueItem(Value::record(vec![Item::from(3), Item::from(4)])),
                Item::ValueItem(Value::record(vec![Item::from(5), Item::from(6)])),
            ]),
        )],
    );

    let parsed_value = Form::default().from_value::<Parent>(&record).unwrap();

    assert_eq!(parsed_value, expected);
}

#[test]
fn simple_vector() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct Parent {
        seq: Vec<i32>,
    }

    let expected = Parent {
        seq: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    };

    let record = Value::Record(
        vec![Attr::of("Parent")],
        vec![Item::Slot(
            Value::from("seq"),
            Value::record(vec![
                Item::from(1),
                Item::from(2),
                Item::from(3),
                Item::from(4),
                Item::from(5),
                Item::from(6),
                Item::from(7),
                Item::from(8),
                Item::from(9),
                Item::from(10),
            ]),
        )],
    );

    let parsed_value = Form::default().from_value::<Parent>(&record).unwrap();

    assert_eq!(parsed_value, expected);
}
