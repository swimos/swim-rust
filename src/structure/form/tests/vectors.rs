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
use crate::structure::form::compound::{to_string};

use serde::Serialize;

#[test]
fn nested_vectors() {
    #[derive(Serialize)]
    struct Test {
        seq: Vec<Vec<&'static str>>,
    }

    let test = Test {
        seq: vec![
            vec!["a", "b"],
            vec!["c", "d"]
        ],
    };

    let parsed_value = to_string(&test).unwrap();
    let expected = Value::Record(Vec::new(), vec![
        Item::Slot(Value::Text(String::from("seq")), Value::Record(Vec::new(), vec![
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("a"))),
                Item::ValueItem(Value::Text(String::from("b"))),
            ])),
            Item::ValueItem(Value::Record(Vec::new(), vec![
                Item::ValueItem(Value::Text(String::from("c"))),
                Item::ValueItem(Value::Text(String::from("d"))),
            ]))
        ]))]);

    assert_eq!(parsed_value, expected);
}