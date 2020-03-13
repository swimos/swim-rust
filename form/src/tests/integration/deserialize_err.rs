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

use form::Form;
use form_derive::*;
use common::model::{Value, Attr, Item};
use deserialize::FormDeserializeErr;

fn main() {
    #[form]
    #[derive(Debug, PartialEq)]
    struct Parent {
        a:i32
    }

    let record = Value::Record(
        vec![Attr::from("Parent")],
        vec![
            Item::from(("a", 1.0)),
        ],
    );

    let result = Parent::try_from_value(&record);

    match result {
        Ok(_) => panic!(),
        Err(e) => assert_eq!(e, FormDeserializeErr::IncorrectType(String::from("Expected: i32, found: 1e0")))
    }
}