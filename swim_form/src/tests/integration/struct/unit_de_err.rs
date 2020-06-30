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

use common::model::{Attr, Item, Value};
use form_derive::*;
use swim_form::Form;
use swim_form::_deserialize::FormDeserializeErr;

fn main() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent;

    let record = Value::Record(vec![Attr::from("Incorrect")], vec![]);

    let result = Parent::try_from_value(&record);
    match result {
        Ok(_) => panic!("Expected failure"),
        Err(e) => assert_eq!(e, FormDeserializeErr::Malformatted),
    }

    let record = Value::Record(vec![Attr::from("Parent")], vec![Item::from("hello")]);

    if let Err(e) = Parent::try_from_value(&record) {
        panic!(
            "Expected deserializer to parse record and discard fields. Err: {:?}",
            e
        );
    }
}
