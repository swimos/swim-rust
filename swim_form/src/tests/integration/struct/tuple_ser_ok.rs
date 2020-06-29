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

fn main() {
    #[form]
    #[derive(PartialEq)]
    struct Parent(i32, i32);

    let record = Value::Record(
        vec![Attr::from("Parent")],
        vec![Item::from(1), Item::from(2)],
    );

    let parent = Parent(1, 2);
    let result = parent.as_value();

    assert_eq!(result, record)
}
