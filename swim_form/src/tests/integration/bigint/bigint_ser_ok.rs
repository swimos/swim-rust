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

use form_derive::*;
use swim_common::model::{Attr, Item, Value};
use swim_form::*;

fn main() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Parent {
        #[form(bigint)]
        a: BigInt,
        #[form(biguint)]
        b: BigUint,
    }

    let record = Value::Record(
        vec![Attr::from("Parent")],
        vec![
            Item::from(("a", Value::BigInt(BigInt::from(100)))),
            Item::from(("b", Value::BigUint(BigUint::from(100u32)))),
        ],
    );

    let parent = Parent {
        a: BigInt::from(100),
        b: BigUint::from(100u32),
    };
    let result = parent.as_value();

    assert_eq!(result, record)
}
