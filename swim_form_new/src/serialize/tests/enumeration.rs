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

use crate::{Form, SerializeToValue};
use common::model::Value::Int32Value;

mod swim_form {
    pub use crate::*;
}
//
// #[test]
// fn enum_unit() {
//     #[form(Value)]
//     enum E {
//         A,
//     }
//
//     let fe = E::A;
//     let value: Value = fe.as_value();
//     println!("{:?}", value);
// }
//
// #[test]
// fn enum_newtype() {
//     #[form(Value)]
//     enum E {
//         A(i32),
//     }
//
//     let fe = E::A(100);
//     let value: Value = fe.as_value();
//     println!("{:?}", value);
// }
//
// #[test]
// fn enum_tuple() {
//     #[form(Value)]
//     enum E {
//         A(i32, i32, i32, i32),
//     }
//
//     let fe = E::A(1, 2, 3, 4);
//
//     let value: Value = fe.as_value();
//     println!("{:?}", value);
// }

#[test]
fn enum_struct() {
    #[form(Value)]
    enum E {
        A { name: String, age: i32 },
    }

    let fe = E::A {
        name: String::from("Name"),
        age: 30,
    };

    let value: Value = fe.as_value();
    println!("{:?}", value);
}
