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

use common::model::Value;

use crate::{Form, SerializeToValue};

mod swim_form {
    pub use crate::*;
}

#[test]
fn single_derve() {
    #[form(Value)]
    struct FormStruct {
        a: i32,
    }

    let fs = FormStruct { a: 1 };
    let _v = fs.as_value();
}

#[test]
fn single_derve_with_generics() {
    #[form(Value)]
    struct FormStruct<V>
    where
        V: SerializeToValue,
    {
        v: V,
    }

    let fs = FormStruct { v: 1 };
    let _v = fs.as_value();
}

#[test]
fn nested_derives() {
    #[form(Value)]
    struct Parent {
        a: i32,
        b: Child,
    }

    #[form(Value)]
    struct Child {
        c: i32,
    }

    let fs = Parent {
        a: 1,
        b: Child { c: 1 },
    };

    let _v = fs.as_value();
}
