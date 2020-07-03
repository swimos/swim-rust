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
use form_derive::*;

#[form(Value)]
struct FormStruct {
    a: i32,
    b: Child,
}

struct Child {
    a: i32,
}

fn main() {
    let _ = FormStruct {
        a: 1,
        b: Child { a: 2 },
    };
}
