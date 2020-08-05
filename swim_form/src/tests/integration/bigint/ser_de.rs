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

use crate::Form;
use common::model::Value;
use form_derive::*;
use num_bigint::{BigInt, BigUint};

#[form(Value)]
struct S {
    #[form(bigint)]
    a: BigInt,
    #[form(biguint)]
    b: BigUint,
}

fn main() {
    let mut rng = rand::thread_rng();

    let s = S {
        a: rng.gen_bigint(100),
        b: rng.gen_biguint(100),
    };

    let rec = s.as_value();
}
