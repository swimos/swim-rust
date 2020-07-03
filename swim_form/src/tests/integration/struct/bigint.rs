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
use std::string::ToString;
use swim_form::Form;

fn main() {
    #[form(Value)]
    #[derive(Debug)]
    pub struct Message {
        #[serde(with = "swim_form::bigint")]
        value: num_bigint::BigInt,
        user_name: String,
        uuid: String,
    }

    let m = Message {
        value: num_bigint::BigInt::from(100),
        user_name: "user_name".to_string(),
        uuid: "uuid".to_string(),
    };

    let r = serde_json::to_string(&m);
    assert_eq!(
        r.unwrap(),
        "{\"value\":\"100\",\"user_name\":\"user_name\",\"uuid\":\"uuid\"}"
    )
}
