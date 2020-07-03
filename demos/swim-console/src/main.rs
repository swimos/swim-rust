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

use futures::StreamExt;
use swim_client::common::model::Value;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::interface::SwimClient;
use swim_form::*;

#[form(Value)]
struct S {
    // #[serde(serialize_with = "swim_form::bigint::serialize_bigint")]
    #[swim(bigint)]
    b: BigInt,
}

#[tokio::main]
async fn main() {
    let s = S {
        b: BigInt::from(100),
    };
}
