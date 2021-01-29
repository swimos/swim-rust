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

use std::time::Duration;
use swim_client::interface::SwimClient;
use swim_common::model::Value;
use swim_common::warp::path::AbsolutePath;
use swim_runtime::time::delay::delay_for;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let host_uri = url::Url::parse(&"ws://127.0.0.1:9001".to_string()).unwrap();
    let node_uri = "unit/foo";
    let lane_uri = "publish";

    let path = AbsolutePath::new(host_uri.clone(), node_uri, lane_uri);

    for _ in 0..10 {
        client
            .send_command(path.clone(), Value::Extant)
            .await
            .expect("Failed to send command!");

        delay_for(Duration::from_secs(5)).await;
    }

    println!("Stopping client in 2 seconds");
    delay_for(Duration::from_secs(2)).await;
}
