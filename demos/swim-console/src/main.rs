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
use swim_client::connections::factory::tungstenite::TungsteniteWsFactory;
use swim_client::interface::SwimClient;
use swim_common::warp::path::AbsolutePath;

#[tokio::main]
async fn main() {
    let mut client = SwimClient::new_with_default().await;
    let path = AbsolutePath::new(
        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
        "/unit/foo",
        "random",
    );

    let (_downlink, mut receiver) = client.value_downlink::<i32>(path, 0).await.unwrap();

    println!("Opened downlink");

    let mut values = Vec::new();
    let mut averages = Vec::new();
    let window_size = 200;

    while let Some(event) = receiver.next().await {
        let i = event.get_inner();
        values.push(i.clone());

        if values.len() > window_size {
            values.remove(0);
            averages.remove(0);
        }

        if !values.is_empty() {
            let sum = values.iter().fold(0, |total, x| total + *x);
            let sum = sum as f64 / values.len() as f64;

            println!("Average: {:?}", sum);

            averages.push(sum);
        }
    }
}
