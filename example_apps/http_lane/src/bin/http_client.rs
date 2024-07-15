// Copyright 2015-2024 Swim Inc.
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

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let lane = "http://127.0.0.1:8080/example/1?lane=http_lane";

    let client = reqwest::Client::new();
    client.put(lane).body("13").send().await?;

    let response = client.get(lane).send().await?;
    let body = response.bytes().await?;

    let num = std::str::from_utf8(body.as_ref())?.parse::<i32>()?;
    assert_eq!(13, num);

    Ok(())
}
