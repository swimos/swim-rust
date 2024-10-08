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

use std::{env::args, pin::pin, time::Duration};

use rand::Rng;
use swimos_client::Commander;
use tutorial_app_model::Message;

const NODE: &str = "/unit/master";
const LANE: &str = "publish";
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const DELAY: Duration = Duration::from_millis(750);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = args()
        .collect::<Vec<_>>()
        .get(1)
        .expect("No port provided.")
        .parse::<u16>()
        .expect("Invalid port.");
    let mut commander = Commander::default();

    let host = format!("ws://localhost:{}", port);
    let mut rng = rand::thread_rng();

    let mut shutdown_hook = Box::pin(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
    });
    let mut indicator = 0;
    loop {
        let mut foo_ = rng.gen_range(-5..=5) + 30;
        let mut bar_ = rng.gen_range(-10..=10) + 60;
        let mut baz_ = rng.gen_range(-15..=15) + 90;

        if (indicator / 25) % 2 == 0 {
            foo_ *= 2;
            bar_ *= 2;
            baz_ *= 2;
        }
        let message = Message {
            foo: foo_,
            bar: bar_,
            baz: baz_,
        };
        commander.send_command(&host, NODE, LANE, &message).await?;
        indicator = (indicator + 1) % 1000;

        let delay = pin!(tokio::time::sleep(DELAY));
        tokio::select! {
            biased;
            _ = &mut shutdown_hook => break,
            _ = delay => {},
        }
    }
    tokio::time::timeout(CLOSE_TIMEOUT, commander.close()).await?;
    Ok(())
}
