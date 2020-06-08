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

use clap::{App, Arg};

use client::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use client::connections::factory::tungstenite::TungsteniteWsFactory;
use client::interface::SwimClient;
use common::model::Value;
use common::warp::path::AbsolutePath;
use futures::StreamExt;
use tokio::time::Duration;
use tracing::info;

const WINDOW_SIZE_NAME: &str = "window size";

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("rolling_average=trace".parse().unwrap());

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .init();
}

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2, Default::default()).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(600),
        5,
        OnInvalidMessage::Terminate,
        10000,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

struct RollingAverage {
    window_size: usize,
    client: SwimClient,
}

impl RollingAverage {
    async fn new(window_size: usize) -> RollingAverage {
        let client = SwimClient::new(config(), TungsteniteWsFactory::new(5).await).await;

        RollingAverage {
            window_size,
            client,
        }
    }

    async fn run(self) {
        let RollingAverage {
            window_size,
            mut client,
        } = self;
        let path = AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/unit/foo",
            "random",
        );

        let (_downlink, mut receiver) = client.value_downlink(path, Value::Extant).await.unwrap();
        let mut values = Vec::new();

        while let Some(event) = receiver.next().await {
            if let Value::Int32Value(i) = event.action() {
                values.push(i.clone());

                if values.len() > window_size {
                    values.remove(0);
                }

                if values.is_empty() {
                    info!("Average: 0");
                } else {
                    let sum = values.iter().fold(0, |total, x| total + *x);
                    let sum = sum as f64 / values.len() as f64;

                    info!("Average: {:?}", sum);
                }
            } else {
                panic!("Expected Int32 value");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    init_tracing();

    let matches = App::new("Moving average calculator")
        .version("1.0")
        .author("SWIM.AI developers")
        .arg(
            Arg::with_name(WINDOW_SIZE_NAME)
                .short("w")
                .long(WINDOW_SIZE_NAME)
                .help("Window size")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let window_size = matches
        .value_of(WINDOW_SIZE_NAME)
        .unwrap()
        .parse::<usize>()
        .expect("Invalid window size");

    let rolling_average = RollingAverage::new(window_size).await;
    rolling_average.run().await;
}
