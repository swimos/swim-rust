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

use std::time::Duration;

use bytes::Bytes;
use futures::future::join;
use rand::{thread_rng, Rng};
use rumqttc::{ClientError, MqttOptions, QoS};
use swimos_utilities::trigger;
use tracing::debug;

pub const TOPIC: &str = "test/ingress";

pub async fn generate_data(
    mqtt_uri: String,
    mut stop: trigger::Receiver,
) -> Result<(), Box<dyn std::error::Error>> {
    let opts = MqttOptions::parse_url(mqtt_uri)?;

    let (client, mut event_loop) = rumqttc::AsyncClient::new(opts, 0);
    let stop_cpy = stop.clone();
    let events_task = async move {
        loop {
            tokio::select! {
                 biased;
                 _ = &mut stop => break Ok(()),
                 event = event_loop.poll() => {
                     match event {
                         Ok(ev) => debug!(event = ?ev, "Processed MQTT event."),
                         Err(err) => {
                             break Err(err);
                         }
                     }
                 }
            }
        }
    };

    let gen_task = async move {
        let mut rand = thread_rng();
        loop {
            if stop_cpy.check_state().is_some() {
                break;
            }
            let mut data = String::new();
            for _ in 0..10 {
                data.push(rand.gen_range('A'..='Z'));
            }
            client
                .publish_bytes(TOPIC, QoS::AtMostOnce, true, Bytes::from(data))
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(()) as Result<(), ClientError>
    };

    let (ev_res, gen_res) = join(events_task, gen_task).await;
    ev_res?;
    gen_res?;
    Ok(())
}
