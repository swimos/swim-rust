// Copyright 2015-2023 Swim Inc.
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

use anyhow::{anyhow, Result};
use client::{ClientHandle, DownlinkOptions, RawMessage, RemotePath, ValueDownlinkOperationError};
use control_ir::{KafkaConnectorSpec, Pipe};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig as KafkaConfig, Message};
use tracing::{error, trace};

pub async fn run_kafka_connector(
    client: ClientHandle,
    port: usize,
    spec: KafkaConnectorSpec,
) -> Result<()> {
    let KafkaConnectorSpec {
        broker,
        topic,
        group,
        pipe,
    } = spec;

    let Pipe { node, lane } = pipe;
    trace!(broker = ?broker, topic = ?topic, group = ?group, "Starting Kafka connector");

    let consumer: StreamConsumer = KafkaConfig::new()
        .set("group.id", group)
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;

    trace!(?topic, "Created consumer. Subscribing to topic");
    consumer.subscribe(&[topic.as_str()])?;

    let remote = RemotePath::new(format!("ws://127.0.0.1:{port}"), &node, &lane);

    let mut view = client
        .untyped_value_downlink(remote.clone())
        .options(DownlinkOptions::KEEP_LINKED)
        .open()
        .await
        .unwrap();

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let msg = msg.payload().unwrap();

                trace!(
                    ?msg,
                    ?node,
                    ?lane,
                    "Connector received message. Dispatching"
                );

                if let Err(ValueDownlinkOperationError::DownlinkStopped) =
                    view.set(RawMessage::from(msg.to_vec())).await
                {
                    view = client
                        .untyped_value_downlink(remote.clone())
                        .options(DownlinkOptions::KEEP_LINKED)
                        .open()
                        .await
                        .unwrap();
                }
            }
            Err(error) => {
                error!(?error, "Kafka error");
                return Err(anyhow!(error));
            }
        }
    }
}
