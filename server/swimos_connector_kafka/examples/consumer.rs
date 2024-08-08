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

use std::{collections::HashMap, pin::pin};

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    error::KafkaError,
    ClientConfig, ClientContext, Message,
};
use swimos_connector_kafka::{
    Endianness, I32Deserializer, JsonDeserializer, MessageDeserializer, MessagePart, MessageView,
};

fn kafka_props() -> HashMap<String, String> {
    [
        ("bootstrap.servers", "datagen.nstream.cloud:9092"),
        ("message.timeout.ms", "5000"),
        ("group.id", "rust-consumer-test"),
        ("auto.offset.reset", "smallest"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

const TOPIC: &str = "cellular-integer-json";

type LoggingConsumer = StreamConsumer<Context>;

async fn consumer_task() -> Result<(), KafkaError> {
    let context = Context;

    let mut config = ClientConfig::new();

    for (k, v) in kafka_props() {
        config.set(k, v);
    }

    let mut stop_signal = pin!(tokio::signal::ctrl_c());

    let consumer: LoggingConsumer = config
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)?;

    consumer.subscribe(&[TOPIC])?;

    let key_deser = I32Deserializer::new(Endianness::LittleEndian);
    let payload_deser = JsonDeserializer;

    loop {
        let message = tokio::select! {
            biased;
            _ = &mut stop_signal => break,
            message = consumer.recv() => message?,
        };
        let view = MessageView {
            topic: message.topic(),
            key: message.key().unwrap_or_default(),
            payload: message.payload().unwrap_or_default(),
        };

        let key = key_deser
            .deserialize(&view, MessagePart::Key)
            .expect("Invalid key.");
        let payload = payload_deser
            .deserialize(&view, MessagePart::Payload)
            .expect("Invalid payload.");

        println!("Key: {}, Payload: {}", key, payload);
    }
    Ok(())
}

struct Context;

impl ClientContext for Context {}

impl ConsumerContext for Context {}

#[tokio::main]
async fn main() {
    consumer_task().await.expect("Failed")
}
