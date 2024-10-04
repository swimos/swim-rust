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

use crate::{
    connector::test_util::create_kafka_props, DataFormat, Endianness, IngressMapLaneSpec,
    IngressValueLaneSpec, KafkaIngressConfiguration, KafkaIngressConnector, KafkaLogLevel,
};
use futures::{future::join, TryStreamExt};
use swimos_connector::{BaseConnector, ConnectorAgent, IngressConnector};
use swimos_utilities::trigger;

use crate::connector::test_util::{run_handler, run_handler_with_futures, TestSpawner};

fn make_config() -> KafkaIngressConfiguration {
    KafkaIngressConfiguration {
        properties: create_kafka_props(),
        log_level: KafkaLogLevel::Debug,
        value_lanes: vec![IngressValueLaneSpec::new(Some("latest_key"), "$key", true)],
        map_lanes: vec![IngressMapLaneSpec::new(
            "times",
            "$payload.ranLatest.mean_ul_sinr",
            "$payload.ranLatest.recorded_time",
            false,
            true,
        )],
        key_deserializer: DataFormat::Int32(Endianness::BigEndian),
        payload_deserializer: DataFormat::Json,
        topics: vec!["cellular-integer-json".to_string()],
        relays: Default::default(),
    }
}

#[tokio::test]
#[ignore] // Ignored by default as this relies on an external service being present.
async fn drive_connector() {
    let config = make_config();
    let connector = KafkaIngressConnector::for_config(config);
    let agent = ConnectorAgent::default();

    let (tx, rx) = trigger::trigger();

    let on_start = run_handler_with_futures(&agent, connector.on_start(tx));

    let (_, result) = join(on_start, rx).await;
    result.expect("Creating lanes failed.");

    let mut stream = connector.create_stream().expect("Failed to open consumer.");

    let spawner = TestSpawner::default();

    for _ in 0..10 {
        let handler = if let Some(handler) = stream
            .try_next()
            .await
            .expect("Failed to handle Kafka message.")
        {
            handler
        } else {
            panic!("Terminated early.");
        };
        run_handler(&agent, &spawner, handler);
    }
}
