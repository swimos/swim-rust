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

use std::{collections::HashMap, time::SystemTime};

use futures::{future::join, TryFutureExt};
use rand::Rng;
use swimos_connector::{
    BaseConnector, ConnectorAgent, EgressConnector, EgressConnectorSender, MessageSource,
    SendResult,
};
use swimos_model::{Item, Value};
use swimos_utilities::trigger;
use tokio::time::sleep;

use crate::config::TopicSpecifier;
use crate::{
    connector::test_util::{create_kafka_props, run_handler_with_futures},
    DataFormat, EgressLaneSpec, Endianness, ExtractionSpec, KafkaEgressConfiguration,
    KafkaEgressConnector, KafkaLogLevel,
};
const LANE: &str = "lane";

fn make_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        properties: create_kafka_props(),
        log_level: KafkaLogLevel::Debug,
        key_serializer: DataFormat::Int32(Endianness::BigEndian),
        payload_serializer: DataFormat::Json,
        fixed_topic: Some("cellular-integer-json".to_string()),
        value_lanes: vec![EgressLaneSpec {
            name: LANE.to_string(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                key_selector: Some("$value.key".to_string()),
                payload_selector: Some("$value.payload".to_string()),
            },
        }],
        map_lanes: vec![],
        value_downlinks: vec![],
        map_downlinks: vec![],
        retry_timeout_ms: 5000,
    }
}

fn make_record() -> Value {
    let mut rng = rand::thread_rng();
    let severity = rng.gen_range(0.0..2.0);
    let mean_ul_sinr = rng.gen_range(1..50);
    let rrc_re_establishment_failures = rng.gen_range(1u32..10u32);
    let recorded_time = i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time out of range.")
            .as_millis(),
    )
    .expect("Time out of range.");
    let status = Value::record(vec![Item::slot("severity", severity)]);
    let ran_latest = Value::record(vec![
        Item::slot("mean_ul_sinr", mean_ul_sinr),
        Item::slot(
            "rrc_re_establishment_failures",
            rrc_re_establishment_failures,
        ),
        Item::slot("recorded_time", recorded_time),
    ]);
    let key = Value::from(rng.gen_range(50..4000));
    let payload = Value::record(vec![
        Item::slot("status", status),
        Item::slot("ranLatest", ran_latest),
    ]);
    Value::record(vec![Item::slot("key", key), Item::slot("payload", payload)])
}

#[tokio::test]
#[ignore] // Ignored by default as this relies on an external service being present.
async fn drive_connector() {
    let config = make_config();
    let connector = KafkaEgressConnector::for_config(config);
    let agent = ConnectorAgent::default();

    let (tx, rx) = trigger::trigger();

    let on_start = run_handler_with_futures(&agent, connector.on_start(tx));

    let (_, result) = join(on_start, rx).await;
    result.expect("Creating lanes failed.");

    let sender = connector
        .make_sender(&HashMap::new())
        .expect("Creating sender failed.");

    for _ in 0..5 {
        let record = make_record();
        match sender
            .send(MessageSource::Lane(LANE), None, &record)
            .expect("No result.")
        {
            SendResult::Suspend(fut) => {
                let handler = fut.into_future().await.expect("Send failed.");
                run_handler_with_futures(&agent, handler).await;
            }
            SendResult::RequestCallback(_, _) => panic!("Queue filled."),
            SendResult::Fail(err) => panic!("Send failed: {}", err),
        }
        sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
