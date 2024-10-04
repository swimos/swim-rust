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

use std::collections::HashMap;

use swimos_api::{address::Address, agent::WarpLaneKind};
use swimos_connector::EgressContext;

use crate::{
    config::{EgressDownlinkSpec, KafkaEgressConfiguration, TopicSpecifier},
    DataFormat, DownlinkAddress, ExtractionSpec, KafkaLogLevel,
};

use super::open_downlinks;

#[cfg(feature = "json")]
mod end_to_end;
mod integration;

const FIXED_TOPIC: &str = "fixed";

fn empty_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        properties: HashMap::new(),
        log_level: KafkaLogLevel::Warning,
        key_serializer: DataFormat::Recon,
        payload_serializer: DataFormat::Recon,
        fixed_topic: Some(FIXED_TOPIC.to_string()),
        value_lanes: vec![],
        map_lanes: vec![],
        value_downlinks: vec![],
        map_downlinks: vec![],
        retry_timeout_ms: 5000,
    }
}

const HOST: &str = "ws://host:9001";
const NODE1: &str = "/node1";
const NODE2: &str = "/node2";
const LANE: &str = "lane";

fn addr1() -> DownlinkAddress {
    DownlinkAddress {
        host: Some(HOST.to_string()),
        node: NODE1.to_string(),
        lane: LANE.to_string(),
    }
}

fn addr2() -> DownlinkAddress {
    DownlinkAddress {
        host: Some(HOST.to_string()),
        node: NODE2.to_string(),
        lane: LANE.to_string(),
    }
}

fn downlinks_config() -> KafkaEgressConfiguration {
    KafkaEgressConfiguration {
        value_downlinks: vec![EgressDownlinkSpec {
            address: addr1(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                key_selector: None,
                payload_selector: None,
            },
        }],
        map_downlinks: vec![EgressDownlinkSpec {
            address: addr2(),
            extractor: ExtractionSpec {
                topic_specifier: TopicSpecifier::Fixed,
                key_selector: Some("$key".to_string()),
                payload_selector: None,
            },
        }],
        ..empty_config()
    }
}

/* #[tokio::test]
async fn open_lanes() {
    let config = lanes_config();
    let agent = ConnectorAgent::default();
    let selectors = MessageSelectors::try_from(&config).expect("Bad configuration.");

    let semaphore = Arc::new(Semaphore::new(0));
    let (done_tx, done_rx) = trigger::trigger();
    let handler = selectors.open_lanes(done_tx, semaphore, 0);

    let handler_task = run_handler_with_futures(&agent, handler);

    let (modified, done_result) = timeout(TEST_TIMEOUT, join(handler_task, done_rx))
        .await
        .expect("Test timed out.");

    assert!(modified.is_empty());
    assert!(done_result.is_ok());

    let expected_value_lanes = [VALUE_LANE.to_string()].into_iter().collect::<HashSet<_>>();
    let expected_map_lanes = [MAP_LANE.to_string()].into_iter().collect::<HashSet<_>>();

    assert_eq!(agent.value_lanes(), expected_value_lanes);
    assert_eq!(agent.map_lanes(), expected_map_lanes);
} */

#[derive(Default, Debug, PartialEq, Eq)]
struct TestEgressContext {
    lanes: Vec<(String, WarpLaneKind)>,
    value: Vec<Address<String>>,
    map: Vec<Address<String>>,
}

impl EgressContext for TestEgressContext {
    fn open_event_downlink(&mut self, address: Address<&str>) {
        self.value.push(address.owned());
    }

    fn open_map_downlink(&mut self, address: Address<&str>) {
        self.map.push(address.owned());
    }

    fn open_lane(&mut self, name: &str, kind: WarpLaneKind) {
        self.lanes.push((name.to_string(), kind));
    }
}

#[test]
fn open_downlinks_from_config() {
    let config = downlinks_config();
    let mut context = TestEgressContext::default();
    open_downlinks(&config, &mut context);

    let expected = TestEgressContext {
        lanes: vec![],
        value: vec![Address::from(&addr1())],
        map: vec![Address::from(&addr2())],
    };

    assert_eq!(context, expected);
}
