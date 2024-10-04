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

use futures::{future::join, FutureExt};
use swimos_api::agent::WarpLaneKind;
use swimos_utilities::trigger;
use tokio::time::{timeout, Duration};

use crate::{
    lifecycle::fixture::{LaneRecord, RequestsRecord},
    ConnectorAgent,
};

use super::{fixture::run_handle_with_futs, open_lanes};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

#[tokio::test]
async fn open_connector_lanes() {
    let lanes = vec![
        (VALUE_LANE.to_string(), WarpLaneKind::Value),
        (MAP_LANE.to_string(), WarpLaneKind::Map),
    ];

    let (tx, rx) = trigger::trigger();

    let handler = open_lanes(lanes, tx);
    let agent = ConnectorAgent::default();

    let handler_task = run_handle_with_futs(&agent, handler).map(|r| r.expect("Handler failed."));

    let (requests, done_result) = timeout(TEST_TIMEOUT, join(handler_task, rx))
        .await
        .expect("Test timed out.");

    let RequestsRecord {
        downlinks,
        timers,
        lanes,
    } = requests;
    assert!(downlinks.is_empty());
    assert!(timers.is_empty());
    assert!(done_result.is_ok());

    let expected_lanes = [
        (VALUE_LANE.to_string(), WarpLaneKind::Value),
        (MAP_LANE.to_string(), WarpLaneKind::Map),
    ]
    .into_iter()
    .collect::<HashMap<_, _>>();

    assert_eq!(lanes.len(), 2);
    let lanes_map = lanes
        .into_iter()
        .map(|LaneRecord { name, kind, .. }| (name, kind))
        .collect::<HashMap<_, _>>();
    assert_eq!(lanes_map, expected_lanes);
}
