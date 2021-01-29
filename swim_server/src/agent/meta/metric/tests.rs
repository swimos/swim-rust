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

use crate::agent::meta::metric::config::MetricCollectorConfig;
use crate::agent::meta::metric::task::ProfileRequestErr;
use crate::agent::meta::metric::uplink::UplinkProfile;
use crate::agent::meta::metric::{MetricCollector, MetricKind, Profile};
use swim_common::warp::path::RelativePath;
use tokio::time::Duration;
use utilities::sync::trigger;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_collector_ok() {
    let (_stop_tx, stop_rx) = trigger::trigger();
    let collector = MetricCollector::new(
        "node".to_string(),
        stop_rx,
        MetricCollectorConfig {
            sample_rate: Duration::from_secs(0),
            ..Default::default()
        },
    );

    let addr = RelativePath::new("/node", "/lane");
    let mut observer = collector.uplink_observer(addr.clone());

    observer.on_command();

    let res = collector
        .request_profile(MetricKind::Uplink(addr.clone()))
        .await;
    assert_eq!(
        res,
        Ok(Profile::Uplink(UplinkProfile {
            command_count: 1,
            ..Default::default()
        }))
    );
}

async fn assert_collector_err(request: MetricKind, expected: ProfileRequestErr) {
    let (_stop_tx, stop_rx) = trigger::trigger();
    let collector = MetricCollector::new(
        "node".to_string(),
        stop_rx,
        MetricCollectorConfig {
            sample_rate: Duration::from_secs(0),
            ..Default::default()
        },
    );

    let result = collector.request_profile(request).await;
    assert_eq!(result, Err(expected));
}

#[tokio::test]
async fn test_collector_no_such_node() {
    assert_collector_err(MetricKind::Node, ProfileRequestErr::NoSuchNode).await;
}

#[tokio::test]
async fn test_collector_no_such_lane() {
    let path = RelativePath::new("/node", "/lane");
    assert_collector_err(
        MetricKind::Lane(path.clone()),
        ProfileRequestErr::NoSuchLane(path),
    )
    .await;
}

#[tokio::test]
async fn test_collector_no_such_uplink() {
    let path = RelativePath::new("/node", "/lane");
    assert_collector_err(
        MetricKind::Uplink(path.clone()),
        ProfileRequestErr::NoSuchUplink(path),
    )
    .await;
}
