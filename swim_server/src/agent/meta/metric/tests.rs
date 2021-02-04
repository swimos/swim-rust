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

use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::LifecycleBase;
use crate::agent::lane::model::supply::make_lane_model;
use crate::agent::lane::strategy::Dropping;
use crate::agent::meta::metric::task::{CollectorStopResult, CollectorTask};
use crate::agent::meta::metric::uplink::UplinkProfile;
use crate::agent::meta::metric::ObserverEvent;
use crate::agent::meta::MetaNodeAddressed;
use crate::routing::LaneIdentifier;
use futures::future::join;
use futures::StreamExt;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_common::form::Form;
use swim_common::topic::Topic;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use utilities::sync::trigger;

#[tokio::test]
async fn test_delivery() {
    let (supply_lane, mut topic) =
        make_lane_model(Dropping.create_strategy(), &AgentExecutionConfig::default());
    let mut supply_rx = topic.subscribe().await.unwrap();

    let (metric_tx, metric_rx) = mpsc::channel(1);
    let prune_frequency = Duration::from_secs(1);
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let ident = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
        node_uri: "/node".into(),
        lane_uri: "/lane".into(),
    });
    let mut lanes = HashMap::new();
    lanes.insert(ident, supply_lane);

    let task = CollectorTask::new(
        "/node".to_string(),
        trigger_rx,
        metric_rx,
        prune_frequency,
        lanes,
    );

    let send_task = async move {
        let profile = UplinkProfile::default();
        let profile_value = profile.as_value();

        assert!(metric_tx
            .send(ObserverEvent::Uplink(
                RelativePath::new("/node", "/lane"),
                profile
            ))
            .await
            .is_ok());

        match supply_rx.next().await {
            Some(value) => {
                assert_eq!(value, profile_value)
            }
            None => {
                panic!("Expected an uplink profile")
            }
        }

        assert!(trigger_tx.trigger());
    };

    let (r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;

    assert_eq!(r1, CollectorStopResult::Normal);
}
