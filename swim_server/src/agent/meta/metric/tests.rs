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
use crate::agent::lane::model::supply::{make_lane_model, Dropping, Queue};
use crate::agent::meta::metric::task::CollectorTask;
use crate::agent::meta::metric::uplink::WarpUplinkProfile;
use crate::agent::meta::metric::ObserverEvent;
use crate::agent::meta::MetaNodeAddressed;
use crate::routing::LaneIdentifier;
use futures::future::join;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use swim_common::form::Form;
use swim_common::topic::Topic;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use utilities::sync::trigger;

#[tokio::test]
async fn test_queue() {
    let (supply_lane, mut topic) =
        make_lane_model(Queue::default(), &AgentExecutionConfig::default());

    let mut supply_rx = topic.subscribe().await.unwrap();
    let (metric_tx, metric_rx) = mpsc::channel(5);
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let ident = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
        node_uri: "/node".into(),
        lane_uri: "/lane".into(),
    });

    let mut lanes = HashMap::new();
    lanes.insert(ident, supply_lane);

    let task = CollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lanes);

    let send_task = async move {
        let profile1 = WarpUplinkProfile::new(1, 1, 1, 1, 1, 1);
        let profile2 = WarpUplinkProfile::new(2, 2, 2, 2, 2, 2);
        let profile3 = WarpUplinkProfile::new(3, 3, 3, 3, 3, 3);
        let values = vec![
            profile1.as_value(),
            profile2.as_value(),
            profile3.as_value(),
        ];
        let profiles = vec![profile1, profile2, profile3];

        for profile in profiles {
            assert!(metric_tx
                .send(ObserverEvent::Uplink(
                    RelativePath::new("/node", "/lane"),
                    profile
                ))
                .await
                .is_ok());
        }

        for profile_value in values {
            match supply_rx.next().await {
                Some(value) => {
                    assert_eq!(value, profile_value);
                }
                None => {
                    panic!("Expected an uplink profile")
                }
            }
        }

        match supply_rx.next().now_or_never() {
            Some(_) => {
                panic!("Unexpected uplink profile")
            }
            None => trigger_tx.trigger(),
        }
    };

    let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
}

#[tokio::test]
async fn test_dropping() {
    let (supply_lane, mut topic) = make_lane_model(Dropping, &AgentExecutionConfig::default());

    let mut supply_rx = topic.subscribe().await.unwrap();
    let (metric_tx, metric_rx) = mpsc::channel(150);
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let ident = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
        node_uri: "/node".into(),
        lane_uri: "/lane".into(),
    });

    let mut lanes = HashMap::new();
    lanes.insert(ident, supply_lane);

    let task = CollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lanes);

    let send_task = async move {
        for i in 0..100 {
            let mut profile = WarpUplinkProfile::default();
            profile.event_count = i;

            assert!(metric_tx
                .send(ObserverEvent::Uplink(
                    RelativePath::new("/node", "/lane"),
                    profile
                ))
                .await
                .is_ok());
        }

        match supply_rx.next().await {
            Some(value) => {
                let mut profile = WarpUplinkProfile::default();
                profile.event_count = 99;

                assert_eq!(value, profile.as_value());
                assert!(trigger_tx.trigger());
            }
            None => {
                panic!("Expected an uplink profile")
            }
        }
    };

    let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
}

#[tokio::test]
async fn test_unknown() {
    let (supply_lane, mut topic) =
        make_lane_model(Queue::default(), &AgentExecutionConfig::default());

    let mut supply_rx = topic.subscribe().await.unwrap();
    let (metric_tx, metric_rx) = mpsc::channel(150);
    let (trigger_tx, trigger_rx) = trigger::trigger();
    let ident = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
        node_uri: "/node".into(),
        lane_uri: "/lane".into(),
    });

    let mut lanes = HashMap::new();
    lanes.insert(ident, supply_lane);

    let task = CollectorTask::new("/node".to_string(), trigger_rx, metric_rx, lanes);

    let send_task = async move {
        assert!(metric_tx
            .send(ObserverEvent::Uplink(
                RelativePath::new("/unknown", "/unknown"),
                WarpUplinkProfile::default()
            ))
            .await
            .is_ok());

        match supply_rx.next().now_or_never() {
            Some(_) => {
                panic!("Unexpected uplink profile")
            }
            None => trigger_tx.trigger(),
        }
    };

    let (_r1, _r2) = join(task.run(NonZeroUsize::new(100).unwrap()), send_task).await;
}
