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

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

use futures::future::join;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;
use swim_warp::backpressure::KeyedBackpressureConfig;
use utilities::sync::trigger;

use crate::agent::lane::model::supply::{make_lane_model, Dropping, Queue};
use crate::agent::meta::metric::config::{MetricCollectorConfig, MetricCollectorTaskConfig};
use crate::agent::meta::metric::task::CollectorTask;
use crate::agent::meta::metric::uplink::WarpUplinkProfile;
use crate::agent::meta::metric::{make_metric_observer, ObserverEvent};
use crate::agent::meta::MetaNodeAddressed;
use crate::routing::LaneIdentifier;
use futures::stream::iter;
use swim_runtime::time::delay::delay_for;
use swim_runtime::time::timeout::timeout;
use tokio::time::Duration;

#[tokio::test]
async fn test_queue() {
    let (supply_lane, mut supply_rx) = make_lane_model(Queue(NonZeroUsize::new(4).unwrap()));

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
    let (supply_lane, mut supply_rx) = make_lane_model(Dropping);

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
async fn test_unknown_uplink() {
    let (supply_lane, mut supply_rx) = make_lane_model(Queue::default());

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

/*
 * Due to the metrics system not guaranteeing message delivery if there's a lot of contention and
 * messages being debounced, it's difficult to test the correct message delivery. This unit test
 * asserts that at least _some_ messages are delivered at the correct supply lanes if the buffers
 * overflow.
 */
#[tokio::test]
async fn test_task_backpressure() {
    let format_lane = |id: usize| -> String { format!("/lane/{}", id) };

    // the number of lanes
    let count = 10;
    let buffer_size = 2;
    // The number of messages to send to each lane. Twice the buffer size to ensure that it overflows
    let message_count = buffer_size * 2;

    let sample_rate = Duration::from_millis(100);

    let node_id = "/node";
    let (stop_tx, stop_rx) = trigger::trigger();
    let config = MetricCollectorConfig {
        task_config: MetricCollectorTaskConfig {
            sample_rate,
            ..Default::default()
        },
        backpressure_config: KeyedBackpressureConfig {
            buffer_size: NonZeroUsize::new(buffer_size).unwrap(),
            bridge_buffer_size: NonZeroUsize::new(buffer_size).unwrap(),
            cache_size: NonZeroUsize::new(count).unwrap(),
            ..Default::default()
        },
    };

    let mut lane_tx = HashMap::new();
    let mut lane_rx = HashMap::new();
    let mut lane_set = HashSet::new();

    (0..=count).into_iter().for_each(|i| {
        let (supply_lane, supply_rx) = make_lane_model(Queue::default());
        let key = format_lane(i);
        let ident = LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
            node_uri: node_id.clone().into(),
            lane_uri: key.clone().into(),
        });

        lane_set.insert(key.clone());
        lane_tx.insert(ident, supply_lane);
        lane_rx.insert(key, supply_rx);
    });

    let (factory, task) = make_metric_observer(node_id.to_string(), stop_rx, config, lane_tx);
    let task_jh = tokio::spawn(task);

    iter(0..=count)
        .fold(factory, |factory, lane_id| async move {
            let observer = factory
                .uplink_observer(RelativePath::new(node_id.to_string(), format_lane(lane_id)));

            iter(0..message_count)
                .fold(observer, |mut observer, _message_id| async {
                    observer.on_event();
                    observer.on_command();

                    delay_for(sample_rate).await;

                    observer
                })
                .await;

            factory
        })
        .await;

    let (_, lane_set) = iter(0..=count)
        .fold(
            (lane_rx, lane_set),
            |(mut lane_rx, mut lane_set), lane_id| async move {
                let lane_key = format_lane(lane_id);
                let lane = lane_rx.get_mut(&lane_key).unwrap();

                let lane_set = timeout(Duration::from_secs(5), async move {
                    while let Some(_) = lane.next().await {
                        lane_set.remove(&lane_key);
                    }

                    lane_set
                })
                .await
                .expect("Failed to receive any profiles");

                (lane_rx, lane_set)
            },
        )
        .await;

    stop_tx.trigger();

    let _ = task_jh.await.unwrap();
}
