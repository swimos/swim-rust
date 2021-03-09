// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use crate::agent::lane::model::supply::supplier::Queue;
use crate::agent::lane::model::supply::{make_lane_model, SupplyLane};
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::tests::{backpressure_config, DEFAULT_BUFFER, DEFAULT_YIELD};
use crate::meta::metric::uplink::{
    uplink_aggregator, uplink_observer, TaggedWarpUplinkProfile, TrySendError, UplinkProfileSender,
    WarpUplinkPulse,
};
use crate::meta::metric::{MetricObserver, WarpUplinkProfile};
use futures::future::{join, join3};
use futures::stream::iter;
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use swim_common::warp::path::RelativePath;
use swim_runtime::time::delay::delay_for;
use swim_runtime::time::timeout::timeout;
use tokio::sync::mpsc;
use tokio::time::sleep;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

#[tokio::test]
async fn uplink_sender_ok() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);
    let profile = WarpUplinkProfile::default();

    assert!(sender.try_send(profile.clone()).is_ok());
    let expected = TaggedWarpUplinkProfile { path, profile };

    assert_eq!(rx.recv().now_or_never().unwrap().unwrap(), expected);
}

#[tokio::test]
async fn uplink_sender_err() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, _) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    assert_eq!(
        sender.try_send(WarpUplinkProfile::default()),
        Err(TrySendError)
    );
}

#[tokio::test]
async fn test_receive() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(1);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    let (event_observer, action_observer) = uplink_observer(Duration::from_nanos(1), sender);

    event_observer
        .inner
        .command_count
        .fetch_add(2, Ordering::Acquire);
    action_observer
        .inner
        .event_count
        .fetch_add(2, Ordering::Acquire);

    event_observer.inner.flush();

    let tagged = rx.recv().await.unwrap();

    assert_eq!(tagged.profile.event_count, 2);
    assert_eq!(tagged.profile.command_count, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_receive_threaded() {
    let path = RelativePath::new("/node", "/lane");
    let (tx, mut rx) = mpsc::channel(2048);
    let sender = UplinkProfileSender::new(path.clone(), tx);

    let sample_rate = Duration::from_secs(1);
    let (event_observer, action_observer) = uplink_observer(sample_rate, sender);

    let left_event_observer = event_observer.clone();
    let left_action_observer = action_observer.clone();

    let right_event_observer = event_observer.clone();
    let right_action_observer = action_observer.clone();

    let task_left = async move {
        for i in 0..100 {
            left_event_observer.on_event();
            left_action_observer.on_command();
            left_action_observer.did_open();
            left_action_observer.did_close();

            if i % 10 == 0 {
                sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let task_right = async move {
        for i in 0..100 {
            right_action_observer.on_command();
            right_event_observer.on_event();
            right_action_observer.did_close();
            right_action_observer.did_open();

            if i % 10 == 0 {
                sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let _r = join(task_left, task_right).await;

    sleep(sample_rate).await;

    event_observer.inner.flush();

    drop(event_observer);
    drop(action_observer);

    let mut profiles = Vec::new();

    while let Some(profile) = rx.recv().await {
        profiles.push(profile);
    }

    let tagged = profiles.pop().expect("Missing profile");
    let WarpUplinkProfile {
        event_delta,
        event_count,
        command_delta,
        command_count,
        open_delta,
        open_count,
        close_delta,
        close_count,
        ..
    } = tagged.profile;

    assert_eq!(event_delta as u64 + event_count, 200);
    assert_eq!(command_delta as u64 + command_count, 200);
    assert_eq!(open_delta + open_count, 200);
    assert_eq!(close_delta + close_count, 200);
}

#[tokio::test]
async fn task_backpressure() {
    let (stop_tx, stop_rx) = trigger::trigger();

    let format_lane = |id: usize| -> String { format!("/lane/{}", id) };

    // the number of lanes
    let count = 10;
    let buffer_size = 2;
    // The number of messages to send to each lane. Twice the buffer size to ensure that it overflows
    let message_count = buffer_size * 2;

    let sample_rate = Duration::from_millis(100);

    let config = MetricAggregatorConfig {
        sample_rate,
        backpressure_config: KeyedBackpressureConfig {
            buffer_size: NonZeroUsize::new(buffer_size).unwrap(),
            yield_after: DEFAULT_YIELD,
            bridge_buffer_size: NonZeroUsize::new(buffer_size).unwrap(),
            cache_size: NonZeroUsize::new(count).unwrap(),
        },
        ..Default::default()
    };

    let mut lanes = HashMap::new();
    let mut lane_rx = HashMap::new();
    let mut lane_set = HashSet::new();

    (0..count).into_iter().for_each(|i| {
        let (supply_lane, supply_rx) = make_lane_model(Queue::default());
        let key = format_lane(i);

        lane_set.insert(key.clone());
        let path = RelativePath::new("/node", key.clone());
        lanes.insert(path, supply_lane);
        lane_rx.insert(key, supply_rx);
    });

    let (lane_profile_tx, _lane_profile_rx) = mpsc::channel(4096);

    let (uplink_task, uplink_tx) = uplink_aggregator(
        stop_rx,
        sample_rate,
        NonZeroUsize::new(buffer_size).unwrap(),
        DEFAULT_YIELD,
        config.backpressure_config,
        lanes,
        lane_profile_tx,
    );

    let observer = MetricObserver::new(
        config.sample_rate,
        RelativeUri::from_str("/node").unwrap(),
        uplink_tx,
    );

    let task_jh = tokio::spawn(uplink_task);

    iter(0..count)
        .fold(observer, |observer, lane_id| async move {
            let (event_observer, action_observer) = observer.uplink_observer(format_lane(lane_id));

            iter(0..message_count)
                .fold(
                    (event_observer, action_observer),
                    |(event_observer, action_observer), _message_id| async {
                        delay_for(sample_rate).await;

                        event_observer
                            .inner
                            .event_delta
                            .fetch_add(1, Ordering::Acquire);
                        event_observer
                            .inner
                            .command_delta
                            .fetch_add(1, Ordering::Acquire);

                        event_observer.inner.flush();

                        (event_observer, action_observer)
                    },
                )
                .await;

            observer
        })
        .await;

    stop_tx.trigger();

    let (_, lane_set) = iter(0..count)
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

    assert!(lane_set.is_empty());

    let _ = task_jh.await.unwrap();
}

#[tokio::test]
async fn with_observer() {
    let (stop_tx, stop_rx) = trigger::trigger();
    let (supply_lane_tx, mut supply_lane_rx) = mpsc::channel(5);
    let lane = SupplyLane::new(Box::new(supply_lane_tx));

    let mut lane_map = HashMap::new();
    let path = RelativePath::new("/node", "lane");

    lane_map.insert(path.clone(), lane);

    let (lane_tx, mut lane_rx) = mpsc::channel(5);
    let sample_rate = Duration::from_millis(100);

    let supply_rcv_task = async move {
        let pulse: WarpUplinkPulse = supply_lane_rx.recv().await.unwrap();
        assert_eq!(pulse.event_count, 1);
    };

    let lane_rcv_task = async move {
        let tagged: TaggedWarpUplinkProfile = lane_rx.recv().await.unwrap();

        assert_eq!(tagged.profile.event_delta, 1);
        assert_eq!(tagged.profile.open_delta, 1);
        assert_eq!(tagged.profile.close_delta, 1);
    };

    let (uplink_task, uplink_tx) = uplink_aggregator(
        stop_rx,
        sample_rate,
        DEFAULT_BUFFER,
        DEFAULT_YIELD,
        backpressure_config(),
        lane_map,
        lane_tx,
    );

    let task = tokio::spawn(join3(uplink_task, lane_rcv_task, supply_rcv_task));

    let observer = MetricObserver::new(
        sample_rate,
        RelativeUri::from_str("/node").unwrap(),
        uplink_tx,
    );

    let (event_observer, action_observer) = observer.uplink_observer("lane".to_string());

    event_observer
        .inner
        .event_delta
        .fetch_add(1, Ordering::Relaxed);
    action_observer
        .inner
        .open_delta
        .fetch_add(1, Ordering::Relaxed);
    action_observer
        .inner
        .close_delta
        .fetch_add(1, Ordering::Relaxed);

    sleep(sample_rate).await;

    event_observer.inner.flush();

    sleep(sample_rate).await;

    stop_tx.trigger();

    let (task_result, _, _) = task.await.unwrap();

    assert_eq!(task_result, Ok(()));
}
