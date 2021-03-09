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

use crate::agent::lane::channels::uplink::backpressure::KeyedBackpressureConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::log::make_node_logger;
use crate::meta::metric::aggregator::{AddressedMetric, AggregatorTask, ProfileItem};
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::meta::metric::lane::{LanePulse, TaggedLaneProfile};
use crate::meta::metric::node::NodePulse;
use crate::meta::metric::uplink::{TaggedWarpUplinkProfile, WarpUplinkPulse};
use crate::meta::metric::{
    AggregatorError, AggregatorErrorKind, MetricKind, NodeMetricAggregator, WarpLaneProfile,
    WarpUplinkProfile,
};
use futures::future::{join, join3};
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

pub const DEFAULT_YIELD: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(256) };
pub const DEFAULT_BUFFER: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(8) };

pub fn create_lane_map(
    count: usize,
    buffer_size: NonZeroUsize,
) -> (
    HashMap<RelativePath, mpsc::Receiver<LanePulse>>,
    HashMap<RelativePath, ProfileItem<TaggedLaneProfile, TaggedWarpUplinkProfile>>,
) {
    let mut lane_map = HashMap::new();
    let mut rx_map = HashMap::new();

    for i in 0..count {
        let (lane_tx, lane_rx) = mpsc::channel(buffer_size.get());
        let lane = SupplyLane::new(Box::new(lane_tx));

        let path = RelativePath::new("/node", format!("lane_{}", i));

        let value = ProfileItem::new(
            TaggedLaneProfile::pack(WarpLaneProfile::default(), path.clone()),
            lane,
        );

        lane_map.insert(path.clone(), value);
        rx_map.insert(path, lane_rx);
    }

    (rx_map, lane_map)
}

pub fn make_profile(count: u32) -> TaggedWarpUplinkProfile {
    let path = RelativePath::new("/node", "lane");
    TaggedWarpUplinkProfile::pack(
        WarpUplinkProfile {
            event_delta: count,
            event_rate: count as u64,
            event_count: count as u64,
            command_delta: count,
            command_rate: count as u64,
            command_count: count as u64,
            open_delta: count,
            open_count: count,
            close_delta: count,
            close_count: count,
        },
        path,
    )
}

pub fn backpressure_config() -> KeyedBackpressureConfig {
    KeyedBackpressureConfig {
        buffer_size: NonZeroUsize::new(2).unwrap(),
        yield_after: NonZeroUsize::new(256).unwrap(),
        bridge_buffer_size: NonZeroUsize::new(16).unwrap(),
        cache_size: NonZeroUsize::new(4).unwrap(),
    }
}

pub fn build_uplink_profile(path: RelativePath, n: u32) -> TaggedWarpUplinkProfile {
    TaggedWarpUplinkProfile {
        path,
        profile: WarpUplinkProfile {
            event_delta: n,
            event_rate: n as u64,
            event_count: n as u64,
            command_delta: n,
            command_rate: n as u64,
            command_count: n as u64,
            open_delta: n,
            open_count: n,
            close_delta: n,
            close_count: n,
        },
    }
}

#[tokio::test]
async fn drain() {
    let (trigger_tx, trigger_rx) = trigger::trigger();
    assert!(trigger_tx.trigger());

    let (lane_tx, mut lane_rx) = mpsc::channel(5);
    let lane = SupplyLane::new(Box::new(lane_tx));

    let mut lane_map = HashMap::new();
    let path = RelativePath::new("/node", "lane");

    let value = ProfileItem::new(
        TaggedLaneProfile::pack(WarpLaneProfile::default(), path.clone()),
        lane,
    );
    lane_map.insert(path.clone(), value);

    let stream = futures::stream::iter(vec![make_profile(1), make_profile(2), make_profile(3)]);
    let (out_tx, _out_rx) = mpsc::channel(4096);
    let aggregator =
        AggregatorTask::new(lane_map, Duration::from_secs(1), trigger_rx, stream, out_tx);

    let assert_task = async move {
        let first = lane_rx.recv().await.unwrap();
        let expected_first = LanePulse {
            uplink_pulse: WarpUplinkPulse {
                link_count: 0,
                event_rate: 1,
                event_count: 1,
                command_rate: 1,
                command_count: 1,
            },
        };

        assert_eq!(first, expected_first);

        let second = lane_rx.recv().await.unwrap();
        let expected_second = LanePulse {
            uplink_pulse: WarpUplinkPulse {
                link_count: 0,
                event_rate: 2,
                event_count: 2,
                command_rate: 2,
                command_count: 2,
            },
        };
        assert_eq!(second, expected_second);

        let third = lane_rx.recv().await.unwrap();
        let expected_third = LanePulse {
            uplink_pulse: WarpUplinkPulse {
                link_count: 0,
                event_rate: 3,
                event_count: 3,
                command_rate: 3,
                command_count: 3,
            },
        };
        assert_eq!(third, expected_third);

        assert!(lane_rx.recv().now_or_never().flatten().is_none());
    };

    let task = async move {
        let result = join(aggregator.run(DEFAULT_YIELD), assert_task).await;
        match result {
            (Ok(()), _) => {}
            (Err(e), _) => {
                panic!(e)
            }
        }
    };

    let handle = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(handle.is_ok());
}

#[tokio::test]
async fn abnormal() {
    let (_trigger_tx, trigger_rx) = trigger::trigger();
    let (lane_tx, _lane_rx) = mpsc::channel(5);
    let lane = SupplyLane::new(Box::new(lane_tx));

    let mut lane_map = HashMap::new();
    let path = RelativePath::new("/node", "lane");

    let value = ProfileItem::new(
        TaggedLaneProfile::pack(WarpLaneProfile::default(), path.clone()),
        lane,
    );
    lane_map.insert(path.clone(), value);

    let (metric_tx, metric_rx) = mpsc::channel(2);
    assert!(metric_tx.send(make_profile(5)).await.is_ok());
    let (out_tx, _out_rx) = mpsc::channel(4096);
    drop(metric_tx);

    let aggregator = AggregatorTask::new(
        lane_map,
        Duration::from_secs(1),
        trigger_rx,
        ReceiverStream::new(metric_rx),
        out_tx,
    );

    let task = async move {
        let result = aggregator.run(DEFAULT_YIELD).await;
        match result {
            Ok(()) => {
                panic!("Expected abnormal stop code")
            }
            Err(AggregatorError { aggregator, error }) => {
                assert_eq!(aggregator, MetricKind::Lane);
                assert_eq!(error, AggregatorErrorKind::AbnormalStop);
            }
        }
    };

    let handle = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(handle.is_ok());
}

fn make_pulse_map<T>(
    count: usize,
    extra_lanes: Vec<String>,
) -> (
    HashMap<RelativePath, SupplyLane<T>>,
    HashMap<RelativePath, Receiver<T>>,
)
where
    T: Send + Sync + 'static,
{
    let mut tx_map = HashMap::new();
    let mut rx_map = HashMap::new();

    for i in 0..count {
        let (path, rx, lane) = make_supply_lane(format!("lane_{}", i));

        tx_map.insert(path.clone(), lane);
        rx_map.insert(path, rx);
    }

    for lane in extra_lanes {
        let (path, rx, lane) = make_supply_lane(lane);

        tx_map.insert(path.clone(), lane);
        rx_map.insert(path, rx);
    }

    (tx_map, rx_map)
}

fn make_supply_lane<T>(lane: String) -> (RelativePath, Receiver<T>, SupplyLane<T>)
where
    T: Send + Sync + 'static,
{
    let path = RelativePath::new("/node", lane);
    let (tx, rx) = mpsc::channel(DEFAULT_BUFFER.get());
    let lane = SupplyLane::new(Box::new(tx));

    (path, rx, lane)
}

fn assert_receive_none<T>(map: &mut HashMap<RelativePath, Receiver<T>>, skip: &Vec<RelativePath>)
where
    T: PartialEq + Debug,
{
    let filtered = map.iter_mut().filter(|(k, _v)| skip.contains(k));

    for (_k, rx) in filtered {
        let received = rx.recv().now_or_never().flatten();
        assert_eq!(received, None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline() {
    let node_uri = RelativeUri::from_str("/node").unwrap();
    let (stop_tx, stop_rx) = trigger::trigger();
    let sample_rate = Duration::from_millis(100);
    let event_count = 10;

    let config = MetricAggregatorConfig {
        sample_rate,
        buffer_size: DEFAULT_BUFFER,
        yield_after: DEFAULT_YIELD,
        backpressure_config: backpressure_config(),
    };

    let endpoint_count = 10;
    let test_lanes = vec!["test".to_string()];

    let (uplink_tx, mut uplink_rx) = make_pulse_map(endpoint_count, test_lanes.clone());
    let (lane_tx, mut lane_rx) = make_pulse_map(endpoint_count, test_lanes.clone());

    let (node_pulse_tx, mut node_pulse_rx) = mpsc::channel(DEFAULT_BUFFER.get());
    let node_pulse_lane = SupplyLane::new(Box::new(node_pulse_tx));

    let (aggregator, aggregator_task) = NodeMetricAggregator::new(
        node_uri.clone(),
        stop_rx,
        config,
        uplink_tx,
        lane_tx,
        node_pulse_lane,
        make_node_logger(node_uri),
    );

    let aggregator_jh = tokio::spawn(aggregator_task);

    let test_lanes = test_lanes
        .into_iter()
        .map(|name| RelativePath::new("/node", name))
        .collect::<Vec<_>>();

    let uplink_test_lanes = test_lanes.clone();

    let uplink_task = async move {
        let lane = uplink_rx
            .get_mut(&RelativePath::new("/node", "test"))
            .expect("Missing lane");

        let uplink_pulse = lane.recv().await.expect("No pulse sent to lane");

        assert_receive_none(&mut uplink_rx, &uplink_test_lanes);

        assert_eq!(uplink_pulse.event_count, 10);
        assert_eq!(uplink_pulse.command_count, 10);
    };

    let lane_task = async move {
        let lane = lane_rx
            .get_mut(&RelativePath::new("/node", "test"))
            .expect("Missing lane");
        let lane_pulse = lane.recv().await.expect("No pulse sent to lane");
        let uplink_pulse = lane_pulse.uplink_pulse;

        assert_receive_none(&mut lane_rx, &test_lanes);

        assert_eq!(uplink_pulse.event_count, 10);
        assert_eq!(uplink_pulse.command_count, 10);
    };

    let node_task = async move {
        let NodePulse { uplinks } = node_pulse_rx.recv().await.expect("No pulse sent to lane");
        let WarpUplinkPulse {
            link_count,
            event_count,
            command_count,
            ..
        } = uplinks;

        assert_eq!(link_count, 0);
        assert_eq!(event_count, 10);
        assert_eq!(command_count, 10);
    };

    sleep(sample_rate).await;

    let task_jh = tokio::spawn(join3(uplink_task, lane_task, node_task));

    let (_event_observer, action_observer) =
        aggregator.observer().uplink_observer("test".to_string());

    action_observer.set_inner_values(event_count);
    action_observer.force_flush();

    assert!(task_jh.await.is_ok());

    stop_tx.trigger();

    assert!(aggregator_jh.await.unwrap().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_multiple_observers() {
    let node_uri = RelativeUri::from_str("/node").unwrap();
    let (stop_tx, stop_rx) = trigger::trigger();
    let sample_rate = Duration::from_millis(100);
    let event_count1 = 10;
    let event_count2 = 5;

    let expected_count = event_count1 + event_count2;

    let config = MetricAggregatorConfig {
        sample_rate,
        buffer_size: DEFAULT_BUFFER,
        yield_after: DEFAULT_YIELD,
        backpressure_config: backpressure_config(),
    };

    let endpoint_count = 10;
    let test_lanes = vec!["test".to_string()];

    let (uplink_tx, mut uplink_rx) = make_pulse_map(endpoint_count, test_lanes.clone());
    let (lane_tx, mut lane_rx) = make_pulse_map(endpoint_count, test_lanes.clone());

    let (node_pulse_tx, mut node_pulse_rx) = mpsc::channel(DEFAULT_BUFFER.get());
    let node_pulse_lane = SupplyLane::new(Box::new(node_pulse_tx));

    let (aggregator, aggregator_task) = NodeMetricAggregator::new(
        node_uri.clone(),
        stop_rx,
        config,
        uplink_tx,
        lane_tx,
        node_pulse_lane,
        make_node_logger(node_uri),
    );

    let aggregator_jh = tokio::spawn(aggregator_task);

    let test_lanes = test_lanes
        .into_iter()
        .map(|name| RelativePath::new("/node", name))
        .collect::<Vec<_>>();

    let uplink_test_lanes = test_lanes.clone();

    let uplink_task = async move {
        let lane = uplink_rx
            .get_mut(&RelativePath::new("/node", "test"))
            .expect("Missing lane");

        let uplink_pulse1 = lane.recv().await.expect("No pulse sent to lane");

        assert_eq!(uplink_pulse1.event_count, event_count1);
        assert_eq!(uplink_pulse1.command_count, event_count1);

        let uplink_pulse2 = lane.recv().await.expect("No pulse sent to lane");

        assert_receive_none(&mut uplink_rx, &uplink_test_lanes);

        assert_eq!(uplink_pulse2.event_count, expected_count);
        assert_eq!(uplink_pulse2.command_count, expected_count);
    };

    let lane_task = async move {
        let lane = lane_rx
            .get_mut(&RelativePath::new("/node", "test"))
            .expect("Missing lane");
        let lane_pulse1 = lane.recv().await.expect("No pulse sent to lane");
        let uplink_pulse1 = lane_pulse1.uplink_pulse;

        assert_eq!(uplink_pulse1.event_count, event_count1);
        assert_eq!(uplink_pulse1.command_count, event_count1);

        let lane_pulse2 = lane.recv().await.expect("No pulse sent to lane");
        let uplink_pulse2 = lane_pulse2.uplink_pulse;

        assert_receive_none(&mut lane_rx, &test_lanes);

        assert_eq!(uplink_pulse2.event_count, expected_count);
        assert_eq!(uplink_pulse2.command_count, expected_count);
    };

    let node_task = async move {
        let NodePulse { uplinks } = node_pulse_rx.recv().await.expect("No pulse sent to lane");
        let WarpUplinkPulse {
            link_count,
            event_count,
            command_count,
            ..
        } = uplinks;

        assert_eq!(link_count, 0);
        assert_eq!(event_count, event_count1);
        assert_eq!(command_count, event_count1);

        let NodePulse { uplinks } = node_pulse_rx.recv().await.expect("No pulse sent to lane");
        let WarpUplinkPulse {
            link_count,
            event_count,
            command_count,
            ..
        } = uplinks;

        assert_eq!(link_count, 0);
        assert_eq!(event_count, expected_count);
        assert_eq!(command_count, expected_count);
    };

    sleep(sample_rate).await;

    let task_jh = tokio::spawn(join3(uplink_task, lane_task, node_task));

    let (_event_observer1, action_observer1) =
        aggregator.observer().uplink_observer("test".to_string());

    action_observer1.set_inner_values(event_count1 as u32);
    action_observer1.force_flush();

    sleep(sample_rate).await;

    let (_event_observer2, action_observer2) =
        aggregator.observer().uplink_observer("test".to_string());

    action_observer2.set_inner_values(event_count2 as u32);
    action_observer2.force_flush();

    assert!(task_jh.await.is_ok());

    stop_tx.trigger();

    assert!(aggregator_jh.await.unwrap().is_ok());
}
