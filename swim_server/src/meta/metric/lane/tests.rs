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

use crate::agent::lane::model::supply::SupplyLane;
use crate::meta::metric::aggregator::{AddressedMetric, AggregatorTask, ProfileItem};
use crate::meta::metric::lane::{LanePulse, TaggedLaneProfile};
use crate::meta::metric::tests::{
    build_uplink_profile, create_lane_map, DEFAULT_BUFFER, DEFAULT_YIELD,
};
use crate::meta::metric::uplink::{TaggedWarpUplinkProfile, WarpUplinkPulse};
use crate::meta::metric::{LaneProfile, WarpUplinkProfile};
use futures::future::join;
use std::collections::HashMap;
use std::time::Duration;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use utilities::sync::trigger;

#[tokio::test]
async fn single() {
    let (stop_tx, stop_rx) = trigger::trigger();
    let (lane_tx, mut lane_rx) = mpsc::channel(5);
    let lane = SupplyLane::new(Box::new(lane_tx));

    let mut lane_map = HashMap::new();
    let path = RelativePath::new("/node", "lane");

    let value = ProfileItem::new(
        TaggedLaneProfile::pack(LaneProfile::default(), path.clone()),
        lane,
    );
    lane_map.insert(path.clone(), value);

    let (lane_profile_tx, lane_profile_rx) = mpsc::channel(5);
    let (node_tx, mut node_rx) = mpsc::channel(5);

    let lane_aggregator = AggregatorTask::new(
        lane_map,
        Duration::from_nanos(1),
        stop_rx.clone(),
        ReceiverStream::new(lane_profile_rx),
        node_tx,
    );

    let aggregator_task = tokio::spawn(lane_aggregator.run(DEFAULT_YIELD));

    let receive_task = async move {
        let received = lane_rx.recv().await.expect("Expected a LanePulse");
        let expected = LanePulse {
            uplink_pulse: WarpUplinkPulse {
                event_delta: 1,
                event_rate: 2,
                event_count: 3,
                command_delta: 4,
                command_rate: 5,
                command_count: 6,
            },
        };

        assert_eq!(received, expected);

        let received = node_rx.recv().await.expect("Expected a lane profile");
        let expected = TaggedLaneProfile {
            path: path,
            profile: LaneProfile {
                uplink_event_delta: 1,
                uplink_event_rate: 2,
                uplink_event_count: 3,
                uplink_command_delta: 4,
                uplink_command_rate: 5,
                uplink_command_count: 6,
                uplink_open_delta: 7,
                uplink_open_count: 8,
                uplink_close_delta: 9,
                uplink_close_count: 10,
            },
        };
        assert_eq!(received, expected);
    };
    let receive_task = tokio::spawn(receive_task);

    let input = TaggedWarpUplinkProfile {
        path: RelativePath::new("/node", "lane"),
        profile: WarpUplinkProfile {
            event_delta: 1,
            event_rate: 2,
            event_count: 3,
            command_delta: 4,
            command_rate: 5,
            command_count: 6,
            open_delta: 7,
            open_count: 8,
            close_delta: 9,
            close_count: 10,
        },
    };

    assert!(lane_profile_tx.send(input).await.is_ok());

    assert!(receive_task.await.is_ok());
    stop_tx.trigger();
    assert!(aggregator_task.await.is_ok());
}

#[tokio::test]
async fn multiple_lanes() {
    let profile_send_count = 10;
    let lane_count = 5;

    let (stop_tx, stop_rx) = trigger::trigger();

    let (rx_map, lane_map) = create_lane_map(5, DEFAULT_BUFFER);
    let (lane_profile_tx, lane_profile_rx) = mpsc::channel(lane_count * profile_send_count * 2);

    let sample_rate = Duration::from_secs(5);
    let (out_tx, _out_rx) = mpsc::channel(4096);

    let lane_aggregator = AggregatorTask::new(
        lane_map,
        sample_rate,
        stop_rx,
        ReceiverStream::new(lane_profile_rx),
        out_tx,
    );

    let assertion_task = async move {
        for (path, _v) in &rx_map {
            for i in 0..profile_send_count {
                let payload = build_uplink_profile(path.clone(), i as u32);
                assert!(lane_profile_tx.send(payload).await.is_ok());
            }
        }

        sleep(sample_rate).await;

        assert!(stop_tx.trigger());

        let expected = LanePulse {
            uplink_pulse: WarpUplinkPulse {
                event_delta: 45,
                event_rate: 45,
                event_count: 45,
                command_delta: 45,
                command_rate: 45,
                command_count: 45,
            },
        };

        for (path, mut v) in rx_map {
            let received = v
                .recv()
                .await
                .expect(&format!("No value at lane: {}", path));
            assert_eq!(received, expected);
        }
    };

    match join(lane_aggregator.run(DEFAULT_YIELD), assertion_task).await {
        (Ok(_), _) => {}
        (Err(e), _) => {
            panic!(e)
        }
    }
}
