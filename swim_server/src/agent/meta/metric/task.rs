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

use std::collections::HashMap;
use std::fmt::Display;
use std::num::NonZeroUsize;

use futures::select;
use futures::FutureExt;
use futures::StreamExt;
use pin_utils::core_reexport::fmt::Formatter;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{event, Level};

use swim_common::form::Form;
use swim_common::model::Value;
use swim_runtime::time::delay::delay_for;
use utilities::sync::trigger;

use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::meta::metric::ObserverEvent;
use crate::agent::meta::MetaNodeAddressed;
use crate::routing::LaneIdentifier;

// const REQ_TX_ERR: &str = "Failed to return requested profile";
// const REMOVE_ERR: &str = "Attempted to deregister a metric that wasn't registered";
const LANE_NOT_FOUND: &str = "Lane not found";

pub struct CollectorTask {
    node_id: String,
    stop_rx: trigger::Receiver,
    metric_rx: mpsc::Receiver<ObserverEvent>,
    prune_frequency: Duration,
    lanes: HashMap<LaneIdentifier, SupplyLane<Value>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum CollectorStopResult {
    Normal,
    Abnormal,
}

impl Display for CollectorStopResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectorStopResult::Normal => write!(f, "Collector stopped normally"),
            CollectorStopResult::Abnormal => write!(f, "Collected stopped abnormally"),
        }
    }
}

enum CollectorEvent {
    ObserverEvent(ObserverEvent),
    Prune,
}

impl CollectorTask {
    // todo: accept collector task config
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        metric_rx: mpsc::Receiver<ObserverEvent>,
        prune_frequency: Duration,
        lanes: HashMap<LaneIdentifier, SupplyLane<Value>>,
    ) -> CollectorTask {
        CollectorTask {
            node_id,
            stop_rx,
            metric_rx,
            prune_frequency,
            lanes,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> CollectorStopResult {
        let CollectorTask {
            node_id,
            stop_rx,
            metric_rx,
            prune_frequency,
            mut lanes,
        } = self;

        let mut fused_metric_rx = metric_rx.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut fused_prune = delay_for(prune_frequency).fuse();

        let mut iteration_count: usize = 0;
        let yield_mod = yield_after.get();

        let stop_code = loop {
            let event: Option<CollectorEvent> = select! {
                _ = fused_prune => Some(CollectorEvent::Prune),
                _ = fused_trigger => {
                    break CollectorStopResult::Normal;
                },
                metric = fused_metric_rx.next() => metric.map(CollectorEvent::ObserverEvent),
            };
            match event {
                None => {
                    break CollectorStopResult::Abnormal;
                }
                Some(CollectorEvent::ObserverEvent(event)) => {
                    let (address, profile_value) = match event {
                        ObserverEvent::Node(profile) => (
                            LaneIdentifier::meta(MetaNodeAddressed::NodeProfile {
                                node_uri: node_id.clone().into(),
                            }),
                            profile.into_value(),
                        ),
                        ObserverEvent::Lane(_, _) => {
                            unimplemented!()
                        }
                        ObserverEvent::Uplink(address, profile) => (
                            LaneIdentifier::meta(MetaNodeAddressed::UplinkProfile {
                                node_uri: address.node.into(),
                                lane_uri: address.lane.into(),
                            }),
                            profile.into_value(),
                        ),
                    };

                    forward(&mut lanes, address, profile_value);
                }
                Some(CollectorEvent::Prune) => {
                    // todo: prune routes where the drop promise has been satisfied
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        event!(Level::INFO, %stop_code, %node_id, "Metric collector stopped");

        stop_code
    }
}

fn forward(
    lanes: &mut HashMap<LaneIdentifier, SupplyLane<Value>>,
    address: LaneIdentifier,
    profile: Value,
) {
    match lanes.get(&address) {
        Some(supply_lane) => {
            match supply_lane.try_send(profile) {
                Ok(()) => {}
                Err(_) => {
                    // no subscribers
                }
            }
        }
        None => {
            event!(Level::ERROR, %address, LANE_NOT_FOUND);
        }
    }
}
