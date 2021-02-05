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
use tracing::{event, Level};

use swim_common::form::Form;
use swim_common::model::Value;
use utilities::sync::trigger;

use crate::agent::lane::model::supply::{SupplyLane, TrySupplyError};
use crate::agent::meta::metric::ObserverEvent;
use crate::agent::meta::MetaNodeAddressed;
use crate::routing::LaneIdentifier;

const REMOVING_LANE: &str = "Lane closed, removing";
const LANE_NOT_FOUND: &str = "Lane not found";
const STOP_OK: &str = "Collector stopped normally";
const STOP_CLOSED: &str = "Collector event stream unexpectedly closed";

/// Collects all the profiles generated for a given node and forwards them to their corresponding
/// supply lanes for consumption.
pub struct CollectorTask {
    node_id: String,
    stop_rx: trigger::Receiver,
    metric_rx: mpsc::Receiver<ObserverEvent>,
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

impl CollectorTask {
    pub fn new(
        node_id: String,
        stop_rx: trigger::Receiver,
        metric_rx: mpsc::Receiver<ObserverEvent>,
        lanes: HashMap<LaneIdentifier, SupplyLane<Value>>,
    ) -> CollectorTask {
        CollectorTask {
            node_id,
            stop_rx,
            metric_rx,
            lanes,
        }
    }

    pub async fn run(self, yield_after: NonZeroUsize) -> CollectorStopResult {
        let CollectorTask {
            node_id,
            stop_rx,
            metric_rx,
            mut lanes,
        } = self;

        let mut fused_metric_rx = metric_rx.fuse();
        let mut fused_trigger = stop_rx.fuse();
        let mut iteration_count: usize = 0;

        let yield_mod = yield_after.get();

        let stop_code = loop {
            let event: Option<ObserverEvent> = select! {
                _ = fused_trigger => {
                    event!(Level::WARN, %node_id, STOP_OK);
                    break CollectorStopResult::Normal;
                },
                metric = fused_metric_rx.next() => metric,
            };
            match event {
                None => {
                    event!(Level::WARN, %node_id, STOP_CLOSED);
                    break CollectorStopResult::Abnormal;
                }
                Some(event) => {
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
                                node_uri: address.node,
                                lane_uri: address.lane,
                            }),
                            profile.into_value(),
                        ),
                    };

                    forward(&mut lanes, address, profile_value);
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };

        event!(Level::INFO, %stop_code, %node_id, STOP_OK);

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
            if let Err(TrySupplyError::Closed) = supply_lane.try_send(profile) {
                event!(Level::ERROR, %address, REMOVING_LANE);
                let _ = lanes.remove(&address);
            }
        }
        None => {
            event!(Level::ERROR, %address, LANE_NOT_FOUND);
        }
    }
}
