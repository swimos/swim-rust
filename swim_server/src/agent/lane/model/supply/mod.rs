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

use crate::agent::lane::model::supply::supplier::BoxSupplier;
use crate::agent::lane::LaneModel;
use std::sync::Arc;

mod supplier;
use crate::agent::lane::channels::AgentExecutionConfig;
pub use supplier::*;

#[cfg(test)]
mod tests;

/// Model for a stateless lane that publishes events to all uplinks.
///
/// # Type Parameters
///
/// * `T` - The type of the events produced.
pub struct SupplyLane<T> {
    sender: BoxSupplier<T>,
    id: Arc<()>,
}

impl<T> SupplyLane<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(sender: BoxSupplier<T>) -> Self {
        SupplyLane {
            sender,
            id: Default::default(),
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SupplyError> {
        self.sender.supply(value).await
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySupplyError> {
        self.sender.try_supply(value)
    }
}

impl<T> LaneModel for SupplyLane<T> {
    type Event = T;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

/// Create a new supply lane model. Returns a new supply lane model and a stream that events can be
/// received from.
pub fn make_lane_model<Event, W>(
    watch: W,
    config: &AgentExecutionConfig,
) -> (SupplyLane<Event>, W::Topic)
where
    Event: Send + Sync + Clone + 'static,
    W: SupplyLaneWatch<Event>,
{
    let (sender, topic) = watch.make_watch(config);
    let lane = SupplyLane::new(Box::new(sender));

    (lane, topic)
}
