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

#[cfg(test)]
mod tests;

pub mod supplier;

use crate::agent::lane::model::supply::supplier::{
    BoxSupplier, SupplyError, SupplyLaneObserver, TrySupplyError,
};
use crate::agent::lane::LaneModel;
use crate::agent::{AgentContext, Eff, Lane, LaneTasks, StatelessLifecycleTasks};
use futures::future::ready;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

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
        self.sender.supply(value).await.map_err(Into::into)
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySupplyError> {
        self.sender.try_supply(value).map_err(Into::into)
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
pub fn make_lane_model<Event, O>(observer: O) -> (SupplyLane<Event>, O::View)
where
    Event: Send + Sync + Clone + 'static,
    O: SupplyLaneObserver<Event>,
{
    let (sender, view) = observer.make_observer();
    let lane = SupplyLane::new(Box::new(sender));

    (lane, view)
}

impl Lane for StatelessLifecycleTasks {
    fn name(&self) -> &str {
        self.name.as_str()
    }
}

impl<Agent, Context> LaneTasks<Agent, Context> for StatelessLifecycleTasks
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, _context: Context) -> Eff {
        ready(()).boxed()
    }
}
