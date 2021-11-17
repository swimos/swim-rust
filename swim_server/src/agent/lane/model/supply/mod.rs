// Copyright 2015-2021 Swim Inc.
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

use crate::agent::lane::LaneModel;
use crate::agent::{AgentContext, Eff, Lane, LaneTasks, StatelessLifecycleTasks};
use crate::meta::info::LaneKind;
use futures::future::ready;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_utilities::future::item_sink::TrySend;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio_stream::wrappers::ReceiverStream;

/// Model for a stateless lane that publishes events to all uplinks.
///
/// # Type Parameters
///
/// * `T` - The type of the events produced.
pub struct SupplyLane<T> {
    sender: mpsc::Sender<T>,
    id: Arc<()>,
}

impl<T> SupplyLane<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(sender: mpsc::Sender<T>) -> Self {
        SupplyLane {
            sender,
            id: Default::default(),
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.sender.send(value).await
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)
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
pub fn make_lane_model<Event>(
    buffer_size: NonZeroUsize,
) -> (SupplyLane<Event>, ReceiverStream<Event>)
where
    Event: Send + Sync + Clone + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = SupplyLane::new(tx);

    (lane, ReceiverStream::new(rx))
}

impl Lane for StatelessLifecycleTasks {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        self.kind
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

impl<Event> TrySend<Event> for SupplyLane<Event> {
    type Error = TrySendError<Event>;

    fn try_send_item(&mut self, value: Event) -> Result<(), Self::Error> {
        self.sender.try_send(value)
    }
}

pub fn into_try_send<T>(lane: SupplyLane<T>) -> Box<dyn TrySend<T, Error = TrySendError<T>> + Send>
where
    T: Send + 'static,
{
    Box::new(lane)
}
