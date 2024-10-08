// Copyright 2015-2024 Swim Inc.
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

mod egress;
#[cfg(test)]
mod fixture;
mod ingress;
#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use egress::EgressConnectorLifecycle;
pub use ingress::IngressConnectorLifecycle;
use swimos_agent::{
    agent_lifecycle::HandlerContext,
    event_handler::{EventHandler, HandlerActionExt, Sequentially, TryHandlerActionExt},
    lanes::OpenLane,
};
use swimos_api::{agent::WarpLaneKind, error::LaneSpawnError};
use swimos_utilities::trigger;
use tokio::sync::Semaphore;

use crate::ConnectorAgent;

fn open_lane(
    name: String,
    kind: WarpLaneKind,
    semaphore: Arc<Semaphore>,
) -> impl EventHandler<ConnectorAgent> + 'static {
    let handler_context: HandlerContext<ConnectorAgent> = Default::default();
    OpenLane::new(name, kind, move |_: Result<_, LaneSpawnError>| {
        handler_context.effect(move || semaphore.add_permits(1))
    })
}

fn open_lanes(
    lanes: Vec<(String, WarpLaneKind)>,
    on_done: trigger::Sender,
) -> impl EventHandler<ConnectorAgent> + 'static {
    let handler_context: HandlerContext<ConnectorAgent> = Default::default();
    handler_context
        .value(u32::try_from(lanes.len()))
        .try_handler()
        .and_then(move |total| {
            let semaphore = Arc::new(Semaphore::new(0));
            let wait_handle = semaphore.clone();
            let await_fut = async move {
                let _ = wait_handle.acquire_many(total).await;
                handler_context.effect(move || {
                    on_done.trigger();
                })
            };
            let await_done = handler_context.suspend(await_fut);
            let lane_handlers: Vec<_> = lanes
                .into_iter()
                .map(|(name, kind)| open_lane(name, kind, semaphore.clone()))
                .collect();

            await_done
                .followed_by(Sequentially::new(lane_handlers))
                .discard()
        })
}
