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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::model::demand_map;
use crate::agent::lane::model::demand_map::{
    DemandMapLane, DemandMapLaneCommand, DemandMapLaneEvent,
};
use crate::agent::lane::LaneKind;
use crate::agent::{AgentContext, DemandMapLaneIo, Lane, LaneIo, LaneTasks};
use futures::future::ready;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use swim_common::form::Form;
use tokio::sync::mpsc;
use tracing::{event, Level};

const CUE_ERR: &str = "Failed to cue message";
const SYNC_ERR: &str = "Failed to sync";

pub(crate) struct MetaDemandMapLifecycleTasks<S, K, V> {
    name: String,
    map: HashMap<K, V>,
    event_stream: S,
}

impl<S, K, V> MetaDemandMapLifecycleTasks<S, K, V> {
    #[cfg(test)]
    pub(crate) fn new(name: String, map: HashMap<K, V>, event_stream: S) -> Self {
        MetaDemandMapLifecycleTasks {
            name,
            map,
            event_stream,
        }
    }
}

pub fn make_meta_demand_map_lane<Agent, Context, Key, Value>(
    name: impl Into<String>,
    is_public: bool,
    buffer_size: NonZeroUsize,
    map: HashMap<Key, Value>,
) -> (
    DemandMapLane<Key, Value>,
    impl LaneTasks<Agent, Context>,
    Option<impl LaneIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Key: Any + Send + Sync + Form + Clone + Debug + Eq + Hash,
    Value: Any + Send + Sync + Form + Clone + Debug,
{
    let (lifecycle_tx, event_stream) = mpsc::channel(buffer_size.get());
    let (lane, topic) = demand_map::make_lane_model(buffer_size, lifecycle_tx);

    let tasks = MetaDemandMapLifecycleTasks {
        name: name.into(),
        map,
        event_stream,
    };

    let lane_io = if is_public {
        Some(DemandMapLaneIo::new(lane.clone(), topic))
    } else {
        None
    };

    (lane, tasks, lane_io)
}

impl<L, S, P> Lane for MetaDemandMapLifecycleTasks<L, S, P> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn kind(&self) -> LaneKind {
        LaneKind::DemandMap
    }
}

impl<Agent, Context, S, Key, Value> LaneTasks<Agent, Context>
    for MetaDemandMapLifecycleTasks<S, Key, Value>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = DemandMapLaneCommand<Key, Value>> + Send + Sync + 'static,
    Key: Any + Clone + Form + Send + Sync + Debug + Eq + Hash,
    Value: Any + Clone + Form + Send + Sync + Debug,
{
    fn start<'a>(&'a self, _context: &'a Context) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let MetaDemandMapLifecycleTasks {
                map, event_stream, ..
            } = *self;

            let events = event_stream.take_until(context.agent_stop_event());

            pin_mut!(events);

            while let Some(event) = events.next().await {
                match event {
                    DemandMapLaneCommand::Sync(sender) => {
                        let size = map.len();
                        let results = map.clone().into_iter().fold(
                            Vec::with_capacity(size),
                            |mut results, (key, value)| {
                                results.push(DemandMapLaneEvent::update(key, value));
                                results
                            },
                        );

                        if sender.send(results).is_err() {
                            event!(Level::ERROR, SYNC_ERR)
                        }
                    }
                    DemandMapLaneCommand::Cue(sender, key) => {
                        let value = map.get(&key).map(Clone::clone);
                        if sender.send(value).is_err() {
                            event!(Level::ERROR, CUE_ERR)
                        }
                    }
                    DemandMapLaneCommand::Remove(_key) => {
                        // no-op as lanes cannot be removed
                    }
                }
            }
        }
        .boxed()
    }
}
