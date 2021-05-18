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

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::LaneIdentifier;
use crate::agent::lane::lifecycle::DemandMapLaneLifecycle;
use crate::agent::lane::model::demand_map;
use crate::agent::lane::model::demand_map::{
    DemandMapLane, DemandMapLaneCommand, DemandMapLaneEvent,
};
use crate::agent::{
    AgentContext, DemandMapLaneIo, DynamicLaneTasks, Lane, LaneTasks, RoutingIo, SwimAgent,
};
use crate::meta::{IdentifiedAgentIo, MetaNodeAddressed, LANES_URI};
use futures::future::ready;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::num::NonZeroUsize;
use swim_common::form::{Form, FormErr};
use swim_common::model::{Item, Value};
use swim_common::{ok, record};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const CUE_ERR: &str = "Failed to cue message";
const SYNC_ERR: &str = "Failed to sync";

const LANE_INFO_TAG: &str = "LaneInfo";
const FIELD_LANE_URI: &str = "laneUri";
const FIELD_LANE_TYPE: &str = "laneType";

/// Lane information metadata that can be retrieved when syncing to
/// `/swim:meta:node/percent-encoded-nodeuri/lanes`.
///
/// E.g: `swim:meta:node/unit%2Ffoo/lanes/`
#[derive(Debug, Clone, PartialEq)]
pub struct LaneInfo {
    /// The URI of the lane.
    lane_uri: String,
    /// The type of the lane.
    lane_type: LaneKind,
}

impl LaneInfo {
    pub fn new<L>(lane_uri: L, lane_type: LaneKind) -> LaneInfo
    where
        L: Into<String>,
    {
        LaneInfo {
            lane_uri: lane_uri.into(),
            lane_type,
        }
    }
}

// todo: add Form::flatten macro functionality
impl Form for LaneInfo {
    fn as_value(&self) -> Value {
        let LaneInfo {
            lane_uri,
            lane_type,
        } = self;

        record! {
            attrs => [LANE_INFO_TAG],
            items => [
                (FIELD_LANE_URI, lane_uri.as_ref()),
                (FIELD_LANE_TYPE, lane_type.to_string())
            ]
        }
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) => match attrs.first() {
                Some(attr) if attr.name == LANE_INFO_TAG => {
                    let mut lane_uri_opt = None;
                    let mut lane_type_opt = None;

                    for item in items {
                        match item {
                            Item::Slot(Value::Text(uri_key), Value::Text(uri_value))
                                if uri_key == FIELD_LANE_URI =>
                            {
                                lane_uri_opt = Some(uri_value.to_string());
                            }
                            Item::Slot(Value::Text(lane_key), Value::Text(lane_type))
                                if lane_key == FIELD_LANE_TYPE =>
                            {
                                lane_type_opt = Some(LaneKind::try_from(lane_type.as_str())?);
                            }
                            _ => return Err(FormErr::Malformatted),
                        }
                    }

                    Ok(LaneInfo {
                        lane_uri: ok!(lane_uri_opt),
                        lane_type: ok!(lane_type_opt),
                    })
                }
                _ => Err(FormErr::MismatchedTag),
            },
            v => Err(FormErr::incorrect_type("Record", v)),
        }
    }
}

/// An enumeration representing the type of a lane.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LaneKind {
    Action,
    Command,
    Demand,
    DemandMap,
    Map,
    JoinMap,
    JoinValue,
    Supply,
    Spatial,
    Value,
}

#[derive(Debug, PartialEq)]
pub struct LaneKindParseErr;

impl From<LaneKindParseErr> for FormErr {
    fn from(_: LaneKindParseErr) -> Self {
        FormErr::Malformatted
    }
}

impl<'a> TryFrom<&'a str> for LaneKind {
    type Error = LaneKindParseErr;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "Action" => Ok(LaneKind::Action),
            "Command" => Ok(LaneKind::Command),
            "Demand" => Ok(LaneKind::Demand),
            "DemandMap" => Ok(LaneKind::DemandMap),
            "Map" => Ok(LaneKind::Map),
            "JoinMap" => Ok(LaneKind::JoinMap),
            "JoinValue" => Ok(LaneKind::JoinValue),
            "Supply" => Ok(LaneKind::Supply),
            "Spatial" => Ok(LaneKind::Spatial),
            "Value" => Ok(LaneKind::Value),
            _ => Err(LaneKindParseErr),
        }
    }
}

impl Display for LaneKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneKind::Action => write!(f, "Action"),
            LaneKind::Command => write!(f, "Command"),
            LaneKind::Demand => write!(f, "Demand"),
            LaneKind::DemandMap => write!(f, "DemandMap"),
            LaneKind::Map => write!(f, "Map"),
            LaneKind::JoinMap => write!(f, "JoinMap"),
            LaneKind::JoinValue => write!(f, "JoinValue"),
            LaneKind::Supply => write!(f, "Supply"),
            LaneKind::Spatial => write!(f, "Spatial"),
            LaneKind::Value => write!(f, "Value"),
        }
    }
}

/// A handle to a lane that returns all of the lanes on an agent.
#[derive(Clone, Debug)]
pub struct LaneInformation {
    info_lane: DemandMapLane<String, LaneInfo>,
}

impl LaneInformation {
    #[cfg(test)]
    pub fn new(info_lane: DemandMapLane<String, LaneInfo>) -> LaneInformation {
        LaneInformation { info_lane }
    }
}

struct LaneInfoLifecycle {
    lanes: HashMap<String, LaneInfo>,
}

impl<'a, Agent> DemandMapLaneLifecycle<'a, String, LaneInfo, Agent> for LaneInfoLifecycle {
    type OnSyncFuture = BoxFuture<'a, Vec<String>>;
    type OnCueFuture = BoxFuture<'a, Option<LaneInfo>>;
    type OnRemoveFuture = BoxFuture<'a, ()>;

    fn on_sync<C>(
        &'a self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
    ) -> Self::OnSyncFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.keys().cloned().collect::<Vec<_>>() })
    }

    fn on_cue<C>(
        &'a self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
        key: String,
    ) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move { self.lanes.get(&key).cloned() })
    }

    fn on_remove<C>(
        &'a mut self,
        _model: &'a DemandMapLane<String, LaneInfo>,
        _context: &'a C,
        key: String,
    ) -> Self::OnRemoveFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let _ = self.lanes.remove(&key);
        })
    }
}

pub(crate) struct MetaDemandMapLifecycleTasks<S, K, V> {
    name: String,
    map: HashMap<K, V>,
    event_stream: S,
}

/// Creates a Demand Map lane that requires no projection to an agent. Used for metadata lanes.
///
/// # Arguments
///
/// * `name` - The name of the lane.
/// * `is_public` - Whether the lane is public (with respect to external message routing).
/// * `buffer_size` - Buffer size for the MPSC channel accepting the commands.
/// * `map` - a map that this lane is back by.
pub fn make_meta_demand_map_lane<Agent, Context, Key, Value>(
    name: impl Into<String>,
    is_public: bool,
    buffer_size: NonZeroUsize,
    map: HashMap<Key, Value>,
) -> (
    DemandMapLane<Key, Value>,
    impl LaneTasks<Agent, Context>,
    Option<impl RoutingIo<Context>>,
)
where
    Agent: 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
    Key: Any + Send + Sync + Form + Clone + Debug + Eq + Hash,
    Value: Any + Send + Sync + Form + Clone + Debug,
{
    let (lifecycle_tx, event_stream) = mpsc::channel(buffer_size.get());
    let event_stream = ReceiverStream::new(event_stream);

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
                mut map,
                event_stream,
                ..
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
                    DemandMapLaneCommand::Remove(key) => {
                        let _ = map.remove(&key);
                    }
                }
            }
        }
        .boxed()
    }
}

/// Opens a Demand Map lane that will serve information about the lanes in `lanes_summary`.
///
/// # Arguments
///
/// * `lane_buffer` - Buffer size for the MPSC channel accepting the commands.
/// * `lanes_summary` - A map keyed by a lane URI and a value that contains information about the
/// lane.
pub fn open_info_lane<Config, Agent, Context>(
    lane_buffer: NonZeroUsize,
    lanes_summary: HashMap<String, LaneInfo>,
) -> (
    LaneInformation,
    DynamicLaneTasks<Agent, Context>,
    IdentifiedAgentIo<Context>,
)
where
    Agent: SwimAgent<Config> + 'static,
    Context: AgentContext<Agent> + AgentExecutionContext + Send + Sync + 'static,
{
    let (info_lane, lane_info_task, lane_info_io) =
        make_meta_demand_map_lane(LANES_URI.to_string(), true, lane_buffer, lanes_summary);

    let info_handler = LaneInformation { info_lane };

    let lane_info_io = lane_info_io.expect("Lane returned private IO").boxed();
    let mut lane_hashmap = HashMap::new();
    lane_hashmap.insert(LaneIdentifier::meta(MetaNodeAddressed::Lanes), lane_info_io);

    (info_handler, vec![lane_info_task.boxed()], lane_hashmap)
}
