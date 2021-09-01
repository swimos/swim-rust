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
    AgentContext, DemandMapLaneIo, DynamicLaneTasks, Lane, LaneIo, LaneTasks, SwimAgent,
};
use crate::meta::{IdentifiedAgentIo, MetaNodeAddressed, LANES_URI};
use futures::future::ready;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::num::NonZeroUsize;
use swim_common::form::structural::read::error::ExpectedEvent;
use swim_common::form::structural::read::event::ReadEvent;
use swim_common::form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use swim_common::form::structural::read::ReadError;
use swim_common::form::structural::write::{PrimitiveWriter, StructuralWritable, StructuralWriter};
use swim_common::form::structural::Tag;
use swim_common::form::Form;
use swim_common::model::text::Text;
use swim_common::model::ValueKind;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

const CUE_ERR: &str = "Failed to cue message";
const SYNC_ERR: &str = "Failed to sync";

/// Lane information metadata that can be retrieved when syncing to
/// `/swim:meta:node/percent-encoded-nodeuri/lanes`.
///
/// E.g: `swim:meta:node/unit%2Ffoo/lanes/`
#[derive(Debug, Clone, PartialEq, Form)]
pub struct LaneInfo {
    /// The URI of the lane.
    #[form(name = "laneUri")]
    lane_uri: String,
    /// The type of the lane.
    #[form(name = "laneType")]
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

/// An enumeration representing the type of a lane.
#[derive(Tag, Debug, PartialEq, Eq, Clone, Copy)]
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

pub struct LaneKindRecognizer;

impl Recognizer for LaneKindRecognizer {
    type Target = LaneKind;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                Some(
                    LaneKind::try_from(txt.borrow()).map_err(|_| ReadError::Malformatted {
                        text: txt.into(),
                        message: Text::new("Not a valid Lane kind."),
                    }),
                )
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl RecognizerReadable for LaneKind {
    type Rec = LaneKindRecognizer;
    type AttrRec = SimpleAttrBody<LaneKindRecognizer>;
    type BodyRec = SimpleRecBody<LaneKindRecognizer>;

    fn make_recognizer() -> Self::Rec {
        LaneKindRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(LaneKindRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(LaneKindRecognizer)
    }
}

impl StructuralWritable for LaneKind {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn num_attributes(&self) -> usize {
        0
    }
}

#[derive(Debug, PartialEq)]
pub struct LaneKindParseErr;

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
        let as_str: &str = self.as_ref();
        write!(f, "{}", as_str)
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
    Option<impl LaneIo<Context>>,
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
