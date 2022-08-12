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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::BytesMut;
use futures::FutureExt;
use futures::{
    future::{BoxFuture, Either},
    stream::{FuturesUnordered, SelectAll},
    StreamExt,
};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, UplinkKind},
    error::{AgentInitError, AgentTaskError, FrameIoError},
    protocol::{agent::LaneRequest, map::MapMessage},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};
use uuid::Uuid;

use crate::agent_lifecycle::lane_event::LaneEvent;
use crate::{
    agent_lifecycle::AgentLifecycle,
    event_handler::{EventHandler, EventHandlerError, HandlerAction, StepResult},
    meta::AgentMetadata,
};

mod io;
#[cfg(test)]
mod tests;

use io::{LaneReader, LaneWriter};

/// Response from a lane after it has written bytes to its outgoing buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    // The lane has no data to write (the buffer is still empty).
    NoData,
    // The lane has written data to the buffer but has no more data to write.
    Done,
    // The lane has written data to the buffer and will have more to write on a subsequent call.
    DataStillAvailable,
}

/// A trait which describes the lanes of an agent which can be run as a task attached to an
/// [`AgentContext`]. A type implementing this trait is sufficient to produce a functional agent
/// although it will not provided any lifecycle events for the agent or its lanes.
pub trait AgentLaneModel: Sized + Send {
    /// The type of handler to run when a command is received for a value lane.
    type ValCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a command is received for a map lane.
    type MapCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a request is received to sync with a lane.
    type OnSyncHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The names of all value like lanes (value lanes, command lanes, etc) in the agent.
    fn value_like_lanes() -> HashSet<&'static str>;

    /// The names of all map like lanes in the agent.
    fn map_like_lanes() -> HashSet<&'static str>;

    /// Mapping from lane identifiers to lane names for all lanes in the agent.
    fn lane_ids() -> HashMap<u64, Text>;

    /// Create a handler that will update the state of the agent when a command is received
    /// for a value lane. There will be no handler if the lane does not exist or does not
    /// accept commands.
    ///
    /// #Arguments
    ///  * `lane` - The name of the lane.
    /// * `body` - The content of the command.
    fn on_value_command(&self, lane: &str, body: BytesMut) -> Option<Self::ValCommandHandler>;

    /// Create a handler that will update the state of the agent when a command is received
    /// for a map lane. There will be no handler if the lane does not exist or does not
    /// accept commands.
    /// #Arguments
    /// * `lane` - The name of the lane.
    /// * `body` - The content of the command.
    fn on_map_command(
        &self,
        lane: &str,
        body: MapMessage<BytesMut, BytesMut>,
    ) -> Option<Self::MapCommandHandler>;

    /// Create a handler that will update the state of an agent when a request is made to
    /// sync with a lane. There will be no handler if the lane does not exist.
    ///
    /// #Arguments
    /// * `lane` - The name of the lane.
    /// * `id` - The ID of the remote that requested the sync.
    fn on_sync(&self, lane: &str, id: Uuid) -> Option<Self::OnSyncHandler>;

    /// Attempt to write pending data from a lane into the outgoing buffer. The result will
    /// indicate if data was written and if the lane has more data to write. There will be
    /// no result if the lane does not exist.
    fn write_event(&self, lane: &str, buffer: &mut BytesMut) -> Option<WriteResult>;
}

/// A factory to create agent lane model instances.
pub trait LaneModelFactory: Send + Sync {
    type LaneModel: AgentLaneModel;

    fn create(&self) -> Self::LaneModel;
}

impl<F, LaneModel: AgentLaneModel> LaneModelFactory for F
where
    F: Fn() -> LaneModel + Send + Sync,
{
    type LaneModel = LaneModel;

    fn create(&self) -> Self::LaneModel {
        self()
    }
}

/// The complete model for an agent consisting of an implementation of [`AgentLaneModel`] to describe the lanes
/// of the agent and an implementation of [`AgentLifecycle`] to describe the lifecycle events that will trigger,
/// for  example, when the agent starts or stops or when the state of a lane changes.
pub struct AgentModel<LaneModel, Lifecycle> {
    lane_model_fac: Arc<dyn LaneModelFactory<LaneModel = LaneModel>>,
    lifecycle: Lifecycle,
}

impl<LaneModel, Lifecycle: Clone> Clone for AgentModel<LaneModel, Lifecycle> {
    fn clone(&self) -> Self {
        Self {
            lane_model_fac: self.lane_model_fac.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<LaneModel, Lifecycle> AgentModel<LaneModel, Lifecycle> {
    pub fn new(
        lane_model_fac: Arc<dyn LaneModelFactory<LaneModel = LaneModel>>,
        lifecycle: Lifecycle,
    ) -> Self {
        AgentModel {
            lane_model_fac,
            lifecycle,
        }
    }
}

impl<LaneModel, Lifecycle> Agent for AgentModel<LaneModel, Lifecycle>
where
    LaneModel: AgentLaneModel + Send + 'static,
    Lifecycle: AgentLifecycle<LaneModel> + Clone + Send + 'static,
{
    fn run<'a>(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: &'a dyn AgentContext,
    ) -> BoxFuture<'a, AgentInitResult<'a>> {
        self.clone()
            .initialize_agent(route, config, context)
            .boxed()
    }
}
enum TaskEvent {
    WriteComplete {
        writer: LaneWriter,
        result: Result<(), std::io::Error>,
    },
    ValueRequest {
        id: u64,
        request: LaneRequest<BytesMut>,
    },
    MapRequest {
        id: u64,
        request: LaneRequest<MapMessage<BytesMut, BytesMut>>,
    },
    RequestError {
        id: u64,
        error: FrameIoError,
    },
}

impl<LaneModel, Lifecycle> AgentModel<LaneModel, Lifecycle>
where
    LaneModel: AgentLaneModel + Send + 'static,
    Lifecycle: AgentLifecycle<LaneModel> + 'static,
{
    /// Initialize the agent, performing the initial setup for all of the lanes (including triggering the
    /// `on_start` event).
    ///
    /// #Arguments
    /// * `route` - The node URI for thhe agent instance.
    /// * `config` - Agent specific configuration parameters.
    /// * `context` - Context through which to communicate with the runtime.
    async fn initialize_agent(
        self,
        route: RelativeUri,
        config: AgentConfig,
        context: &dyn AgentContext,
    ) -> AgentInitResult<'_>
    where
        LaneModel: AgentLaneModel,
        Lifecycle: AgentLifecycle<LaneModel>,
    {
        let AgentModel {
            lane_model_fac,
            lifecycle,
        } = self;

        let meta = AgentMetadata::new(&route, &config);

        let mut value_lane_io = HashMap::new();
        let mut map_lane_io = HashMap::new();

        let val_lane_names = LaneModel::value_like_lanes();
        let map_lane_names = LaneModel::map_like_lanes();
        let lane_ids = LaneModel::lane_ids();

        // Set up the lanes of the agent.
        for name in val_lane_names {
            let io = context.add_lane(name, UplinkKind::Value, None).await?;
            value_lane_io.insert(Text::new(name), io);
        }
        for name in map_lane_names {
            if value_lane_io.contains_key(name) {
                return Err(AgentInitError::DuplicateLane(Text::new(name)));
            }
            let io = context.add_lane(name, UplinkKind::Map, None).await?;
            map_lane_io.insert(Text::new(name), io);
        }

        let lane_model = lane_model_fac.create();
        // Run the agent's `on_start` event handler.
        let on_start_handler = lifecycle.on_start();
        if let Err(e) = run_handler(
            meta,
            &lane_model,
            &lifecycle,
            on_start_handler,
            &lane_ids,
            &mut Discard,
        ) {
            return Err(AgentInitError::UserCodeError(Box::new(e)));
        }
        let agent_task = AgentTask::new(
            lane_model,
            lifecycle,
            route,
            config,
            lane_ids,
            value_lane_io,
            map_lane_io,
        );
        Ok(agent_task.run_agent(context).boxed())
    }
}

struct AgentTask<LaneModel, Lifecycle> {
    lane_model: LaneModel,
    lifecycle: Lifecycle,
    route: RelativeUri,
    config: AgentConfig,
    lane_ids: HashMap<u64, Text>,
    value_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    map_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
}

impl<LaneModel, Lifecycle> AgentTask<LaneModel, Lifecycle>
where
    LaneModel: AgentLaneModel + Send + 'static,
    Lifecycle: AgentLifecycle<LaneModel> + 'static,
{
    /// #Arguments
    /// * `lane_model` - Defines the agent lanes.
    /// * `lifecycle` - User specified event handlers.
    /// * `route` - The node URI of the agent instance.
    /// * `config` - Agent specific configuration parameters.
    /// * `lane_ids` - Mapping between lane names and lane IDs.
    /// * `value_lane_io` - Channels to the runtime for value like lanes.
    /// * `map_lane_io` - Channels to the runtime for map like lanes.
    fn new(
        lane_model: LaneModel,
        lifecycle: Lifecycle,
        route: RelativeUri,
        config: AgentConfig,
        lane_ids: HashMap<u64, Text>,
        value_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
        map_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    ) -> Self {
        AgentTask {
            lane_model,
            lifecycle,
            route,
            config,
            lane_ids,
            value_lane_io,
            map_lane_io,
        }
    }

    /// Core event loop for the agent that routes incoming data from the runtime to the lanes and
    /// state changes fromt he lanes to the runtime.
    ///
    /// #Arguments
    /// * `_context` - Context through which to communicate with the runtime.
    async fn run_agent(
        self,
        _context: &dyn AgentContext, //Will be needed when downlinks are supported.
    ) -> Result<(), AgentTaskError> {
        let AgentTask {
            lane_model,
            lifecycle,
            route,
            config,
            lane_ids,
            value_lane_io,
            map_lane_io,
        } = self;
        let meta = AgentMetadata::new(&route, &config);

        let mut lane_ids_rev = HashMap::new();
        for (id, name) in &lane_ids {
            lane_ids_rev.insert(name.clone(), *id);
        }

        let mut lane_readers = SelectAll::new();
        let mut lane_writers = HashMap::new();
        let mut pending_writes = FuturesUnordered::new();

        // Set up readers and writes for each lane.
        for (name, (tx, rx)) in value_lane_io {
            let id = lane_ids_rev[&name];
            lane_readers.push(LaneReader::value(id, rx));
            lane_writers.insert(id, LaneWriter::new(id, tx));
        }

        for (name, (tx, rx)) in map_lane_io {
            let id = lane_ids_rev[&name];
            lane_readers.push(LaneReader::map(id, rx));
            lane_writers.insert(id, LaneWriter::new(id, tx));
        }

        // This set keeps track of which lanes have data to be written (according to executed event handlers).
        let mut dirty_lanes: HashSet<u64> = HashSet::new();

        loop {
            let task_event: TaskEvent = tokio::select! {
                biased;
                write_done = pending_writes.next(), if !pending_writes.is_empty() => {
                    if let Some((writer, result)) = write_done {
                        TaskEvent::WriteComplete {
                            writer, result
                        }
                    } else {
                        continue;
                    }
                }
                maybe_req = lane_readers.next() => {
                    match maybe_req {
                        Some((id, Ok(Either::Left(request)))) => TaskEvent::ValueRequest{
                            id, request
                        },
                        Some((id, Ok(Either::Right(request)))) => TaskEvent::MapRequest{
                            id, request
                        },
                        Some((id, Err(error))) => TaskEvent::RequestError {
                            id, error
                        },
                        _ => {
                            break Ok(());
                        }
                    }
                }
            };
            match task_event {
                TaskEvent::WriteComplete { writer, result } => {
                    if result.is_err() {
                        break Ok(()); //Failing to write indicates that the runtime has stopped so we can exit without an error.
                    }
                    lane_writers.insert(writer.lane_id(), writer);
                }
                TaskEvent::ValueRequest { id, request } => {
                    let name = &lane_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = lane_model.on_value_command(name.as_str(), body)
                            {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = lane_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                    }
                }
                TaskEvent::MapRequest { id, request } => {
                    let name = &lane_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = lane_model.on_map_command(name.as_str(), body) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = lane_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                    }
                }
                TaskEvent::RequestError { id, error } => {
                    let lane = lane_ids[&id].clone();
                    break Err(AgentTaskError::BadFrame { lane, error });
                }
            }
            // Attempt to write to the outgoing buffers for any lanes with data.
            dirty_lanes.retain(|id| {
                if let Some(mut tx) = lane_writers.remove(id) {
                    let name = &lane_ids[id];
                    match lane_model.write_event(name.as_str(), &mut tx.buffer) {
                        Some(WriteResult::Done) => {
                            pending_writes.push(tx.write());
                            false
                        }
                        Some(WriteResult::DataStillAvailable) => {
                            pending_writes.push(tx.write());
                            true
                        }
                        _ => false,
                    }
                } else {
                    true
                }
            });
        }?;
        // Try to run the `on_stop` handler before we stop.
        let on_stop_handler = lifecycle.on_stop();
        if let Err(e) = run_handler(
            meta,
            &lane_model,
            &lifecycle,
            on_stop_handler,
            &lane_ids,
            &mut Discard,
        ) {
            Err(AgentTaskError::UserCodeError(Box::new(e)))
        } else {
            Ok(())
        }
    }
}

/// As an event handler runs it indicates which lanes now have state updates that need
/// to be written out via the runtime. An implementation of this trait collects these
/// IDs to track which lanes to attempt to write data for later.
trait IdCollector {
    fn add_id(&mut self, id: u64);
}

/// When the agent is intializing, no IO is taking place so we simply discard the IDs.
struct Discard;

impl IdCollector for Discard {
    fn add_id(&mut self, _id: u64) {}
}

impl IdCollector for HashSet<u64> {
    fn add_id(&mut self, id: u64) {
        self.insert(id);
    }
}

/// Run an event handler within the context of the lifecycle of an agent. If the event handler causes another
/// event to trigger, it is suspended while that other event handler is executed. When an event handler changes
/// the state of a lane, that is recorded by the collector so that the change can be written out after the chain
/// of handlers completes.
///
/// This function does not check for invalid identifiers/lanes. If a lane is referred to that does not exist,
/// ther will be no error and no side effects will ocurr.
///
/// TODO: This methis recursive and has no checks to detect cycles. It would be very easy to create a set of
/// event handles which cause this to go into an infinite loop (this is also the case in Java). We could add some
/// heuristics to prevent this (for example terminating with an error if the same event handler gets executed
/// some number of times in a single chain) but this will likely add a bit of overhead.
///
/// #Arguments
///
/// * `meta` - Agent instance metadata (which can be requested by the event handler).
/// * `context` - The context within which the event handler is running. This provides access to the lanes of the
/// agent (typically it will be an instance of a struct where the fields are lane instances).
/// * `lifecycle` - The agent lifecycle which provides event handlers for state changes for each lane.
/// * `handler` - The initial event handler that starts the chain. This could be a lifecycle event or triggered
/// by an incoming message from the runtime.
/// * `lanes` - Mapping between lane IDs (returned by the handler to indicate that it has changed the state of
/// a lane) an the lane names (which are used by the lifecycle to identify the lanes).
/// * `collector` - Collects the IDs of lanes with state changes.
fn run_handler<Context, Lifecycle, Handler, Collector>(
    meta: AgentMetadata,
    context: &Context,
    lifecycle: &Lifecycle,
    mut handler: Handler,
    lanes: &HashMap<u64, Text>,
    collector: &mut Collector,
) -> Result<(), EventHandlerError>
where
    Lifecycle: for<'a> LaneEvent<'a, Context>,
    Handler: EventHandler<Context>,
    Collector: IdCollector,
{
    loop {
        match handler.step(meta, context) {
            StepResult::Continue { modified_lane } => {
                if let Some((modification, lane)) = modified_lane.and_then(|modification| {
                    lanes
                        .get(&modification.lane_id)
                        .map(|name| (modification, name))
                }) {
                    collector.add_id(modification.lane_id);
                    if modification.trigger_handler {
                        if let Some(consequence) = lifecycle.lane_event(context, lane.as_str()) {
                            run_handler(meta, context, lifecycle, consequence, lanes, collector)?;
                        }
                    }
                }
            }
            StepResult::Fail(err) => {
                break Err(err);
            }
            StepResult::Complete { modified_lane, .. } => {
                if let Some((modification, lane)) = modified_lane.and_then(|modification| {
                    lanes
                        .get(&modification.lane_id)
                        .map(|name| (modification, name))
                }) {
                    collector.add_id(modification.lane_id);
                    if modification.trigger_handler {
                        if let Some(consequence) = lifecycle.lane_event(context, lane.as_str()) {
                            run_handler(meta, context, lifecycle, consequence, lanes, collector)?;
                        }
                    }
                }
                break Ok(());
            }
        }
    }
}
