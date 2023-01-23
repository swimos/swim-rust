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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use bytes::BytesMut;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::{
    future::{BoxFuture, Either},
    stream::{FuturesUnordered, SelectAll},
    StreamExt,
};
use swim_api::error::{AgentRuntimeError, DownlinkRuntimeError, OpenStoreError};
use swim_api::meta::lane::LaneKind;
use swim_api::protocol::map::{MapMessageDecoder, RawMapOperationDecoder};
use swim_api::protocol::WithLengthBytesCodec;
use swim_api::store::StoreKind;
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, UplinkKind},
    error::{AgentInitError, AgentTaskError, FrameIoError},
    protocol::{agent::LaneRequest, map::MapMessage},
};
use swim_model::Text;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::routing::route_uri::RouteUri;
use tracing::{error, info};
use uuid::Uuid;

use crate::agent_lifecycle::item_event::ItemEvent;
use crate::event_handler::{ActionContext, BoxEventHandler, HandlerFuture, WriteStream};
use crate::{
    agent_lifecycle::AgentLifecycle,
    event_handler::{EventHandler, EventHandlerError, HandlerAction, StepResult},
    meta::AgentMetadata,
};

pub mod downlink;
mod init;
mod io;
#[cfg(test)]
mod tests;

use io::{ItemWriter, LaneReader};

use bitflags::bitflags;

use self::downlink::handlers::BoxDownlinkChannel;
use self::init::{run_item_initializer, InitializedItem};
pub use init::{
    ItemInitializer, MapLaneInitializer, MapStoreInitializer, ValueLaneInitializer,
    ValueStoreInitializer,
};

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

pub type InitFn<Agent> = Box<dyn FnOnce(&Agent) + Send + 'static>;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum ItemKind {
    Lane,
    Store,
}

bitflags! {

    #[derive(Default)]
    pub struct LaneFlags: u8 {
        /// The state of the lane should not be persisted.
        const TRANSIENT = 0b01;
    }

}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemSpec {
    pub kind: ItemKind,
    pub flags: LaneFlags,
}

impl ItemSpec {
    pub fn new(kind: ItemKind, flags: LaneFlags) -> Self {
        ItemSpec { kind, flags }
    }
}

/// A trait which describes the lanes of an agent which can be run as a task attached to an
/// [`AgentContext`]. A type implementing this trait is sufficient to produce a functional agent
/// although it will not provided any lifecycle events for the agent or its lanes.
pub trait AgentSpec: Sized + Send {
    /// The type of handler to run when a command is received for a value lane.
    type ValCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a command is received for a map lane.
    type MapCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a request is received to sync with a lane.
    type OnSyncHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The names and flags of all value like items (value lanes and stores, command lanes, etc) in the agent.
    fn value_like_item_specs() -> HashMap<&'static str, ItemSpec>;

    /// The names and flags of all map like items in the agent.
    fn map_like_item_specs() -> HashMap<&'static str, ItemSpec>;

    /// Mapping from item identifiers to lane names for all items in the agent.
    fn item_ids() -> HashMap<u64, Text>;

    /// Create a handler that will update the state of the agent when a command is received
    /// for a value lane. There will be no handler if the lane does not exist or does not
    /// accept commands.
    ///
    /// #Arguments
    /// * `lane` - The name of the lane.
    /// * `body` - The content of the command.
    fn on_value_command(&self, lane: &str, body: BytesMut) -> Option<Self::ValCommandHandler>;

    /// Create an initializer that will consume the state of a value-like item, as reported by the runtime.
    ///
    /// #Arguments
    /// * `lane` - The name of the item.
    fn init_value_like_item(
        &self,
        item: &str,
    ) -> Option<Box<dyn ItemInitializer<Self, BytesMut> + Send + 'static>>
    where
        Self: 'static;

    /// Create an initializer that will consume the state of a map-like item, as reported by the runtime.
    ///
    /// #Arguments
    /// * `item` - The name of the item.
    fn init_map_like_item(
        &self,
        item: &str,
    ) -> Option<Box<dyn ItemInitializer<Self, MapMessage<BytesMut, BytesMut>> + Send + 'static>>
    where
        Self: 'static;

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
pub trait ItemModelFactory: Send + Sync {
    type ItemModel: AgentSpec;

    fn create(&self) -> Self::ItemModel;
}

impl<F, ItemModel: AgentSpec> ItemModelFactory for F
where
    F: Fn() -> ItemModel + Send + Sync,
{
    type ItemModel = ItemModel;

    fn create(&self) -> Self::ItemModel {
        self()
    }
}

/// The complete model for an agent consisting of an implementation of [`AgentLaneModel`] to describe the lanes
/// of the agent and an implementation of [`AgentLifecycle`] to describe the lifecycle events that will trigger,
/// for  example, when the agent starts or stops or when the state of a lane changes.
pub struct AgentModel<ItemModel, Lifecycle> {
    item_model_fac: Arc<dyn ItemModelFactory<ItemModel = ItemModel>>,
    lifecycle: Lifecycle,
}

impl<ItemModel, Lifecycle: Clone> Clone for AgentModel<ItemModel, Lifecycle> {
    fn clone(&self) -> Self {
        Self {
            item_model_fac: self.item_model_fac.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<ItemModel, Lifecycle> AgentModel<ItemModel, Lifecycle> {
    pub fn new<F>(item_model_fac: F, lifecycle: Lifecycle) -> Self
    where
        F: ItemModelFactory<ItemModel = ItemModel> + Sized + 'static,
    {
        AgentModel {
            item_model_fac: Arc::new(item_model_fac),
            lifecycle,
        }
    }

    pub fn from_arc(
        item_model_fac: Arc<dyn ItemModelFactory<ItemModel = ItemModel>>,
        lifecycle: Lifecycle,
    ) -> Self {
        AgentModel {
            item_model_fac,
            lifecycle,
        }
    }
}

impl<ItemModel, Lifecycle> Agent for AgentModel<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + Clone + Send + 'static,
{
    fn run(
        &self,
        route: RouteUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        self.clone()
            .initialize_agent(route, config, context)
            .boxed()
    }
}
enum TaskEvent<ItemModel> {
    WriteComplete {
        writer: ItemWriter,
        result: Result<(), std::io::Error>,
    },
    SuspendedComplete {
        handler: BoxEventHandler<'static, ItemModel>,
    },
    DownlinkReady {
        downlink_event: Option<(HostedDownlink<ItemModel>, HostedDownlinkEvent)>,
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

struct HostedDownlink<Context> {
    channel: BoxDownlinkChannel<Context>,
    write_stream: Option<BoxStream<'static, Result<(), std::io::Error>>>,
}

impl<Context> HostedDownlink<Context> {
    fn new(
        channel: BoxDownlinkChannel<Context>,
        write_stream: BoxStream<'static, Result<(), std::io::Error>>,
    ) -> Self {
        HostedDownlink {
            channel,
            write_stream: Some(write_stream),
        }
    }
}

#[derive(Debug)]
enum HostedDownlinkEvent {
    WriterFailed(std::io::Error),
    Written,
    WriterTerminated,
    HandlerReady,
}

impl<Context> HostedDownlink<Context> {
    async fn wait_on_downlink(mut self) -> Option<(Self, HostedDownlinkEvent)> {
        let HostedDownlink {
            channel,
            write_stream,
        } = &mut self;

        let next = if let Some(out) = write_stream.as_mut() {
            tokio::select! {
                handler_ready = channel.await_ready() => Either::Left(handler_ready),
                maybe_result = out.next() => Either::Right(maybe_result),
            }
        } else {
            Either::Left(channel.await_ready().await)
        };

        match next {
            Either::Left(Some(Ok(_))) => Some((self, HostedDownlinkEvent::HandlerReady)),
            Either::Left(Some(Err(e))) => {
                error!(error = %e, "A downlink input channel failed.");
                None
            }
            Either::Right(Some(Ok(_))) => Some((self, HostedDownlinkEvent::Written)),
            Either::Right(Some(Err(e))) => {
                *write_stream = None;
                Some((self, HostedDownlinkEvent::WriterFailed(e)))
            }
            Either::Right(_) => {
                *write_stream = None;
                Some((self, HostedDownlinkEvent::WriterTerminated))
            }
            _ => None,
        }
    }
}

impl<ItemModel, Lifecycle> AgentModel<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + 'static,
{
    /// Initialize the agent, performing the initial setup for all of the lanes (including triggering the
    /// `on_start` event).
    ///
    /// #Arguments
    /// * `route` - The node URI for the agent instance.
    /// * `config` - Agent specific configuration parameters.
    /// * `context` - Context through which to communicate with the runtime.
    async fn initialize_agent(
        self,
        route: RouteUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> AgentInitResult
    where
        ItemModel: AgentSpec,
        Lifecycle: AgentLifecycle<ItemModel>,
    {
        let AgentModel {
            item_model_fac,
            lifecycle,
        } = self;

        let meta = AgentMetadata::new(&route, &config);

        let mut value_like_lane_io = HashMap::new();
        let mut map_lane_io = HashMap::new();
        let mut value_like_store_io = HashMap::new();
        let mut map_store_io = HashMap::new();

        let val_lane_specs = ItemModel::value_like_item_specs();
        let map_lane_specs = ItemModel::map_like_item_specs();
        let item_ids = <ItemModel as AgentSpec>::item_ids();

        let suspended = FuturesUnordered::new();
        let downlink_channels = RefCell::new(vec![]);

        let item_model = item_model_fac.create();

        {
            let mut lane_init_tasks = FuturesUnordered::new();
            let default_lane_config = config.default_lane_config.unwrap_or_default();
            // Set up the lanes of the agent.
            for (name, spec) in val_lane_specs {
                let mut lane_conf = default_lane_config;
                if spec.flags.contains(LaneFlags::TRANSIENT) {
                    lane_conf.transient = true;
                }
                match spec.kind {
                    ItemKind::Lane => {
                        let io = context.add_lane(name, LaneKind::Value, lane_conf).await?;
                        if lane_conf.transient {
                            value_like_lane_io.insert(Text::new(name), io);
                        } else if let Some(init) = item_model.init_value_like_item(name) {
                            let init_task = run_item_initializer(
                                ItemKind::Lane,
                                name,
                                UplinkKind::Value,
                                io,
                                WithLengthBytesCodec::default(),
                                init,
                            );
                            lane_init_tasks.push(init_task.boxed());
                        } else {
                            value_like_lane_io.insert(Text::new(name), io);
                        }
                    }
                    ItemKind::Store => {
                        if !lane_conf.transient {
                            match context.add_store(name, StoreKind::Value).await {
                                Ok(io) => {
                                    if let Some(init) = item_model.init_value_like_item(name) {
                                        let init_task = run_item_initializer(
                                            ItemKind::Store,
                                            name,
                                            UplinkKind::Value,
                                            io,
                                            WithLengthBytesCodec::default(),
                                            init,
                                        );
                                        lane_init_tasks.push(init_task.boxed());
                                    }
                                }
                                Err(OpenStoreError::StoresNotSupported) => {
                                    info!(
                                        name,
                                        "Lane running as transient as stores not supported."
                                    );
                                }
                                Err(OpenStoreError::RuntimeError(err)) => return Err(err.into()),
                                _ => todo!(),
                            }
                        }
                    }
                }
            }
            for (name, spec) in map_lane_specs {
                if value_like_lane_io.contains_key(name) {
                    return Err(AgentInitError::DuplicateLane(Text::new(name)));
                }
                let mut lane_conf = default_lane_config;
                if spec.flags.contains(LaneFlags::TRANSIENT) {
                    lane_conf.transient = true;
                }
                let io = context.add_lane(name, LaneKind::Map, lane_conf).await?;
                if lane_conf.transient {
                    map_lane_io.insert(Text::new(name), io);
                } else if let Some(init) = item_model.init_map_like_item(name) {
                    let init_task = run_item_initializer(
                        ItemKind::Lane,
                        name,
                        UplinkKind::Map,
                        io,
                        MapMessageDecoder::new(RawMapOperationDecoder::default()),
                        init,
                    );
                    lane_init_tasks.push(init_task.boxed());
                } else {
                    map_lane_io.insert(Text::new(name), io);
                }
            }

            while let Some(result) = lane_init_tasks.next().await {
                let InitializedItem {
                    item_kind,
                    name,
                    kind,
                    init_fn,
                    io,
                } = result.map_err(AgentInitError::LaneInitializationFailure)?;
                init_fn(&item_model);
                match (item_kind, kind) {
                    (ItemKind::Lane, UplinkKind::Value | UplinkKind::Supply) => {
                        value_like_lane_io.insert(Text::new(name), io);
                    }
                    (ItemKind::Store, UplinkKind::Map) => {
                        let (tx, _) = io;
                        map_store_io.insert(Text::new(name), tx);
                    }
                    (ItemKind::Lane, UplinkKind::Map) => {
                        map_lane_io.insert(Text::new(name), io);
                    }
                    _ => {
                        let (tx, _) = io;
                        value_like_store_io.insert(Text::new(name), tx);
                    }
                };
            }
        }

        // Run the agent's `on_start` event handler.
        let on_start_handler = lifecycle.on_start();

        if let Err(e) = run_handler(
            ActionContext::new(&suspended, &*context, &downlink_channels),
            meta,
            &item_model,
            &lifecycle,
            on_start_handler,
            &item_ids,
            &mut Discard,
        ) {
            return Err(AgentInitError::UserCodeError(Box::new(e)));
        }
        let agent_task = AgentTask {
            item_model,
            lifecycle,
            route,
            config,
            item_ids,
            value_like_lane_io,
            value_like_store_io,
            map_lane_io,
            map_store_io,
            suspended,
            downlink_channels: downlink_channels.into_inner(),
        };
        Ok(agent_task.run_agent(context).boxed())
    }
}

struct AgentTask<ItemModel, Lifecycle> {
    item_model: ItemModel,
    lifecycle: Lifecycle,
    route: RouteUri,
    config: AgentConfig,
    item_ids: HashMap<u64, Text>,
    value_like_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    value_like_store_io: HashMap<Text, ByteWriter>,
    map_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    map_store_io: HashMap<Text, ByteWriter>,
    suspended: FuturesUnordered<HandlerFuture<ItemModel>>,
    downlink_channels: Vec<(BoxDownlinkChannel<ItemModel>, WriteStream)>,
}

impl<ItemModel, Lifecycle> AgentTask<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + 'static,
{
    /// Core event loop for the agent that routes incoming data from the runtime to the lanes and
    /// state changes from the lanes to the runtime.
    ///
    /// #Arguments
    /// * `_context` - Context through which to communicate with the runtime.
    async fn run_agent(
        self,
        context: Box<dyn AgentContext + Send>, //Will be needed when downlinks are supported.
    ) -> Result<(), AgentTaskError> {
        let AgentTask {
            item_model,
            lifecycle,
            route,
            config,
            item_ids,
            value_like_lane_io,
            value_like_store_io,
            map_lane_io,
            map_store_io,
            mut suspended,
            downlink_channels,
        } = self;
        let meta = AgentMetadata::new(&route, &config);

        let mut item_ids_rev = HashMap::new();
        for (id, name) in &item_ids {
            item_ids_rev.insert(name.clone(), *id);
        }

        let mut lane_readers = SelectAll::new();
        let mut item_writers = HashMap::new();
        let mut pending_writes = FuturesUnordered::new();
        let mut downlinks = FuturesUnordered::new();

        // Start waiting on downlinks from the init phase.
        for (channel, write_stream) in downlink_channels {
            let dl = HostedDownlink::new(channel, write_stream);
            downlinks.push(dl.wait_on_downlink());
        }

        // Set up readers and writes for each lane.
        for (name, (tx, rx)) in value_like_lane_io {
            let id = item_ids_rev[&name];
            lane_readers.push(LaneReader::value(id, rx));
            item_writers.insert(id, ItemWriter::new(id, tx));
        }

        for (name, tx) in value_like_store_io {
            let id = item_ids_rev[&name];
            item_writers.insert(id, ItemWriter::new(id, tx));
        }

        for (name, (tx, rx)) in map_lane_io {
            let id = item_ids_rev[&name];
            lane_readers.push(LaneReader::map(id, rx));
            item_writers.insert(id, ItemWriter::new(id, tx));
        }

        for (name, tx) in map_store_io {
            let id = item_ids_rev[&name];
            item_writers.insert(id, ItemWriter::new(id, tx));
        }

        // This set keeps track of which items have data to be written (according to executed event handlers).
        let mut dirty_items: HashSet<u64> = HashSet::new();

        loop {
            let select_event = async {
                tokio::select! {
                    maybe_suspended = suspended.next(), if !suspended.is_empty() => {
                        maybe_suspended.map(|handler| TaskEvent::SuspendedComplete { handler })
                    }
                    maybe_downlink = downlinks.next(), if !downlinks.is_empty() => {
                        maybe_downlink.map(|downlink_event| TaskEvent::DownlinkReady { downlink_event })
                    }
                    maybe_req = lane_readers.next() => {
                        maybe_req.map(|req| {
                            match req {
                                (id, Ok(Either::Left(request))) => TaskEvent::ValueRequest{
                                    id, request
                                },
                                (id, Ok(Either::Right(request))) => TaskEvent::MapRequest{
                                    id, request
                                },
                                (id, Err(error)) => TaskEvent::RequestError {
                                    id, error
                                },
                            }
                        })
                    }
                }
            };
            let task_event: TaskEvent<ItemModel> = tokio::select! {
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
                maybe_event = select_event => {
                    if let Some(event) = maybe_event {
                        event
                    } else {
                        break Ok(());
                    }
                }
            };
            let add_downlink = |channel, write_stream| {
                let dl = HostedDownlink::new(channel, write_stream);
                downlinks.push(dl.wait_on_downlink());
                Ok(())
            };
            match task_event {
                TaskEvent::WriteComplete { writer, result } => {
                    if result.is_err() {
                        break Ok(()); //Failing to write indicates that the runtime has stopped so we can exit without an error.
                    }
                    item_writers.insert(writer.lane_id(), writer);
                }
                TaskEvent::SuspendedComplete { handler } => {
                    if let Err(e) = run_handler(
                        ActionContext::new(&suspended, &*context, &add_downlink),
                        meta,
                        &item_model,
                        &lifecycle,
                        handler,
                        &item_ids,
                        &mut dirty_items,
                    ) {
                        break Err(AgentTaskError::UserCodeError(Box::new(e)));
                    }
                }
                TaskEvent::DownlinkReady { downlink_event } => {
                    if let Some((mut downlink, event)) = downlink_event {
                        match event {
                            HostedDownlinkEvent::WriterFailed(err) => {
                                error!(error = %err, "A downlink hosted by the agent failed.");
                                downlinks.push(downlink.wait_on_downlink());
                            }
                            HostedDownlinkEvent::WriterTerminated => {
                                info!("A downlink hosted by the agent stopped writing output.");
                                downlinks.push(downlink.wait_on_downlink());
                            }
                            HostedDownlinkEvent::HandlerReady => {
                                if let Some(handler) = downlink.channel.next_event(&item_model) {
                                    if let Err(e) = run_handler(
                                        ActionContext::new(&suspended, &*context, &add_downlink),
                                        meta,
                                        &item_model,
                                        &lifecycle,
                                        handler,
                                        &item_ids,
                                        &mut dirty_items,
                                    ) {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                    }
                                }
                                downlinks.push(downlink.wait_on_downlink());
                            }
                            _ => {}
                        }
                    }
                }
                TaskEvent::ValueRequest { id, request } => {
                    let name = &item_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = item_model.on_value_command(name.as_str(), body)
                            {
                                if let Err(e) = run_handler(
                                    ActionContext::new(&suspended, &*context, &add_downlink),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    ActionContext::new(&suspended, &*context, &add_downlink),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::InitComplete => {}
                    }
                }
                TaskEvent::MapRequest { id, request } => {
                    let name = &item_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = item_model.on_map_command(name.as_str(), body) {
                                if let Err(e) = run_handler(
                                    ActionContext::new(&suspended, &*context, &add_downlink),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    ActionContext::new(&suspended, &*context, &add_downlink),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::InitComplete => {}
                    }
                }
                TaskEvent::RequestError { id, error } => {
                    let lane = item_ids[&id].clone();
                    break Err(AgentTaskError::BadFrame { lane, error });
                }
            }
            // Attempt to write to the outgoing buffers for any items with data.
            dirty_items.retain(|id| {
                if let Some(mut tx) = item_writers.remove(id) {
                    let name = &item_ids[id];
                    match item_model.write_event(name.as_str(), &mut tx.buffer) {
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
        let discard = |_, _| {
            Err(DownlinkRuntimeError::RuntimeError(
                AgentRuntimeError::Stopping,
            ))
        };
        if let Err(e) = run_handler(
            ActionContext::new(&suspended, &*context, &discard),
            meta,
            &item_model,
            &lifecycle,
            on_stop_handler,
            &item_ids,
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

/// When the agent is initializing, no IO is taking place so we simply discard the IDs.
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
/// there will be no error and no side effects will occur.
///
/// TODO: This method is recursive and has no checks to detect cycles. It would be very easy to create a set of
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
/// * `items` - Mapping between item IDs (returned by the handler to indicate that it has changed the state of
/// an item) an the item names (which are used by the lifecycle to identify the items).
/// * `collector` - Collects the IDs of lanes with state changes.
fn run_handler<Context, Lifecycle, Handler, Collector>(
    action_context: ActionContext<Context>,
    meta: AgentMetadata,
    context: &Context,
    lifecycle: &Lifecycle,
    mut handler: Handler,
    items: &HashMap<u64, Text>,
    collector: &mut Collector,
) -> Result<(), EventHandlerError>
where
    Lifecycle: ItemEvent<Context>,
    Handler: EventHandler<Context>,
    Collector: IdCollector,
{
    loop {
        match handler.step(action_context, meta, context) {
            StepResult::Continue { modified_item } => {
                if let Some((modification, lane)) = modified_item.and_then(|modification| {
                    items
                        .get(&modification.item_id)
                        .map(|name| (modification, name))
                }) {
                    collector.add_id(modification.item_id);
                    if modification.trigger_handler {
                        if let Some(consequence) = lifecycle.item_event(context, lane.as_str()) {
                            run_handler(
                                action_context,
                                meta,
                                context,
                                lifecycle,
                                consequence,
                                items,
                                collector,
                            )?;
                        }
                    }
                }
            }
            StepResult::Fail(err) => {
                break Err(err);
            }
            StepResult::Complete { modified_item, .. } => {
                if let Some((modification, lane)) = modified_item.and_then(|modification| {
                    items
                        .get(&modification.item_id)
                        .map(|name| (modification, name))
                }) {
                    collector.add_id(modification.item_id);
                    if modification.trigger_handler {
                        if let Some(consequence) = lifecycle.item_event(context, lane.as_str()) {
                            run_handler(
                                action_context,
                                meta,
                                context,
                                lifecycle,
                                consequence,
                                items,
                                collector,
                            )?;
                        }
                    }
                }
                break Ok(());
            }
        }
    }
}
