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

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{ready, Poll};

use bytes::{Bytes, BytesMut};
use downlink::BoxDownlinkChannelFactory;
use futures::future::{Fuse, OptionFuture};
use futures::{
    future::{BoxFuture, FusedFuture},
    stream::{FuturesUnordered, SelectAll},
    StreamExt,
};
use futures::{Future, FutureExt};
use pin_project::pin_project;
use swimos_agent_protocol::encoding::store::{RawMapStoreInitDecoder, RawValueStoreInitDecoder};
use swimos_agent_protocol::{LaneRequest, MapMessage};
use swimos_api::agent::DownlinkKind;
use swimos_api::agent::{HttpLaneRequest, LaneConfig, RawHttpLaneResponse};
use swimos_api::error::{
    AgentRuntimeError, CommanderRegistrationError, DownlinkRuntimeError, DynamicRegistrationError,
    LaneSpawnError, OpenStoreError,
};
use swimos_api::{
    address::Address,
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult},
    error::{AgentInitError, AgentTaskError, FrameIoError},
    http::{Header, StandardHeaderName, StatusCode, Version},
};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::errors::Recoverable;
use swimos_utilities::future::RetryStrategy;
use swimos_utilities::routing::RouteUri;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Instant, Sleep};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use crate::agent_lifecycle::item_event::ItemEvent;
use crate::agent_model::io::LaneReadEvent;
use crate::event_handler::{
    ActionContext, BoxJoinLaneInit, DownlinkSpawnOnDone, HandlerFuture, LaneSpawnOnDone,
    LaneSpawner, LinkSpawner, LocalBoxEventHandler, ModificationFlags, Sequentially, Spawner,
};
use crate::{
    agent_lifecycle::AgentLifecycle,
    event_handler::{EventHandler, EventHandlerError, HandlerAction, StepResult},
    meta::AgentMetadata,
};

/// Support for executing downlink lifecycles within agents.
pub mod downlink;
mod init;
mod io;
#[cfg(test)]
mod tests;

use io::{ItemWriter, LaneReader};

use bitflags::bitflags;

use self::downlink::{BoxDownlinkChannel, DownlinkChannelError, DownlinkChannelEvent};
use self::init::{run_item_initializer, InitializedItem};
pub use init::{
    ItemInitializer, MapLaneInitializer, MapStoreInitializer, ValueLaneInitializer,
    ValueStoreInitializer,
};
pub use swimos_api::agent::{StoreKind, WarpLaneKind};

/// Response from a lane after it has written bytes to its outgoing buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    // The lane has no data to write (the buffer is still empty).
    NoData,
    // The lane has written data to the buffer but has no more data to write.
    Done,
    // The lane has written data to the buffer and will have more to write on a subsequent call.
    DataStillAvailable,
    // The lane has written data to the buffer and needs it's event handler to be executed for
    // more to be available.
    RequiresEvent,
}

/// Enumerates the kinds of items that agents can have (excluding HTTP lanes).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum ItemKind {
    Lane(WarpLaneKind),
    Store(StoreKind),
}

impl std::fmt::Display for ItemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemKind::Lane(k) => write!(f, "{}", k),
            ItemKind::Store(k) => write!(f, "{}", k),
        }
    }
}

impl ItemKind {
    /// Whether an item is map like and so uses the map protocol to communicate with the runtime.
    pub fn map_like(&self) -> bool {
        match self {
            ItemKind::Lane(ty) => ty.map_like(),
            ItemKind::Store(StoreKind::Map) => true,
            ItemKind::Store(StoreKind::Value) => false,
        }
    }

    /// Whether an item is a lane (as opposed to a store) and so should be publicly exposed by the runtime.
    pub fn is_lane(&self) -> bool {
        matches!(self, ItemKind::Lane(_))
    }
}

impl ItemKind {
    pub const VALUE_LANE: ItemKind = ItemKind::Lane(WarpLaneKind::Value);
    pub const MAP_LANE: ItemKind = ItemKind::Lane(WarpLaneKind::Map);
    pub const VALUE_STORE: ItemKind = ItemKind::Store(StoreKind::Value);
    pub const MAP_STORE: ItemKind = ItemKind::Store(StoreKind::Map);
}

bitflags! {
    /// Flags to instruct the runtime on how to handle a lane.
    #[derive(Default, Copy, Clone, Hash, Debug, PartialEq, Eq)]
    pub struct ItemFlags: u8 {
        /// The state of the item should not be persisted.
        const TRANSIENT = 0b01;
    }
}

/// Type information about an item of an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemDescriptor {
    WarpLane {
        kind: WarpLaneKind,
        flags: ItemFlags,
    },
    Store {
        kind: StoreKind,
        flags: ItemFlags,
    },
    Http,
}

/// Full description of an item of an agent.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ItemSpec {
    /// Unique (within the agent) ID of the item.
    pub id: u64,
    /// The name of them item (use in the Warp address for lanes).
    pub lifecycle_name: &'static str,
    /// Type information for the item.
    pub descriptor: ItemDescriptor,
}

impl ItemSpec {
    pub fn new(id: u64, lifecycle_name: &'static str, descriptor: ItemDescriptor) -> Self {
        ItemSpec {
            id,
            lifecycle_name,
            descriptor,
        }
    }
}

#[doc(hidden)]
pub type MapLikeInitializer<T> =
    Box<dyn ItemInitializer<T, MapMessage<BytesMut, BytesMut>> + Send + 'static>;

#[doc(hidden)]
pub type ValueLikeInitializer<T> = Box<dyn ItemInitializer<T, BytesMut> + Send + 'static>;

pub trait AgentDescription {
    /// Get the item name for an item ID.
    fn item_name(&self, id: u64) -> Option<Cow<'_, str>>;
}

/// A trait which describes the lanes of an agent which can be run as a task attached to an
/// [`AgentContext`]. A type implementing this trait is sufficient to produce a functional agent
/// although it will not provided any lifecycle events for the agent or its lanes.
pub trait AgentSpec: AgentDescription + Sized + Send {
    /// The type of handler to run when a command is received for a value lane.
    type ValCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a command is received for a map lane.
    type MapCommandHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of handler to run when a request is received to sync with a lane.
    type OnSyncHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The type of the handler to run when an HTTP request is received for a lane.
    type HttpRequestHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The names and flags of all items (lanes and stores) in the agent.
    fn item_specs() -> HashMap<&'static str, ItemSpec>;

    /// Create a handler that will update the state of the agent when a command is received
    /// for a value lane. There will be no handler if the lane does not exist or does not
    /// accept commands.
    ///
    /// # Arguments
    /// * `lane` - The name of the lane.
    /// * `body` - The content of the command.
    fn on_value_command(&self, lane: &str, body: BytesMut) -> Option<Self::ValCommandHandler>;

    /// Create an initializer that will consume the state of a value-like item, as reported by the runtime.
    ///
    /// # Arguments
    /// * `lane` - The name of the item.
    fn init_value_like_item(&self, item: &str) -> Option<ValueLikeInitializer<Self>>
    where
        Self: 'static;

    /// Create an initializer that will consume the state of a map-like item, as reported by the runtime.
    ///
    /// # Arguments
    /// * `item` - The name of the item.
    fn init_map_like_item(&self, item: &str) -> Option<MapLikeInitializer<Self>>
    where
        Self: 'static;

    /// Create a handler that will update the state of the agent when a command is received
    /// for a map lane. There will be no handler if the lane does not exist or does not
    /// accept commands.
    /// # Arguments
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
    /// # Arguments
    /// * `lane` - The name of the lane.
    /// * `id` - The ID of the remote that requested the sync.
    fn on_sync(&self, lane: &str, id: Uuid) -> Option<Self::OnSyncHandler>;

    /// Create a handler that will update the state of the agent when an HTTP request is
    /// made to a lane. If no HTTP lane exists with the specified name the request will
    /// be returned as an error (so that the caller can handle it will a 404 response).
    ///
    /// # Arguments
    /// * `lane` - The name of the lane.
    /// * `request` - The HTTP request.
    fn on_http_request(
        &self,
        lane: &str,
        request: HttpLaneRequest,
    ) -> Result<Self::HttpRequestHandler, HttpLaneRequest>;

    /// Attempt to write pending data from a lane into the outgoing buffer. The result will
    /// indicate if data was written and if the lane has more data to write. There will be
    /// no result if the lane does not exist.
    fn write_event(&self, lane: &str, buffer: &mut BytesMut) -> Option<WriteResult>;

    /// Register a dynamically created item with the agent. The returned value is the unique ID of the item.
    ///
    /// # Arguments
    /// * `_name` - The name of the item.
    /// * `_descriptor` - The kind of the item and any flags.
    fn register_dynamic_item(
        &self,
        _name: &str,
        _descriptor: ItemDescriptor,
    ) -> Result<u64, DynamicRegistrationError> {
        Err(DynamicRegistrationError::DynamicRegistrationsNotSupported)
    }
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

/// A factory for creating [`AgentLifecycle`]s.
/// An agent route may need to create an agent instance multiple times (for parameterized routes or
/// in cases where the agent is stopped and restarted). Therefore, a route is registered with a
/// factory rather then a single instance of the lifecycle.
pub trait LifecycleFactory<ItemModel>: Send + Sync {
    type LifecycleType: AgentLifecycle<ItemModel> + Send;

    /// Create an instance of the agent type.
    fn create(&self) -> Self::LifecycleType;
}

/// A lifecycle factory that creates clones of a provided instance.
struct CloneableLifecycle<LC>(LC);

impl<ItemModel, LC> LifecycleFactory<ItemModel> for CloneableLifecycle<LC>
where
    LC: Send + Sync + Clone + AgentLifecycle<ItemModel>,
{
    type LifecycleType = LC;

    fn create(&self) -> Self::LifecycleType {
        self.0.clone()
    }
}

struct FnLifecycleFac<F>(F);

impl<ItemModel, F, LC> LifecycleFactory<ItemModel> for FnLifecycleFac<F>
where
    F: Fn() -> LC + Send + Sync,
    LC: Send + AgentLifecycle<ItemModel>,
{
    type LifecycleType = LC;

    fn create(&self) -> Self::LifecycleType {
        self.0()
    }
}

/// The complete model for an agent consisting of an implementation of [`AgentSpec`] to describe the lanes
/// of the agent and an implementation of [`AgentLifecycle`] to describe the lifecycle events that will trigger,
/// for  example, when the agent starts or stops or when the state of a lane changes.
pub struct AgentModel<ItemModel, Lifecycle> {
    item_model_fac: Arc<dyn ItemModelFactory<ItemModel = ItemModel>>,
    lifecycle_fac: Arc<dyn LifecycleFactory<ItemModel, LifecycleType = Lifecycle>>,
}

impl<ItemModel, Lifecycle> Clone for AgentModel<ItemModel, Lifecycle> {
    fn clone(&self) -> Self {
        Self {
            item_model_fac: self.item_model_fac.clone(),
            lifecycle_fac: self.lifecycle_fac.clone(),
        }
    }
}

impl<ItemModel, Lifecycle> AgentModel<ItemModel, Lifecycle>
where
    Lifecycle: Send + Sync + Clone + AgentLifecycle<ItemModel> + 'static,
{
    pub fn new<F>(item_model_fac: F, lifecycle: Lifecycle) -> Self
    where
        F: ItemModelFactory<ItemModel = ItemModel> + Sized + 'static,
    {
        AgentModel {
            item_model_fac: Arc::new(item_model_fac),
            lifecycle_fac: Arc::new(CloneableLifecycle(lifecycle)),
        }
    }

    pub fn from_arc(
        item_model_fac: Arc<dyn ItemModelFactory<ItemModel = ItemModel>>,
        lifecycle: Lifecycle,
    ) -> Self {
        AgentModel {
            item_model_fac,
            lifecycle_fac: Arc::new(CloneableLifecycle(lifecycle)),
        }
    }
}

impl<ItemModel, Lifecycle> AgentModel<ItemModel, Lifecycle>
where
    Lifecycle: Send + AgentLifecycle<ItemModel> + 'static,
{
    pub fn from_fn<F, G>(item_model_fac: F, lifecycle_fn: G) -> Self
    where
        F: ItemModelFactory<ItemModel = ItemModel> + Sized + 'static,
        G: Fn() -> Lifecycle + Send + Sync + 'static,
    {
        AgentModel {
            item_model_fac: Arc::new(item_model_fac),
            lifecycle_fac: Arc::new(FnLifecycleFac(lifecycle_fn)),
        }
    }
}

impl<ItemModel, Lifecycle> Agent for AgentModel<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + Send + 'static,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        self.clone()
            .initialize_agent(route, route_params, config, context)
            .boxed()
    }
}

enum TaskEvent<ItemModel> {
    WriteComplete {
        writer: ItemWriter,
        result: Result<bool, std::io::Error>,
    },
    SuspendedComplete {
        result: SuspendResult<ItemModel>,
    },
    LinksEvent {
        downlink_event: LinksEvent<ItemModel>,
    },
    ValueRequest {
        id: u64,
        request: LaneRequest<BytesMut>,
    },
    MapRequest {
        id: u64,
        request: LaneRequest<MapMessage<BytesMut, BytesMut>>,
    },
    HttpRequest {
        id: u64,
        request: HttpLaneRequest,
    },
    RequestError {
        id: u64,
        error: FrameIoError,
    },
    CommandSendComplete {
        result: Result<CommandWriter, std::io::Error>,
    },
}

struct HostedDownlink<Context> {
    channel: BoxDownlinkChannel<Context>,
    failed: bool,
}

impl<Context> HostedDownlink<Context> {
    fn new(channel: BoxDownlinkChannel<Context>) -> Self {
        HostedDownlink {
            channel,
            failed: false,
        }
    }

    fn address(&self) -> &Address<Text> {
        self.channel.address()
    }

    fn kind(&self) -> DownlinkKind {
        self.channel.kind()
    }
}

#[derive(Debug)]
enum HostedDownlinkEvent {
    WriterFailed(std::io::Error),
    Written,
    WriterTerminated,
    HandlerReady {
        failed: bool,
    },
    ReconnectNotPossible {
        retries_expired: bool,
    },
    ReconnectSucceeded(ReconnectDownlink),
    ReconnectFailed {
        error: DownlinkRuntimeError,
        retry: RetryStrategy,
    },
    Stopped,
}

impl<Context> HostedDownlink<Context> {
    async fn wait_on_downlink(mut self) -> (Self, HostedDownlinkEvent) {
        let HostedDownlink { channel, failed } = &mut self;

        if *failed {
            return (self, HostedDownlinkEvent::Stopped);
        }

        match channel.await_ready().await {
            Some(Ok(DownlinkChannelEvent::HandlerReady)) => {
                (self, HostedDownlinkEvent::HandlerReady { failed: false })
            }
            Some(Ok(DownlinkChannelEvent::WriteCompleted)) => (self, HostedDownlinkEvent::Written),
            Some(Ok(DownlinkChannelEvent::WriteStreamTerminated)) => {
                (self, HostedDownlinkEvent::WriterTerminated)
            }
            Some(Err(DownlinkChannelError::ReadFailed)) => {
                *failed = true;
                (self, HostedDownlinkEvent::HandlerReady { failed: true })
            }
            Some(Err(DownlinkChannelError::WriteFailed(err))) => {
                (self, HostedDownlinkEvent::WriterFailed(err))
            }
            None => (self, HostedDownlinkEvent::Stopped),
        }
    }

    async fn flush(&mut self) -> Result<(), std::io::Error> {
        self.channel.flush().await
    }

    async fn reconnect(
        mut self,
        agent_context: &dyn AgentContext,
        mut retry: RetryStrategy,
        first_attempt: bool,
    ) -> (Self, HostedDownlinkEvent) {
        if self.channel.can_restart() {
            if let Err(err) = self.flush().await {
                debug!(error = %err, "Flushing downlink before restart failed.");
            }
            let HostedDownlink { channel, .. } = &mut self;
            let Address { host, node, lane } = channel.address().borrow_parts();
            let open_task = agent_context.open_downlink(host, node, lane, channel.kind());
            let delay = if first_attempt {
                None
            } else {
                match retry.next() {
                    Some(delay) => delay,
                    None => {
                        return (
                            self,
                            HostedDownlinkEvent::ReconnectNotPossible {
                                retries_expired: true,
                            },
                        )
                    }
                }
            };
            if let Some(t) = delay {
                tokio::time::sleep(t).await;
            }
            let result = match open_task.await {
                Ok((writer, reader)) => {
                    HostedDownlinkEvent::ReconnectSucceeded(ReconnectDownlink::new(writer, reader))
                }
                Err(error) => HostedDownlinkEvent::ReconnectFailed { error, retry },
            };
            (self, result)
        } else {
            (
                self,
                HostedDownlinkEvent::ReconnectNotPossible {
                    retries_expired: false,
                },
            )
        }
    }

    fn next_event(&mut self, context: &Context) -> Option<LocalBoxEventHandler<'_, Context>> {
        self.channel.next_event(context)
    }
}

#[derive(Debug)]
struct ReconnectDownlink {
    writer: ByteWriter,
    reader: ByteReader,
}

impl ReconnectDownlink {
    fn new(writer: ByteWriter, reader: ByteReader) -> Self {
        ReconnectDownlink { writer, reader }
    }
}

impl ReconnectDownlink {
    fn connect<Context>(self, downlink: &mut HostedDownlink<Context>, context: &Context) {
        let ReconnectDownlink { writer, reader } = self;
        let HostedDownlink { channel, failed } = downlink;
        channel.connect(context, writer, reader);
        *failed = false;
    }
}

impl<ItemModel, Lifecycle> AgentModel<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + 'static,
{
    /// Initialize the agent, performing the initial setup for all of the lanes (including triggering the
    /// `on_start` event).
    ///
    /// # Arguments
    /// * `route` - The node URI for the agent instance.
    /// * `route_params` - Parameters extracted from the route URI.
    /// * `config` - Agent specific configuration parameters.
    /// * `context` - Context through which to communicate with the runtime.
    async fn initialize_agent(
        self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> AgentInitResult
    where
        ItemModel: AgentSpec,
        Lifecycle: AgentLifecycle<ItemModel>,
    {
        let AgentModel {
            item_model_fac,
            lifecycle_fac,
        } = self;

        let lifecycle = lifecycle_fac.create();

        let meta = AgentMetadata::new(&route, &route_params, &config);

        let mut lane_io = HashMap::new();
        let mut store_io = HashMap::new();
        let mut http_lane_rxs = HashMap::new();

        let item_specs = ItemModel::item_specs();
        let mut lifecycle_item_ids: HashMap<u64, Text> = item_specs
            .values()
            .map(|spec| (spec.id, Text::new(spec.lifecycle_name)))
            .collect();

        let mut external_item_ids: HashMap<Text, u64> = item_specs
            .iter()
            .map(|(name, spec)| (Text::new(name), spec.id))
            .collect();

        let suspended = FuturesUnordered::new();
        let link_requests = RefCell::new(LinkRequestCollector::default());
        let mut dynamic_lanes = RefCell::new(vec![]);
        let mut join_lane_init = HashMap::new();
        let mut command_buffer = BytesMut::new();

        let item_model = item_model_fac.create();
        let default_lane_config = config.default_lane_config.unwrap_or_default();

        {
            let mut lane_init_tasks = FuturesUnordered::new();
            macro_rules! with_init {
                ($init:ident => $body:block) => {{
                    let mut $init = InitContext::new(&mut lane_io, &item_model, &lane_init_tasks);

                    $body
                }};
            }
            for (name, spec) in item_specs {
                match spec.descriptor {
                    ItemDescriptor::WarpLane { kind, flags } => {
                        let key = (Text::new(name), kind);
                        if kind.map_like() {
                            if lane_io.contains_key(&key) {
                                return Err(AgentInitError::DuplicateLane(key.0));
                            }
                            let mut lane_conf = default_lane_config;
                            if flags.contains(ItemFlags::TRANSIENT) {
                                lane_conf.transient = true;
                            }
                            let io = context.add_lane(name, kind, lane_conf).await?;
                            with_init!(init => {
                                init.init_map_lane(name, kind, lane_conf, io);
                            })
                        } else {
                            if lane_io.contains_key(&key) {
                                return Err(AgentInitError::DuplicateLane(key.0));
                            }
                            let mut lane_conf = default_lane_config;
                            if flags.contains(ItemFlags::TRANSIENT) {
                                lane_conf.transient = true;
                            }
                            let io = context.add_lane(name, kind, lane_conf).await?;
                            with_init!(init => {
                                init.init_value_lane(name, kind, lane_conf, io);
                            })
                        }
                    }
                    ItemDescriptor::Store {
                        kind: StoreKind::Map,
                        flags,
                    } => {
                        if !flags.contains(ItemFlags::TRANSIENT) {
                            if let Some(io) = handle_store_error(
                                context.add_store(name, StoreKind::Map).await,
                                name,
                            )? {
                                with_init!(init => {
                                    init.init_map_store(name, io);
                                })
                            }
                        }
                    }
                    ItemDescriptor::Store {
                        kind: StoreKind::Value,
                        flags,
                    } => {
                        if !flags.contains(ItemFlags::TRANSIENT) {
                            if let Some(io) = handle_store_error(
                                context.add_store(name, StoreKind::Value).await,
                                name,
                            )? {
                                with_init!(init => {
                                    init.init_value_store(name, io);
                                })
                            }
                        }
                    }
                    ItemDescriptor::Http => {
                        let channel = context.add_http_lane(name).await?;
                        http_lane_rxs.insert(Text::new(name), channel);
                    }
                }
            }

            while let Some(result) = lane_init_tasks.next().await {
                let InitializedItem {
                    item_kind,
                    name,
                    init_fn,
                    io,
                } = result.map_err(AgentInitError::LaneInitializationFailure)?;
                init_fn(&item_model);
                match item_kind {
                    ItemKind::Lane(kind) => {
                        lane_io.insert((Text::new(name), kind), io);
                    }
                    //The receivers for stores are no longer needed as the runtime never sends messages after initialization.
                    ItemKind::Store(_) => {
                        let (tx, _) = io;
                        store_io.insert(Text::new(name), tx);
                    }
                };
            }
        }

        lifecycle.initialize(
            &mut ActionContext::new(
                &suspended,
                &link_requests,
                &dynamic_lanes,
                &mut join_lane_init,
                &mut command_buffer,
            ),
            meta,
            &item_model,
        );

        let mut dyn_lane_handlers = vec![];

        // This handles any requests that have been received to open dynamic lanes. Due to complex, interleaved
        // borrows of non-send variables, it is impractical to extract this out as a function. This macro is a
        // compromise to avoid duplicating the block of code.
        macro_rules! handle_dyn_lanes {
            () => {
                for LaneSpawnRequest {
                    name,
                    kind,
                    on_done,
                } in dynamic_lanes.get_mut().drain(..)
                {
                    let key = (Text::new(&name), kind);
                    if let Entry::Vacant(entry) = lane_io.entry(key) {
                        let mut lane_conf = default_lane_config;
                        lane_conf.transient = true;
                        let io = context.add_lane(&name, kind, lane_conf).await?;
                        let descriptor = ItemDescriptor::WarpLane {
                            kind,
                            flags: ItemFlags::TRANSIENT, // For now, all dynamic lanes are transient.
                        };
                        let result = item_model.register_dynamic_item(&name, descriptor);
                        if let Ok(id) = result {
                            entry.insert(io);
                            external_item_ids.insert(Text::new(&name), id);
                            lifecycle_item_ids.insert(id, Text::new(&name));
                        }
                        dyn_lane_handlers.push(on_done(result.map_err(Into::into)));
                    } else {
                        let handler = on_done(Err(LaneSpawnError::Registration(
                            DynamicRegistrationError::DuplicateName(name),
                        )));
                        dyn_lane_handlers.push(handler);
                    }
                }
            };
        }

        // Handle any dynamic lane requests received in the `on_init` event. We don't trigger the
        // corresponding event handlers yet to respect the contract that `on_start` is the first
        // event handler to be run in the agent.
        handle_dyn_lanes!();

        // Run the agent's `on_start` event handler.
        let on_start_handler = lifecycle.on_start();

        match run_handler(
            &mut ActionContext::new(
                &suspended,
                &link_requests,
                &dynamic_lanes,
                &mut join_lane_init,
                &mut command_buffer,
            ),
            meta,
            &item_model,
            &lifecycle,
            on_start_handler,
            &lifecycle_item_ids,
            &mut Discard,
        ) {
            Err(EventHandlerError::StopInstructed) => return Err(AgentInitError::FailedToStart),
            Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
            Ok(_) => {}
        }

        // Handle any dynamic lane requests received in the `on_start` event.
        handle_dyn_lanes!();

        // Run all of the event handlers associated with requests for dynamic lanes. These could have bee
        // generated by either (or both) of the `on_init` and `on_start` events.
        match run_handler(
            &mut ActionContext::new(
                &suspended,
                &link_requests,
                &dynamic_lanes,
                &mut join_lane_init,
                &mut command_buffer,
            ),
            meta,
            &item_model,
            &lifecycle,
            Sequentially::new(dyn_lane_handlers),
            &lifecycle_item_ids,
            &mut Discard,
        ) {
            Err(EventHandlerError::StopInstructed) => return Err(AgentInitError::FailedToStart),
            Err(e) => return Err(AgentInitError::UserCodeError(Box::new(e))),
            Ok(_) => {}
        }

        let agent_task = AgentTask {
            item_model,
            lifecycle,
            route,
            route_params,
            config,
            lifecycle_item_ids,
            external_item_ids,
            lane_io,
            store_io,
            http_lane_rxs,
            suspended,
            link_requests: link_requests.into_inner(),
            command_buffer,
            join_lane_init,
        };
        Ok(agent_task.run_agent(context).boxed())
    }
}

type InitFut<'a, ItemModel> = BoxFuture<'a, Result<InitializedItem<'a, ItemModel>, FrameIoError>>;

struct InitContext<'a, 'b, ItemModel> {
    lane_io: &'a mut HashMap<(Text, WarpLaneKind), (ByteWriter, ByteReader)>,
    item_model: &'a ItemModel,
    item_init_tasks: &'a FuturesUnordered<InitFut<'b, ItemModel>>,
}

fn handle_store_error<T>(
    result: Result<T, OpenStoreError>,
    name: &str,
) -> Result<Option<T>, AgentInitError> {
    match result {
        Ok(t) => Ok(Some(t)),
        Err(OpenStoreError::StoresNotSupported) => {
            info!(
                name,
                "Store running as transient as persistence not supported."
            );
            Ok(None)
        }
        Err(OpenStoreError::RuntimeError(err)) => Err(err.into()),
        Err(OpenStoreError::IncorrectStoreKind { requested, actual }) => {
            Err(AgentInitError::IncorrectStoreKind {
                name: Text::new(name),
                requested,
                actual,
            })
        }
    }
}

impl<'a, 'b, ItemModel> InitContext<'a, 'b, ItemModel>
where
    ItemModel: AgentSpec + 'static,
{
    fn new(
        lane_io: &'a mut HashMap<(Text, WarpLaneKind), (ByteWriter, ByteReader)>,
        item_model: &'a ItemModel,
        item_init_tasks: &'a FuturesUnordered<InitFut<'b, ItemModel>>,
    ) -> Self {
        InitContext {
            lane_io,
            item_model,
            item_init_tasks,
        }
    }

    fn init_value_lane(
        &mut self,
        name: &'b str,
        kind: WarpLaneKind,
        lane_conf: LaneConfig,
        io: (ByteWriter, ByteReader),
    ) {
        let InitContext {
            lane_io,
            item_model,
            item_init_tasks,
            ..
        } = self;
        if lane_conf.transient {
            lane_io.insert((Text::new(name), kind), io);
        } else if let Some(init) = item_model.init_value_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Lane(kind),
                name,
                io,
                RawValueStoreInitDecoder::default(),
                init,
            );
            item_init_tasks.push(init_task.boxed());
        } else {
            lane_io.insert((Text::new(name), kind), io);
        }
    }

    fn init_value_store(&mut self, name: &'b str, io: (ByteWriter, ByteReader)) {
        let InitContext {
            item_model,
            item_init_tasks,
            ..
        } = self;
        if let Some(init) = item_model.init_value_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Store(StoreKind::Value),
                name,
                io,
                RawValueStoreInitDecoder::default(),
                init,
            );
            item_init_tasks.push(init_task.boxed());
        }
    }

    fn init_map_lane(
        &mut self,
        name: &'b str,
        kind: WarpLaneKind,
        lane_conf: LaneConfig,
        io: (ByteWriter, ByteReader),
    ) {
        let InitContext {
            lane_io,
            item_model,
            item_init_tasks,
            ..
        } = self;
        if lane_conf.transient {
            lane_io.insert((Text::new(name), kind), io);
        } else if let Some(init) = item_model.init_map_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Lane(kind),
                name,
                io,
                RawMapStoreInitDecoder::default(),
                init,
            );
            item_init_tasks.push(init_task.boxed());
        } else {
            lane_io.insert((Text::new(name), kind), io);
        }
    }

    fn init_map_store(&mut self, name: &'b str, io: (ByteWriter, ByteReader)) {
        let InitContext {
            item_model,
            item_init_tasks,
            ..
        } = self;
        if let Some(init) = item_model.init_map_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Store(StoreKind::Map),
                name,
                io,
                RawMapStoreInitDecoder::default(),
                init,
            );
            item_init_tasks.push(init_task.boxed());
        }
    }
}

struct AgentTask<ItemModel, Lifecycle> {
    item_model: ItemModel,
    lifecycle: Lifecycle,
    route: RouteUri,
    route_params: HashMap<String, String>,
    config: AgentConfig,
    external_item_ids: HashMap<Text, u64>,
    lifecycle_item_ids: HashMap<u64, Text>,
    lane_io: HashMap<(Text, WarpLaneKind), (ByteWriter, ByteReader)>,
    store_io: HashMap<Text, ByteWriter>,
    http_lane_rxs: HashMap<Text, mpsc::Receiver<HttpLaneRequest>>,
    suspended: FuturesUnordered<Suspended<ItemModel>>,
    link_requests: LinkRequestCollector<ItemModel>,
    join_lane_init: HashMap<u64, BoxJoinLaneInit<'static, ItemModel>>,
    command_buffer: BytesMut,
}

impl<ItemModel, Lifecycle> AgentTask<ItemModel, Lifecycle>
where
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + 'static,
{
    /// Core event loop for the agent that routes incoming data from the runtime to the lanes and
    /// state changes from the lanes to the runtime.
    ///
    /// # Arguments
    /// * `_context` - Context through which to communicate with the runtime.
    async fn run_agent(
        self,
        context: Box<dyn AgentContext + Send>, //Will be needed when downlinks are supported.
    ) -> Result<(), AgentTaskError> {
        let AgentTask {
            item_model,
            lifecycle,
            route,
            route_params,
            config,
            lifecycle_item_ids,
            external_item_ids,
            lane_io,
            store_io,
            http_lane_rxs,
            mut suspended,
            link_requests:
                LinkRequestCollector {
                    downlinks: downlink_requests,
                    commander_ids,
                },
            mut join_lane_init,
            mut command_buffer,
        } = self;
        let meta = AgentMetadata::new(&route, &route_params, &config);

        let mut lane_readers = SelectAll::new();
        let mut item_writers = HashMap::new();
        let mut pending_writes = FuturesUnordered::new();
        let mut link_futures = FuturesUnordered::new();
        let mut external_item_ids_rev = HashMap::new();
        let cmd_ids = RefCell::new(commander_ids);

        for (name, id) in external_item_ids.iter() {
            external_item_ids_rev.insert(*id, name);
        }

        let mut cmd_writer = if let Ok(cmd_tx) = context.command_channel().await {
            Some(CommandWriter::new(cmd_tx))
        } else {
            return Err(AgentTaskError::OutputFailed(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            )));
        };

        let mut cmd_send_fut = pin!(OptionFuture::from(None));

        // Start opening downlinks from the init phase.
        for request in downlink_requests {
            let fut = open_new_downlink(
                &*context,
                &request.path,
                request.kind,
                config.keep_linked_retry,
                None,
            );
            link_futures.push(LinkFuture::Opening(Some(request), fut));
        }

        for ((name, kind), (tx, rx)) in lane_io {
            if kind.map_like() {
                let id = external_item_ids[&name];
                lane_readers.push(LaneReader::map(id, rx));
                item_writers.insert(id, ItemWriter::new(id, tx));
            } else {
                let id = external_item_ids[&name];
                lane_readers.push(LaneReader::value(id, rx));
                item_writers.insert(id, ItemWriter::new(id, tx));
            }
        }

        for (name, tx) in store_io {
            let id = external_item_ids[&name];
            item_writers.insert(id, ItemWriter::new(id, tx));
        }

        for (name, rx) in http_lane_rxs {
            let id = external_item_ids[&name];
            lane_readers.push(LaneReader::http(id, rx));
        }

        // We need to check if anything has been written into the command buffer as the agent
        // initialisation process called lifecycle::on_start and that may have sent commands.
        check_cmds(
            &mut command_buffer,
            &mut cmd_writer,
            &mut cmd_send_fut,
            CommandWriter::write,
        );

        // This set keeps track of which items have data to be written (according to executed event handlers).
        let mut dirty_items: HashSet<u64> = HashSet::new();

        loop {
            let select_event = async {
                tokio::select! {
                    maybe_suspended = suspended.next(), if !suspended.is_empty() => {
                        maybe_suspended.map(|result| TaskEvent::SuspendedComplete { result })
                    }
                    maybe_downlink = link_futures.next(), if !link_futures.is_empty() => {
                        maybe_downlink.map(|downlink_event| TaskEvent::LinksEvent { downlink_event })
                    }
                    maybe_req = lane_readers.next(), if !lane_readers.is_empty() => {
                        maybe_req.map(|req| {
                            match req {
                                (id, Ok(LaneReadEvent::Value(request))) => TaskEvent::ValueRequest{
                                    id, request
                                },
                                (id, Ok(LaneReadEvent::Map(request))) => TaskEvent::MapRequest{
                                    id, request
                                },
                                (id, Ok(LaneReadEvent::Http(request))) => TaskEvent::HttpRequest {
                                    id, request
                                },
                                (id, Err(error)) => TaskEvent::RequestError {
                                    id, error
                                },
                            }
                        })
                    }
                    else => None,
                }
            };

            let task_event: TaskEvent<ItemModel> = tokio::select! {
                biased;
                maybe_cmd_result = &mut cmd_send_fut, if !cmd_send_fut.is_terminated() => {
                    if let Some(result) = maybe_cmd_result {
                        TaskEvent::CommandSendComplete { result }
                    } else {
                        continue;
                    }
                },
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
            let add_downlink = |request: DownlinkSpawnRequest<ItemModel>| {
                let fut = open_new_downlink(
                    &*context,
                    &request.path,
                    request.kind,
                    config.keep_linked_retry,
                    None,
                );
                link_futures.push(LinkFuture::Opening(Some(request), fut));
            };
            let add_commander = |address: Address<Text>| cmd_ids.borrow_mut().get_request(&address);
            let add_link = (add_downlink, add_commander);
            let add_lane = NoDynLanes;

            // Calling run_handler is very verbose so is pulled out into this macro to make the code easier to read.
            macro_rules! exec_handler {
                ($handler:expr) => {
                    match run_handler(
                        &mut ActionContext::new(
                            &suspended,
                            &add_link,
                            &add_lane,
                            &mut join_lane_init,
                            &mut command_buffer,
                        ),
                        meta,
                        &item_model,
                        &lifecycle,
                        $handler,
                        &lifecycle_item_ids,
                        &mut dirty_items,
                    ) {
                        Err(EventHandlerError::StopInstructed) => break Ok(()),
                        Err(e) => break Err(AgentTaskError::UserCodeError(Box::new(e))),
                        Ok(_) => check_cmds(
                            &mut command_buffer,
                            &mut cmd_writer,
                            &mut cmd_send_fut,
                            CommandWriter::write,
                        ),
                    }
                };
            }

            match task_event {
                TaskEvent::WriteComplete { writer, result } => {
                    match result {
                        Ok(true) => {
                            // The event handler for the item needs to be executed.
                            let lane = &lifecycle_item_ids[&writer.lane_id()];
                            if let Some(handler) = lifecycle.item_event(&item_model, lane.as_str())
                            {
                                exec_handler!(handler);
                            }
                        }
                        Err(_) => break Ok(()), //Failing to write indicates that the runtime has stopped so we can exit without an error.
                        _ => {}
                    }
                    item_writers.insert(writer.lane_id(), writer);
                }
                TaskEvent::SuspendedComplete {
                    result: SuspendResult::Handler(handler),
                } => {
                    exec_handler!(handler);
                }
                TaskEvent::SuspendedComplete {
                    result: SuspendResult::TimedOut(id),
                } => {
                    let handler = lifecycle.on_timer(id);
                    exec_handler!(handler);
                }
                TaskEvent::LinksEvent {
                    downlink_event: LinksEvent::Opened { request, result },
                } => match result {
                    OpenDownlinkResult::Success(tx, rx) => {
                        let DownlinkSpawnRequest {
                            make_channel,
                            on_done,
                            ..
                        } = request;
                        let channel = make_channel.create_box(&item_model, tx, rx);
                        let downlink = HostedDownlink::new(channel);
                        link_futures.push(LinkFuture::Running(downlink.wait_on_downlink()));
                        let handler = on_done(Ok(()));
                        exec_handler!(handler);
                    }
                    OpenDownlinkResult::Failed(error, retry) => {
                        if error.is_fatal() {
                            let DownlinkSpawnRequest {
                                path,
                                kind,
                                on_done,
                                ..
                            } = request;
                            error!(error = %error, address = %path, kind = ?kind, "Failed to start a downlink due a fatal error.");
                            let handler = on_done(Err(error));
                            exec_handler!(handler);
                        } else {
                            error!(address = %{&request.path}, kind = ?{request.kind}, error = %error, "Starting a downlink failed. Attempting to retry.");
                            let fut = open_new_downlink(
                                &*context,
                                &request.path,
                                request.kind,
                                retry,
                                Some(error),
                            );
                            link_futures.push(LinkFuture::Opening(Some(request), fut));
                        }
                    }
                    OpenDownlinkResult::RetriesExpired(error) => {
                        let DownlinkSpawnRequest {
                            path,
                            kind,
                            on_done,
                            ..
                        } = request;
                        error!(address = %path, kind = ?kind, error = %error, "A downlink could not be established.");
                        let handler = on_done(Err(error));
                        exec_handler!(handler);
                    }
                },
                TaskEvent::LinksEvent {
                    downlink_event:
                        LinksEvent::Event {
                            mut downlink,
                            event,
                            ..
                        },
                } => match event {
                    HostedDownlinkEvent::Written => {
                        link_futures.push(LinkFuture::Running(downlink.wait_on_downlink()))
                    }
                    HostedDownlinkEvent::WriterFailed(err) => {
                        error!(error = %err, "A downlink hosted by the agent failed.");
                        debug!(address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect downlink.");
                        link_futures.push(LinkFuture::Reconnecting(downlink.reconnect(
                            &*context,
                            config.keep_linked_retry,
                            true,
                        )));
                    }
                    HostedDownlinkEvent::WriterTerminated => {
                        debug!("A downlink hosted by the agent stopped writing output.");
                        link_futures.push(LinkFuture::Running(downlink.wait_on_downlink()));
                    }
                    HostedDownlinkEvent::HandlerReady { failed } => {
                        if let Some(handler) = downlink.next_event(&item_model) {
                            exec_handler!(handler);
                        }
                        if failed {
                            error!("Reading from a downlink failed.");
                            debug!(address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect downlink.");
                            link_futures.push(LinkFuture::Reconnecting(downlink.reconnect(
                                &*context,
                                config.keep_linked_retry,
                                true,
                            )));
                        } else {
                            link_futures.push(LinkFuture::Running(downlink.wait_on_downlink()));
                        }
                    }
                    HostedDownlinkEvent::ReconnectSucceeded(reconnect) => {
                        reconnect.connect(&mut downlink, &item_model);
                        link_futures.push(LinkFuture::Running(downlink.wait_on_downlink()));
                    }
                    HostedDownlinkEvent::ReconnectFailed { error, retry } => {
                        if error.is_fatal() {
                            error!(error = %error, address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect a downlink failed with a fatal error.");
                        } else {
                            error!(error = %error, address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect a downlink failed; attempting again.");
                            link_futures.push(LinkFuture::Reconnecting(
                                downlink.reconnect(&*context, retry, false),
                            ));
                        }
                    }
                    HostedDownlinkEvent::Stopped => {
                        link_futures.push(LinkFuture::Reconnecting(downlink.reconnect(
                            &*context,
                            config.keep_linked_retry,
                            true,
                        )));
                    }
                    HostedDownlinkEvent::ReconnectNotPossible { retries_expired } => {
                        if retries_expired {
                            error!(address = %downlink.address(), kind = ?downlink.kind(), "A downlink stopped and could not be restarted.");
                        } else {
                            debug!(address = %downlink.address(), kind = ?downlink.kind(), "A downlink stopped and was removed.");
                        }
                    }
                },
                TaskEvent::ValueRequest { id, request } => {
                    let name = &external_item_ids_rev[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            trace!(name = %name, "Received a command for a value-like lane.");
                            if let Some(handler) = item_model.on_value_command(name.as_str(), body)
                            {
                                let result = run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &add_link,
                                        &add_lane,
                                        &mut join_lane_init,
                                        &mut command_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &lifecycle_item_ids,
                                    &mut dirty_items,
                                );
                                match result {
                                    Err(EventHandlerError::StopInstructed) => break Ok(()),
                                    Err(
                                        e @ (EventHandlerError::RuntimeError(_)
                                        | EventHandlerError::SteppedAfterComplete),
                                    ) => {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                    }
                                    Err(error) => {
                                        debug!(
                                            error = %error,
                                            "Incoming frame was rejected by the item."
                                        );
                                    }
                                    _ => check_cmds(
                                        &mut command_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            trace!(name = %name, remote_id = %remote_id, "Received a sync request for a value-like lane.");
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                exec_handler!(handler);
                            }
                        }
                        LaneRequest::InitComplete => {}
                    }
                }
                TaskEvent::MapRequest { id, request } => {
                    let name = &external_item_ids_rev[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            trace!(name = %name, "Received a command for a map-like lane.");
                            if let Some(handler) = item_model.on_map_command(name.as_str(), body) {
                                let result = run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &add_link,
                                        &add_lane,
                                        &mut join_lane_init,
                                        &mut command_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &lifecycle_item_ids,
                                    &mut dirty_items,
                                );
                                match result {
                                    Err(EventHandlerError::StopInstructed) => break Ok(()),
                                    Err(
                                        e @ (EventHandlerError::RuntimeError(_)
                                        | EventHandlerError::SteppedAfterComplete),
                                    ) => {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                    }
                                    Err(error) => {
                                        debug!(
                                            error = %error,
                                            "Incoming frame was rejected by the item."
                                        );
                                    }
                                    _ => check_cmds(
                                        &mut command_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            trace!(name = %name, remote_id = %remote_id, "Received a sync request for a map-like lane.");
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                exec_handler!(handler);
                            }
                        }
                        LaneRequest::InitComplete => {}
                    }
                }
                TaskEvent::HttpRequest { id, request } => {
                    let name = &external_item_ids_rev[&id];
                    trace!(name = %name, "Received an HTTP request for a lane.");
                    match item_model.on_http_request(name.as_str(), request) {
                        Ok(handler) => {
                            exec_handler!(handler);
                        }
                        Err(request) => not_found(name.as_str(), request),
                    }
                }
                TaskEvent::RequestError { id, error } => {
                    let lane = external_item_ids_rev[&id].clone();
                    break Err(AgentTaskError::BadFrame { lane, error });
                }
                TaskEvent::CommandSendComplete { result: Ok(writer) } => {
                    cmd_send_fut.set(None.into());
                    if !command_buffer.is_empty() {
                        let fut = writer.write(&mut command_buffer);
                        cmd_send_fut.set(Some(fut.fuse()).into());
                    } else {
                        cmd_writer = Some(writer);
                    }
                }
                TaskEvent::CommandSendComplete { result: Err(err) } => {
                    break Err(AgentTaskError::OutputFailed(err));
                }
            }
            // Attempt to write to the outgoing buffers for any items with data.
            dirty_items.retain(|id| {
                if let Some(mut tx) = item_writers.remove(id) {
                    let name = &external_item_ids_rev[id];
                    match item_model.write_event(name.as_str(), &mut tx.buffer) {
                        Some(WriteResult::Done) => {
                            pending_writes.push(do_write(tx, false));
                            false
                        }
                        Some(WriteResult::RequiresEvent) => {
                            pending_writes.push(do_write(tx, true));
                            false
                        }
                        Some(WriteResult::DataStillAvailable) => {
                            pending_writes.push(do_write(tx, false));
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
        let discard = (
            |_| {},
            |_| {
                Err(CommanderRegistrationError::RuntimeError(
                    AgentRuntimeError::Stopping,
                ))
            },
        );
        let add_lane = NoDynLanes;
        match run_handler(
            &mut ActionContext::new(
                &suspended,
                &discard,
                &add_lane,
                &mut join_lane_init,
                &mut command_buffer,
            ),
            meta,
            &item_model,
            &lifecycle,
            on_stop_handler,
            &lifecycle_item_ids,
            &mut Discard,
        ) {
            Ok(_) | Err(EventHandlerError::StopInstructed) => Ok(()),
            Err(e) => Err(AgentTaskError::UserCodeError(Box::new(e))),
        }
    }
}

struct NoDynLanes;

impl<Context> LaneSpawner<Context> for NoDynLanes {
    fn spawn_warp_lane(
        &self,
        _name: &str,
        _kind: WarpLaneKind,
        _on_done: LaneSpawnOnDone<Context>,
    ) -> Result<(), DynamicRegistrationError> {
        Err(DynamicRegistrationError::AfterInitialization)
    }
}

fn not_found(lane_name: &str, request: HttpLaneRequest) {
    let (_, response_tx) = request.into_parts();
    let payload = Bytes::from(format!(
        "This agent does not have an HTTP lane called `{}`",
        lane_name
    ));
    let content_len = Header::new(StandardHeaderName::ContentLength, payload.len().to_string());
    let response = RawHttpLaneResponse {
        status_code: StatusCode::NOT_FOUND,
        version: Version::HTTP_1_1,
        headers: vec![content_len],
        payload,
    };
    if response_tx.send(response).is_err() {
        debug!("HTTP request dropped before it was fulfilled.");
    }
}

async fn do_write(
    writer: ItemWriter,
    requires_event: bool,
) -> (ItemWriter, Result<bool, std::io::Error>) {
    let (writer, result) = writer.write().await;
    (writer, result.map(move |_| requires_event))
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
/// # Arguments
///
/// * `meta` - Agent instance metadata (which can be requested by the event handler).
/// * `context` - The context within which the event handler is running. This provides access to the lanes of the
///    agent (typically it will be an instance of a struct where the fields are lane instances).
/// * `lifecycle` - The agent lifecycle which provides event handlers for state changes for each lane.
/// * `handler` - The initial event handler that starts the chain. This could be a lifecycle event or triggered
///     by an incoming message from the runtime.
/// * `items` - Mapping between item IDs (returned by the handler to indicate that it has changed the state of
///    an item) and the item names (which are used by the lifecycle to identify the items).
/// * `collector` - Collects the IDs of lanes with state changes.
fn run_handler<Context, Lifecycle, Handler, Collector>(
    action_context: &mut ActionContext<Context>,
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
                    if modification.flags.contains(ModificationFlags::DIRTY) {
                        collector.add_id(modification.item_id);
                    }
                    if modification
                        .flags
                        .contains(ModificationFlags::TRIGGER_HANDLER)
                    {
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
                    if modification.flags.contains(ModificationFlags::DIRTY) {
                        collector.add_id(modification.item_id);
                    }
                    if modification
                        .flags
                        .contains(ModificationFlags::TRIGGER_HANDLER)
                    {
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

struct CommandWriter {
    tx: ByteWriter,
    buffer: BytesMut,
}

impl CommandWriter {
    fn new(tx: ByteWriter) -> Self {
        CommandWriter {
            tx,
            buffer: BytesMut::new(),
        }
    }

    fn write(
        mut self,
        content: &mut BytesMut,
    ) -> impl Future<Output = Result<Self, std::io::Error>> + 'static {
        self.buffer.clear();
        std::mem::swap(&mut self.buffer, content);
        async move {
            let CommandWriter { tx, buffer } = &mut self;
            tx.write_all(buffer).await?;
            Ok(self)
        }
    }
}

/// Check of an event handler wrote into the ad-hoc commands buffer and schedule a
/// write to the command channel.
#[inline]
fn check_cmds<Fut>(
    command_buffer: &mut BytesMut,
    cmd_writer: &mut Option<CommandWriter>,
    cmd_send_fut: &mut Pin<&mut OptionFuture<Fuse<Fut>>>,
    write: fn(CommandWriter, &mut BytesMut) -> Fut,
) where
    Fut: Future,
{
    if !command_buffer.is_empty() {
        if let Some(writer) = cmd_writer.take() {
            let fut = write(writer, command_buffer);
            cmd_send_fut.set(Some(fut.fuse()).into());
        }
    }
}

/// A request to the agent to open a new lane.
struct LaneSpawnRequest<Context> {
    name: String,
    kind: WarpLaneKind,
    on_done: LaneSpawnOnDone<Context>,
}

impl<Context> LaneSpawner<Context> for RefCell<Vec<LaneSpawnRequest<Context>>> {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<Context>,
    ) -> Result<(), DynamicRegistrationError> {
        self.borrow_mut().push(LaneSpawnRequest {
            name: name.to_string(),
            kind,
            on_done,
        });
        Ok(())
    }
}

/// A request to open a new downlink in the agent task.
struct DownlinkSpawnRequest<Context> {
    path: Address<Text>,
    kind: DownlinkKind,
    make_channel: BoxDownlinkChannelFactory<Context>,
    on_done: DownlinkSpawnOnDone<Context>,
}

impl<Context> std::fmt::Debug for DownlinkSpawnRequest<Context> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkSpawnRequest")
            .field("path", &self.path)
            .field("kind", &self.kind)
            .field("make_channel", &"...")
            .field("on_done", &"...")
            .finish()
    }
}

/// Unifies events for opening new downlinks, registering commanders and polling existing downlinks for activity.
enum LinksEvent<Context> {
    Opened {
        request: DownlinkSpawnRequest<Context>,
        result: OpenDownlinkResult,
    },
    Event {
        downlink: HostedDownlink<Context>,
        event: HostedDownlinkEvent,
    },
}

/// A future that either opens a new downlink, registers a commander or waits on activity from an open downlink.
#[pin_project(project = LinkFutureProj)]
enum LinkFuture<Context, F1, F2, F3> {
    Opening(Option<DownlinkSpawnRequest<Context>>, #[pin] F1),
    Running(#[pin] F2),
    Reconnecting(#[pin] F3),
}

impl<Context, F1, F2, F3> Future for LinkFuture<Context, F1, F2, F3>
where
    F1: Future<Output = OpenDownlinkResult>,
    F2: Future<Output = (HostedDownlink<Context>, HostedDownlinkEvent)>,
    F3: Future<Output = (HostedDownlink<Context>, HostedDownlinkEvent)>,
{
    type Output = LinksEvent<Context>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            LinkFutureProj::Opening(request, fut) => {
                let result = ready!(fut.poll(cx));
                Poll::Ready(LinksEvent::Opened {
                    request: request.take().expect("Polled twice."),
                    result,
                })
            }
            LinkFutureProj::Running(fut) => {
                let (dl, ev) = ready!(fut.poll(cx));
                Poll::Ready(LinksEvent::Event {
                    downlink: dl,
                    event: ev,
                })
            }
            LinkFutureProj::Reconnecting(fut) => {
                let (dl, ev) = ready!(fut.poll(cx));
                Poll::Ready(LinksEvent::Event {
                    downlink: dl,
                    event: ev,
                })
            }
        }
    }
}

/// The outcome of attempting to open a new downlink.
#[derive(Debug)]
enum OpenDownlinkResult {
    /// The downlink was opened successfully.
    Success(ByteWriter, ByteReader),
    /// Attempting to open the downlink failed with an error.
    Failed(DownlinkRuntimeError, RetryStrategy),
    /// The retry strategy used for retrying the downlink became exhausted.
    RetriesExpired(DownlinkRuntimeError),
}

fn open_new_downlink(
    agent_context: &dyn AgentContext,
    path: &Address<Text>,
    kind: DownlinkKind,
    retry: RetryStrategy,
    last_error: Option<DownlinkRuntimeError>,
) -> impl Future<Output = OpenDownlinkResult> + Send + 'static {
    let Address { host, node, lane } = path;
    let open_fut = agent_context.open_downlink(
        host.as_ref().map(AsRef::as_ref),
        node.as_ref(),
        lane.as_ref(),
        kind,
    );
    open_with_retry(open_fut, retry, last_error)
}

async fn open_with_retry(
    open_fut: BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>>,
    mut retry: RetryStrategy,
    last_error: Option<DownlinkRuntimeError>,
) -> OpenDownlinkResult {
    let delay = if let Some(error) = last_error {
        match retry.next() {
            Some(delay) => delay,
            None => return OpenDownlinkResult::RetriesExpired(error),
        }
    } else {
        None
    };
    if let Some(t) = delay {
        tokio::time::sleep(t).await;
    }
    match open_fut.await {
        Ok((tx, rx)) => OpenDownlinkResult::Success(tx, rx),
        Err(err) => OpenDownlinkResult::Failed(err, retry),
    }
}

struct LinkRequestCollector<Context> {
    downlinks: Vec<DownlinkSpawnRequest<Context>>,
    commander_ids: CommanderIds,
}

impl<Context> Default for LinkRequestCollector<Context> {
    fn default() -> Self {
        Self {
            downlinks: Default::default(),
            commander_ids: CommanderIds::default(),
        }
    }
}

enum SuspendResult<Context> {
    Handler(LocalBoxEventHandler<'static, Context>),
    TimedOut(u64),
}

#[pin_project(project = SuspendedProj)]
enum Suspended<Context> {
    Handler(HandlerFuture<Context>),
    Timeout {
        #[pin]
        at: Sleep,
        id: u64,
    },
}

impl<Context> From<HandlerFuture<Context>> for Suspended<Context> {
    fn from(value: HandlerFuture<Context>) -> Self {
        Suspended::Handler(value)
    }
}

impl<Context> Suspended<Context> {
    fn timeout(time: Instant, id: u64) -> Self {
        Suspended::Timeout {
            at: sleep_until(time),
            id,
        }
    }
}

impl<Context> std::fmt::Debug for LinkRequestCollector<Context> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkRequestCollector")
            .field("downlinks", &self.downlinks)
            .field("commander_ids", &self.commander_ids)
            .finish()
    }
}

impl<Context> LinkSpawner<Context> for RefCell<LinkRequestCollector<Context>> {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<Context>,
        on_done: DownlinkSpawnOnDone<Context>,
    ) {
        self.borrow_mut().downlinks.push(DownlinkSpawnRequest {
            path,
            kind: make_channel.kind(),
            make_channel,
            on_done,
        })
    }

    fn register_commander(&self, path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        let mut guard = self.borrow_mut();
        guard.commander_ids.get_request(&path)
    }
}

impl<Context, F1, F2> LinkSpawner<Context> for (F1, F2)
where
    F1: Fn(DownlinkSpawnRequest<Context>),
    F2: Fn(Address<Text>) -> Result<u16, CommanderRegistrationError>,
{
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<Context>,
        on_done: DownlinkSpawnOnDone<Context>,
    ) {
        (self.0)(DownlinkSpawnRequest {
            path,
            kind: make_channel.kind(),
            make_channel,
            on_done,
        })
    }

    fn register_commander(&self, path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        (self.1)(path)
    }
}

#[derive(Default, Debug)]
struct CommanderIds {
    next_id: u16,
    assigned: HashMap<Address<Text>, u16>,
}

impl CommanderIds {
    fn get_request(&mut self, address: &Address<Text>) -> Result<u16, CommanderRegistrationError> {
        let id = if let Some(id) = self.assigned.get(address) {
            *id
        } else {
            let id = self.next_id;
            if let Some(next) = id.checked_add(1) {
                self.next_id = next;
                self.assigned.insert(address.clone(), id);
                id
            } else {
                return Err(CommanderRegistrationError::CommanderIdOverflow);
            }
        };

        Ok(id)
    }
}

impl<Context> Future for Suspended<Context> {
    type Output = SuspendResult<Context>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            SuspendedProj::Handler(fut) => fut.poll_unpin(cx).map(SuspendResult::Handler),
            SuspendedProj::Timeout { at, id } => {
                at.poll(cx).map(move |_| SuspendResult::TimedOut(*id))
            }
        }
    }
}

impl<F, Context> Spawner<Context> for F
where
    F: Fn(Suspended<Context>),
{
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self(fut.into())
    }

    fn schedule_timer(&self, at: Instant, id: u64) {
        self(Suspended::timeout(at, id))
    }
}

impl<Context> Spawner<Context> for FuturesUnordered<Suspended<Context>> {
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self.push(fut.into());
    }

    fn schedule_timer(&self, at: Instant, id: u64) {
        self.push(Suspended::timeout(at, id))
    }
}
