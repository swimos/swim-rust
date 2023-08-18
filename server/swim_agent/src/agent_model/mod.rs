// Copyright 2015-2023 Swim Inc.
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
use std::pin::{pin, Pin};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::{Fuse, OptionFuture};
use futures::{
    future::{BoxFuture, Either, FusedFuture},
    stream::{FuturesUnordered, SelectAll},
    StreamExt,
};
use futures::{Future, FutureExt};
use swim_api::agent::{HttpLaneRequest, HttpLaneResponse, LaneConfig};
use swim_api::downlink::DownlinkKind;
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
use swim_model::address::Address;
use swim_model::http::{StatusCode, Version};
use swim_model::Text;
use swim_utilities::future::retryable::RetryStrategy;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::routing::route_uri::RouteUri;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::agent_lifecycle::item_event::ItemEvent;
use crate::agent_model::io::LaneReadEvent;
use crate::event_handler::{
    ActionContext, BoxEventHandler, BoxJoinValueInit, HandlerFuture, ModificationFlags,
};
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

use self::downlink::handlers::{BoxDownlinkChannel, DownlinkChannelError, DownlinkChannelEvent};
use self::init::{run_item_initializer, InitializedItem};
pub use init::{
    ItemInitializer, JoinValueInitializer, MapLaneInitializer, MapStoreInitializer,
    ValueLaneInitializer, ValueStoreInitializer,
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
    // The lane has written data to the buffer and needs it's event handler to be executed for
    // more to be available.
    RequiresEvent,
}

pub type InitFn<Agent> = Box<dyn FnOnce(&Agent) + Send + 'static>;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum ItemKind {
    Lane,
    Store,
}

bitflags! {

    #[derive(Default)]
    pub struct ItemFlags: u8 {
        /// The state of the item should not be persisted.
        const TRANSIENT = 0b01;
    }

}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemSpec {
    pub kind: ItemKind,
    pub flags: ItemFlags,
}

impl ItemSpec {
    pub fn new(kind: ItemKind, flags: ItemFlags) -> Self {
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

    /// The type of the handler to run when an HTTP request is received for a lane.
    type HttpRequestHandler: HandlerAction<Self, Completion = ()> + Send + 'static;

    /// The names and flags of all value like items (value lanes and stores, command lanes, etc) in the agent.
    fn value_like_item_specs() -> HashMap<&'static str, ItemSpec>;

    /// The names and flags of all map like items in the agent.
    fn map_like_item_specs() -> HashMap<&'static str, ItemSpec>;

    /// The names of all HTTP lanes in the agent.
    fn http_lane_names() -> HashSet<&'static str>;

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

    /// Create a handler that will update the state of the agent when an HTTP request is
    /// made to a lane. If not HTTP lane exists with the specified name the request will
    /// be returned as an error (so that the caller can handle it will a 404 response).
    ///
    /// #Arguments
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

pub trait LifecycleFac<ItemModel>: Send + Sync {
    type LifecycleType: AgentLifecycle<ItemModel> + Send;

    fn create(&self) -> Self::LifecycleType;
}

struct CloneableLifecycle<LC>(LC);

impl<ItemModel, LC> LifecycleFac<ItemModel> for CloneableLifecycle<LC>
where
    LC: Send + Sync + Clone + AgentLifecycle<ItemModel>,
{
    type LifecycleType = LC;

    fn create(&self) -> Self::LifecycleType {
        self.0.clone()
    }
}

struct FnLifecycleFac<F>(F);

impl<ItemModel, F, LC> LifecycleFac<ItemModel> for FnLifecycleFac<F>
where
    F: Fn() -> LC + Send + Sync,
    LC: Send + AgentLifecycle<ItemModel>,
{
    type LifecycleType = LC;

    fn create(&self) -> Self::LifecycleType {
        self.0()
    }
}

/// The complete model for an agent consisting of an implementation of [`AgentLaneModel`] to describe the lanes
/// of the agent and an implementation of [`AgentLifecycle`] to describe the lifecycle events that will trigger,
/// for  example, when the agent starts or stops or when the state of a lane changes.
pub struct AgentModel<ItemModel, Lifecycle> {
    item_model_fac: Arc<dyn ItemModelFactory<ItemModel = ItemModel>>,
    lifecycle_fac: Arc<dyn LifecycleFac<ItemModel, LifecycleType = Lifecycle>>,
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
        handler: BoxEventHandler<'static, ItemModel>,
    },
    DownlinkReady {
        downlink_event: (HostedDownlink<ItemModel>, HostedDownlinkEvent),
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
    ItemModel: AgentSpec + Send + 'static,
    Lifecycle: AgentLifecycle<ItemModel> + 'static,
{
    /// Initialize the agent, performing the initial setup for all of the lanes (including triggering the
    /// `on_start` event).
    ///
    /// #Arguments
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

        let mut value_like_lane_io = HashMap::new();
        let mut map_lane_io = HashMap::new();
        let mut value_like_store_io = HashMap::new();
        let mut map_store_io = HashMap::new();
        let mut http_lane_rxs = HashMap::new();

        let val_lane_specs = ItemModel::value_like_item_specs();
        let map_lane_specs = ItemModel::map_like_item_specs();
        let http_lane_names = ItemModel::http_lane_names();
        let item_ids = <ItemModel as AgentSpec>::item_ids();

        let suspended = FuturesUnordered::new();
        let downlink_channels = RefCell::new(vec![]);
        let mut join_value_init = HashMap::new();
        let mut ad_hoc_buffer = BytesMut::new();

        let item_model = item_model_fac.create();

        {
            let mut lane_init_tasks = FuturesUnordered::new();
            let default_lane_config = config.default_lane_config.unwrap_or_default();
            macro_rules! with_init {
                ($init:ident => $body:block) => {{
                    let mut $init = InitContext::new(
                        &mut value_like_lane_io,
                        &mut map_lane_io,
                        &item_model,
                        &lane_init_tasks,
                    );

                    $body
                }};
            }
            // Set up the lanes of the agent.
            for (name, spec) in val_lane_specs {
                match spec.kind {
                    ItemKind::Lane => {
                        let mut lane_conf = default_lane_config;
                        if spec.flags.contains(ItemFlags::TRANSIENT) {
                            lane_conf.transient = true;
                        }
                        let io = context.add_lane(name, LaneKind::Value, lane_conf).await?;
                        with_init!(init => {
                            init.init_value_lane(name, lane_conf, io);
                        })
                    }
                    ItemKind::Store => {
                        if !spec.flags.contains(ItemFlags::TRANSIENT) {
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
                }
            }
            for (name, spec) in map_lane_specs {
                match spec.kind {
                    ItemKind::Lane => {
                        if value_like_lane_io.contains_key(name) {
                            return Err(AgentInitError::DuplicateLane(Text::new(name)));
                        }
                        let mut lane_conf = default_lane_config;
                        if spec.flags.contains(ItemFlags::TRANSIENT) {
                            lane_conf.transient = true;
                        }
                        let io = context.add_lane(name, LaneKind::Map, lane_conf).await?;
                        with_init!(init => {
                            init.init_map_lane(name, lane_conf, io);
                        })
                    }
                    ItemKind::Store => {
                        if !spec.flags.contains(ItemFlags::TRANSIENT) {
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
                }
            }

            for name in http_lane_names {
                let channel = context.add_http_lane(name).await?;
                http_lane_rxs.insert(Text::new(name), channel);
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
                    (ItemKind::Lane, UplinkKind::Map) => {
                        map_lane_io.insert(Text::new(name), io);
                    }
                    //The receivers for stores are no longer needed as the runtime never sends messages after initialization.
                    (ItemKind::Store, UplinkKind::Value | UplinkKind::Supply) => {
                        let (tx, _) = io;
                        value_like_store_io.insert(Text::new(name), tx);
                    }
                    (ItemKind::Store, UplinkKind::Map) => {
                        let (tx, _) = io;
                        map_store_io.insert(Text::new(name), tx);
                    }
                };
            }
        }

        lifecycle.initialize(
            &mut ActionContext::new(
                &suspended,
                &*context,
                &downlink_channels,
                &mut join_value_init,
                &mut ad_hoc_buffer,
            ),
            meta,
            &item_model,
        );

        // Run the agent's `on_start` event handler.
        let on_start_handler = lifecycle.on_start();

        match run_handler(
            &mut ActionContext::new(
                &suspended,
                &*context,
                &downlink_channels,
                &mut join_value_init,
                &mut ad_hoc_buffer,
            ),
            meta,
            &item_model,
            &lifecycle,
            on_start_handler,
            &item_ids,
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
            item_ids,
            value_like_lane_io,
            value_like_store_io,
            map_lane_io,
            map_store_io,
            http_lane_rxs,
            suspended,
            downlink_channels: downlink_channels.into_inner(),
            ad_hoc_buffer,
            join_value_init,
        };
        Ok(agent_task.run_agent(context).boxed())
    }
}

type InitFut<'a, ItemModel> = BoxFuture<'a, Result<InitializedItem<'a, ItemModel>, FrameIoError>>;

struct InitContext<'a, 'b, ItemModel> {
    value_like_lane_io: &'a mut HashMap<Text, (ByteWriter, ByteReader)>,
    map_like_lane_io: &'a mut HashMap<Text, (ByteWriter, ByteReader)>,
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
        value_like_lane_io: &'a mut HashMap<Text, (ByteWriter, ByteReader)>,
        map_like_lane_io: &'a mut HashMap<Text, (ByteWriter, ByteReader)>,
        item_model: &'a ItemModel,
        item_init_tasks: &'a FuturesUnordered<InitFut<'b, ItemModel>>,
    ) -> Self {
        InitContext {
            value_like_lane_io,
            map_like_lane_io,
            item_model,
            item_init_tasks,
        }
    }

    fn init_value_lane(
        &mut self,
        name: &'b str,
        lane_conf: LaneConfig,
        io: (ByteWriter, ByteReader),
    ) {
        let InitContext {
            value_like_lane_io,
            item_model,
            item_init_tasks,
            ..
        } = self;
        if lane_conf.transient {
            value_like_lane_io.insert(Text::new(name), io);
        } else if let Some(init) = item_model.init_value_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Lane,
                name,
                UplinkKind::Value,
                io,
                WithLengthBytesCodec,
                init,
            );
            item_init_tasks.push(init_task.boxed());
        } else {
            value_like_lane_io.insert(Text::new(name), io);
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
                ItemKind::Store,
                name,
                UplinkKind::Value,
                io,
                WithLengthBytesCodec,
                init,
            );
            item_init_tasks.push(init_task.boxed());
        }
    }

    fn init_map_lane(
        &mut self,
        name: &'b str,
        lane_conf: LaneConfig,
        io: (ByteWriter, ByteReader),
    ) {
        let InitContext {
            map_like_lane_io,
            item_model,
            item_init_tasks,
            ..
        } = self;
        if lane_conf.transient {
            map_like_lane_io.insert(Text::new(name), io);
        } else if let Some(init) = item_model.init_map_like_item(name) {
            let init_task = run_item_initializer(
                ItemKind::Lane,
                name,
                UplinkKind::Map,
                io,
                MapMessageDecoder::new(RawMapOperationDecoder),
                init,
            );
            item_init_tasks.push(init_task.boxed());
        } else {
            map_like_lane_io.insert(Text::new(name), io);
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
                ItemKind::Store,
                name,
                UplinkKind::Map,
                io,
                MapMessageDecoder::new(RawMapOperationDecoder),
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
    item_ids: HashMap<u64, Text>,
    value_like_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    value_like_store_io: HashMap<Text, ByteWriter>,
    map_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    map_store_io: HashMap<Text, ByteWriter>,
    http_lane_rxs: HashMap<Text, mpsc::Receiver<HttpLaneRequest>>,
    suspended: FuturesUnordered<HandlerFuture<ItemModel>>,
    join_value_init: HashMap<u64, BoxJoinValueInit<'static, ItemModel>>,
    ad_hoc_buffer: BytesMut,
    downlink_channels: Vec<BoxDownlinkChannel<ItemModel>>,
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
            route_params,
            config,
            item_ids,
            value_like_lane_io,
            value_like_store_io,
            map_lane_io,
            map_store_io,
            http_lane_rxs,
            mut suspended,
            mut join_value_init,
            mut ad_hoc_buffer,
            downlink_channels,
        } = self;
        let meta = AgentMetadata::new(&route, &route_params, &config);

        let mut item_ids_rev = HashMap::new();
        for (id, name) in &item_ids {
            item_ids_rev.insert(name.clone(), *id);
        }

        let mut lane_readers = SelectAll::new();
        let mut item_writers = HashMap::new();
        let mut pending_writes = FuturesUnordered::new();
        let mut downlinks = FuturesUnordered::new();

        let mut cmd_writer = if let Ok(cmd_tx) = context.ad_hoc_commands().await {
            Some(CommandWriter::new(cmd_tx))
        } else {
            return Err(AgentTaskError::OutputFailed(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            )));
        };

        let mut cmd_send_fut = pin!(OptionFuture::from(None));

        // Start waiting on downlinks from the init phase.
        for channel in downlink_channels {
            let dl = HostedDownlink::new(channel);
            downlinks.push(Either::Left(dl.wait_on_downlink()));
        }

        // Set up readers and writers for each lane.
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

        for (name, rx) in http_lane_rxs {
            let id = item_ids_rev[&name];
            lane_readers.push(LaneReader::http(id, rx));
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
            let add_downlink = |channel| {
                let dl = HostedDownlink::new(channel);
                downlinks.push(Either::Left(dl.wait_on_downlink()));
                Ok(())
            };
            match task_event {
                TaskEvent::WriteComplete { writer, result } => {
                    match result {
                        Ok(true) => {
                            // The event handler for the item needs to be executed.
                            let lane = &item_ids[&writer.lane_id()];
                            if let Some(handler) = lifecycle.item_event(&item_model, lane.as_str())
                            {
                                match run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &*context,
                                        &add_downlink,
                                        &mut join_value_init,
                                        &mut ad_hoc_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    Err(EventHandlerError::StopInstructed) => break Ok(()),
                                    Err(e) => {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)))
                                    }
                                    Ok(_) => check_cmds(
                                        &mut ad_hoc_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        Err(_) => break Ok(()), //Failing to write indicates that the runtime has stopped so we can exit without an error.
                        _ => {}
                    }
                    item_writers.insert(writer.lane_id(), writer);
                }
                TaskEvent::SuspendedComplete { handler } => {
                    match run_handler(
                        &mut ActionContext::new(
                            &suspended,
                            &*context,
                            &add_downlink,
                            &mut join_value_init,
                            &mut ad_hoc_buffer,
                        ),
                        meta,
                        &item_model,
                        &lifecycle,
                        handler,
                        &item_ids,
                        &mut dirty_items,
                    ) {
                        Err(EventHandlerError::StopInstructed) => break Ok(()),
                        Err(e) => break Err(AgentTaskError::UserCodeError(Box::new(e))),
                        Ok(_) => check_cmds(
                            &mut ad_hoc_buffer,
                            &mut cmd_writer,
                            &mut cmd_send_fut,
                            CommandWriter::write,
                        ),
                    }
                }
                TaskEvent::DownlinkReady {
                    downlink_event: (mut downlink, event),
                } => match event {
                    HostedDownlinkEvent::Written => {
                        downlinks.push(Either::Left(downlink.wait_on_downlink()))
                    }
                    HostedDownlinkEvent::WriterFailed(err) => {
                        error!(error = %err, "A downlink hosted by the agent failed.");
                        debug!(address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect downlink.");
                        downlinks.push(Either::Right(downlink.reconnect(
                            &*context,
                            config.keep_linked_retry,
                            true,
                        )));
                    }
                    HostedDownlinkEvent::WriterTerminated => {
                        info!("A downlink hosted by the agent stopped writing output.");
                        downlinks.push(Either::Left(downlink.wait_on_downlink()));
                    }
                    HostedDownlinkEvent::HandlerReady { failed } => {
                        if let Some(handler) = downlink.channel.next_event(&item_model) {
                            match run_handler(
                                &mut ActionContext::new(
                                    &suspended,
                                    &*context,
                                    &add_downlink,
                                    &mut join_value_init,
                                    &mut ad_hoc_buffer,
                                ),
                                meta,
                                &item_model,
                                &lifecycle,
                                handler,
                                &item_ids,
                                &mut dirty_items,
                            ) {
                                Err(EventHandlerError::StopInstructed) => break Ok(()),
                                Err(e) => break Err(AgentTaskError::UserCodeError(Box::new(e))),
                                Ok(_) => check_cmds(
                                    &mut ad_hoc_buffer,
                                    &mut cmd_writer,
                                    &mut cmd_send_fut,
                                    CommandWriter::write,
                                ),
                            }
                        }
                        if failed {
                            error!("Reading from a downlink failed.");
                            debug!(address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect downlink.");
                            downlinks.push(Either::Right(downlink.reconnect(
                                &*context,
                                config.keep_linked_retry,
                                true,
                            )));
                        } else {
                            downlinks.push(Either::Left(downlink.wait_on_downlink()));
                        }
                    }
                    HostedDownlinkEvent::ReconnectSucceeded(reconnect) => {
                        reconnect.connect(&mut downlink, &item_model);
                        downlinks.push(Either::Left(downlink.wait_on_downlink()));
                    }
                    HostedDownlinkEvent::ReconnectFailed { error, retry } => {
                        error!(error = %error, address = %downlink.address(), kind = ?downlink.kind(), "Attempting to reconnect downlink failed.");
                        downlinks.push(Either::Right(downlink.reconnect(&*context, retry, false)));
                    }
                    HostedDownlinkEvent::Stopped => {
                        downlinks.push(Either::Right(downlink.reconnect(
                            &*context,
                            config.keep_linked_retry,
                            true,
                        )));
                    }
                    HostedDownlinkEvent::ReconnectNotPossible { retries_expired } => {
                        if retries_expired {
                            error!(address = %downlink.address(), kind = ?downlink.kind(), "A downlink stopped and could not be restarted.");
                        } else {
                            info!(address = %downlink.address(), kind = ?downlink.kind(), "A downlink stopped and was removed.");
                        }
                    }
                },
                TaskEvent::ValueRequest { id, request } => {
                    let name = &item_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = item_model.on_value_command(name.as_str(), body)
                            {
                                let result = run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &*context,
                                        &add_downlink,
                                        &mut join_value_init,
                                        &mut ad_hoc_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
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
                                        info!(
                                            error = %error,
                                            "Incoming frame was rejected by the item."
                                        );
                                    }
                                    _ => check_cmds(
                                        &mut ad_hoc_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                match run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &*context,
                                        &add_downlink,
                                        &mut join_value_init,
                                        &mut ad_hoc_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    Err(EventHandlerError::StopInstructed) => break Ok(()),
                                    Err(e) => {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)))
                                    }
                                    Ok(_) => check_cmds(
                                        &mut ad_hoc_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
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
                                let result = run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &*context,
                                        &add_downlink,
                                        &mut join_value_init,
                                        &mut ad_hoc_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
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
                                        info!(
                                            error = %error,
                                            "Incoming frame was rejected by the item."
                                        );
                                    }
                                    _ => check_cmds(
                                        &mut ad_hoc_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = item_model.on_sync(name.as_str(), remote_id) {
                                match run_handler(
                                    &mut ActionContext::new(
                                        &suspended,
                                        &*context,
                                        &add_downlink,
                                        &mut join_value_init,
                                        &mut ad_hoc_buffer,
                                    ),
                                    meta,
                                    &item_model,
                                    &lifecycle,
                                    handler,
                                    &item_ids,
                                    &mut dirty_items,
                                ) {
                                    Err(EventHandlerError::StopInstructed) => break Ok(()),
                                    Err(e) => {
                                        break Err(AgentTaskError::UserCodeError(Box::new(e)))
                                    }
                                    Ok(_) => check_cmds(
                                        &mut ad_hoc_buffer,
                                        &mut cmd_writer,
                                        &mut cmd_send_fut,
                                        CommandWriter::write,
                                    ),
                                }
                            }
                        }
                        LaneRequest::InitComplete => {}
                    }
                }
                TaskEvent::HttpRequest { id, request } => {
                    let name = &item_ids[&id];
                    match item_model.on_http_request(name.as_str(), request) {
                        Ok(handler) => {
                            match run_handler(
                                &mut ActionContext::new(
                                    &suspended,
                                    &*context,
                                    &add_downlink,
                                    &mut join_value_init,
                                    &mut ad_hoc_buffer,
                                ),
                                meta,
                                &item_model,
                                &lifecycle,
                                handler,
                                &item_ids,
                                &mut dirty_items,
                            ) {
                                Err(EventHandlerError::StopInstructed) => break Ok(()),
                                Err(e) => break Err(AgentTaskError::UserCodeError(Box::new(e))),
                                Ok(_) => check_cmds(
                                    &mut ad_hoc_buffer,
                                    &mut cmd_writer,
                                    &mut cmd_send_fut,
                                    CommandWriter::write,
                                ),
                            }
                        }
                        Err(request) => not_found(name.as_str(), request),
                    }
                }
                TaskEvent::RequestError { id, error } => {
                    let lane = item_ids[&id].clone();
                    break Err(AgentTaskError::BadFrame { lane, error });
                }
                TaskEvent::CommandSendComplete { result: Ok(writer) } => {
                    cmd_send_fut.set(None.into());
                    if !ad_hoc_buffer.is_empty() {
                        let fut = writer.write(&mut ad_hoc_buffer);
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
                    let name = &item_ids[id];
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
        let discard = |_| {
            Err(DownlinkRuntimeError::RuntimeError(
                AgentRuntimeError::Stopping,
            ))
        };
        match run_handler(
            &mut ActionContext::new(
                &suspended,
                &*context,
                &discard,
                &mut join_value_init,
                &mut ad_hoc_buffer,
            ),
            meta,
            &item_model,
            &lifecycle,
            on_stop_handler,
            &item_ids,
            &mut Discard,
        ) {
            Ok(_) | Err(EventHandlerError::StopInstructed) => Ok(()),
            Err(e) => Err(AgentTaskError::UserCodeError(Box::new(e))),
        }
    }
}

fn not_found(lane_name: &str, request: HttpLaneRequest) {
    let HttpLaneRequest { response_tx, .. } = request;
    let response = HttpLaneResponse {
        status_code: StatusCode::NOT_FOUND,
        version: Version::HTTP_1_1,
        headers: vec![],
        payload: Bytes::from(format!(
            "This agent does not have an HTTP lane called `{}`",
            lane_name
        )),
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
    ad_hoc_buffer: &mut BytesMut,
    cmd_writer: &mut Option<CommandWriter>,
    cmd_send_fut: &mut Pin<&mut OptionFuture<Fuse<Fut>>>,
    write: fn(CommandWriter, &mut BytesMut) -> Fut,
) where
    Fut: Future,
{
    if !ad_hoc_buffer.is_empty() {
        if let Some(writer) = cmd_writer.take() {
            let fut = write(writer, ad_hoc_buffer);
            cmd_send_fut.set(Some(fut.fuse()).into());
        }
    }
}
