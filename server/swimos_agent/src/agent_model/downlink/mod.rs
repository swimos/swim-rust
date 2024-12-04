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

mod hosted;
#[cfg(test)]
mod tests;

use std::any::type_name;
use std::fmt::Formatter;
use std::{cell::RefCell, marker::PhantomData};

use futures::future::BoxFuture;
use std::hash::Hash;
use swimos_agent_protocol::MapOperation;
use swimos_api::{address::Address, agent::DownlinkKind};
use swimos_form::{read::RecognizerReadable, Form};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::{circular_buffer, trigger};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::error;

use crate::event_handler::LocalBoxEventHandler;
use crate::map_storage::MapOpsWithEntry;
use crate::{
    config::{MapDownlinkConfig, SimpleDownlinkConfig},
    downlink_lifecycle::{EventDownlinkLifecycle, MapDownlinkLifecycle, ValueDownlinkLifecycle},
    event_handler::{ActionContext, HandlerAction, StepResult, UnitHandler},
    meta::AgentMetadata,
};

use self::hosted::{EventDownlinkFactory, MapDownlinkFactory, ValueDownlinkFactory};
pub use self::hosted::{EventDownlinkHandle, MapDownlinkHandle, ValueDownlinkHandle};

struct Inner<LC> {
    address: Address<Text>,
    lifecycle: LC,
}

/// [`HandlerAction`] that attempts to open a value downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenValueDownlinkAction<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: SimpleDownlinkConfig,
}

/// [`HandlerAction`] that attempts to open an event downlink to a remote lane.
pub struct OpenEventDownlinkAction<T, LC> {
    _type: PhantomData<fn(T) -> T>,
    inner: Option<Inner<LC>>,
    config: SimpleDownlinkConfig,
    map_events: bool,
}

type KvInvariant<K, V, M> = fn(K, V) -> M;

/// [`HandlerAction`] that attempts to open a map downlink to a remote lane and results in
/// a handle to the downlink.
pub struct OpenMapDownlinkAction<K, V, M, LC> {
    _type: PhantomData<KvInvariant<K, V, M>>,
    inner: Option<Inner<LC>>,
    config: MapDownlinkConfig,
}

impl<T, LC> OpenValueDownlinkAction<T, LC> {
    pub fn new(address: Address<Text>, lifecycle: LC, config: SimpleDownlinkConfig) -> Self {
        OpenValueDownlinkAction {
            _type: PhantomData,
            inner: Some(Inner { address, lifecycle }),
            config,
        }
    }
}

impl<T, LC> OpenEventDownlinkAction<T, LC> {
    /// # Arguments
    /// * `address` - The address of the remote lane to link to.
    /// * `lifecycle` - The lifecycle events associated with the downlink.
    /// * `config` - Runtime configuration parameters for the downlink.
    /// * `map_events` - Determines whether the downlink will report [`DownlinkKind::Event`] or
    ///   [`DownlinkKind::MapEvent`] as its kind (and makes no functional difference).
    pub fn new(
        address: Address<Text>,
        lifecycle: LC,
        config: SimpleDownlinkConfig,
        map_events: bool,
    ) -> Self {
        OpenEventDownlinkAction {
            _type: PhantomData,
            inner: Some(Inner { address, lifecycle }),
            config,
            map_events,
        }
    }
}

impl<K, V, M, LC> OpenMapDownlinkAction<K, V, M, LC> {
    pub fn new(address: Address<Text>, lifecycle: LC, config: MapDownlinkConfig) -> Self {
        OpenMapDownlinkAction {
            _type: PhantomData,
            inner: Some(Inner { address, lifecycle }),
            config,
        }
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenValueDownlinkAction<T, LC>
where
    Context: 'static,
    T: Form + Send + 'static,
    LC: ValueDownlinkLifecycle<T, Context> + Send + 'static,
    T::Rec: Send,
{
    type Completion = ValueDownlinkHandle<T>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenValueDownlinkAction { inner, config, .. } = self;
        if let Some(Inner {
            address: path,
            lifecycle,
        }) = inner.take()
        {
            let state: RefCell<Option<T>> = Default::default();
            let (tx, rx) = circular_buffer::watch_channel();
            let (stop_tx, stop_rx) = trigger::trigger();

            let config = *config;

            let fac =
                ValueDownlinkFactory::new(path.clone(), lifecycle, state, config, stop_rx, rx);
            let handle = ValueDownlinkHandle::new(path.clone(), tx, stop_tx, fac.dl_state());

            action_context.start_downlink(path, fac, |result| {
                if let Err(err) = result {
                    error!(error = %err, "Registering value downlink failed.");
                }
                UnitHandler::default()
            });

            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let OpenValueDownlinkAction { inner, config, .. } = self;
        if let Some(Inner { address, .. }) = inner {
            f.debug_struct("OpenValueDownlinkAction")
                .field("config", config)
                .field("address", address)
                .field("consumed", &false)
                .field("type", &type_name::<T>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        } else {
            f.debug_struct("OpenValueDownlinkAction")
                .field("config", config)
                .field("consumed", &true)
                .field("type", &type_name::<T>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        }
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        self.inner.is_some()
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, _context: &Context, mut hasher: &mut dyn std::hash::Hasher) {
        use std::{any::TypeId, hash::Hash};

        if let Some(Inner { address, .. }) = &self.inner {
            TypeId::of::<OpenValueDownlinkAction<(), ()>>().hash(&mut hasher);
            address.hash(&mut hasher);
        }
    }
}

impl<T, LC, Context> HandlerAction<Context> for OpenEventDownlinkAction<T, LC>
where
    Context: 'static,
    T: RecognizerReadable + Send + 'static,
    LC: EventDownlinkLifecycle<T, Context> + Send + 'static,
    T::Rec: Send,
{
    type Completion = EventDownlinkHandle;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenEventDownlinkAction {
            inner,
            config,
            map_events,
            ..
        } = self;
        if let Some(Inner { address, lifecycle }) = inner.take() {
            let config = *config;
            let (stop_tx, stop_rx) = trigger::trigger();

            let fac =
                EventDownlinkFactory::new(address.clone(), lifecycle, config, stop_rx, *map_events);
            let handle = EventDownlinkHandle::new(address.clone(), stop_tx, fac.dl_state());
            action_context.start_downlink(address, fac, |result| {
                if let Err(err) = result {
                    error!(error = %err, "Registering event downlink failed.");
                }
                UnitHandler::default()
            });
            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let OpenEventDownlinkAction { inner, config, .. } = self;
        if let Some(Inner { address, .. }) = inner {
            f.debug_struct("OpenEventDownlinkAction")
                .field("config", config)
                .field("address", address)
                .field("consumed", &false)
                .field("type", &type_name::<T>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        } else {
            f.debug_struct("OpenEventDownlinkAction")
                .field("config", config)
                .field("consumed", &true)
                .field("type", &type_name::<T>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        }
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        self.inner.is_some()
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, _context: &Context, mut hasher: &mut dyn std::hash::Hasher) {
        use std::{any::TypeId, hash::Hash};

        if let Some(Inner { address, .. }) = &self.inner {
            TypeId::of::<OpenEventDownlinkAction<(), ()>>().hash(&mut hasher);
            address.hash(&mut hasher);
        }
    }
}

impl<K, V, M, LC, Context> HandlerAction<Context> for OpenMapDownlinkAction<K, V, M, LC>
where
    Context: 'static,
    K: Form + Hash + Eq + Clone + Send + Sync + 'static,
    V: Form + Send + Sync + 'static,
    LC: MapDownlinkLifecycle<K, V, M, Context> + Send + 'static,
    K::Rec: Send,
    V::Rec: Send,
    M: Default + MapOpsWithEntry<K, V, K> + Send + 'static,
{
    type Completion = MapDownlinkHandle<K, V>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenMapDownlinkAction { inner, config, .. } = self;
        if let Some(Inner { address, lifecycle }) = inner.take() {
            let (tx, rx) = mpsc::unbounded_channel::<MapOperation<K, V>>();
            let (stop_tx, stop_rx) = trigger::trigger();
            let config = *config;
            let fac = MapDownlinkFactory::new(address.clone(), lifecycle, config, stop_rx, rx);
            let handle = MapDownlinkHandle::new(address.clone(), tx, stop_tx, fac.dl_state());

            action_context.start_downlink(address, fac, |result| {
                if let Err(err) = result {
                    error!(error = %err, "Registering map downlink failed.");
                }
                UnitHandler::default()
            });

            StepResult::done(handle)
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        _context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let OpenMapDownlinkAction { inner, config, .. } = self;
        if let Some(Inner { address, .. }) = inner {
            f.debug_struct("OpenMapDownlinkAction")
                .field("config", config)
                .field("address", address)
                .field("consumed", &false)
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        } else {
            f.debug_struct("OpenMapDownlinkAction")
                .field("config", config)
                .field("consumed", &true)
                .field("key_type", &type_name::<K>())
                .field("value_type", &type_name::<V>())
                .field("lifecycle_type", &type_name::<LC>())
                .finish()
        }
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        self.inner.is_some()
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, _context: &Context, mut hasher: &mut dyn std::hash::Hasher) {
        use std::{any::TypeId, hash::Hash};

        if let Some(Inner { address, .. }) = &self.inner {
            TypeId::of::<OpenMapDownlinkAction<(), (), (), ()>>().hash(&mut hasher);
            address.hash(&mut hasher);
        }
    }
}

/// Indication that the downlink task has completed some unit of work.
#[derive(Debug, PartialEq, Eq)]
pub enum DownlinkChannelEvent {
    /// A change has occurred to the state of the downlink (either an event or a change to
    /// the linked/synced/unlinked state). This indicates that the consumer should attempt
    /// to call `next_event` to consume the generated event handler. These two steps are
    /// necessarily separate to avoid holding a borrow across an await point so that the task
    /// remains [`Send`].
    HandlerReady,
    /// Indicates that the downlink has written a message to its output.
    WriteCompleted,
    /// Indicates that the write half of the downlink has terminated (this will happen immediately
    /// for downlinks that never send anything.)
    WriteStreamTerminated,
}

/// Indication that an error occurred while polling a downlink task.
#[derive(Debug, Error)]
pub enum DownlinkChannelError {
    /// Reading from the downlink failed.
    #[error("Reading from the downlink input failed.")]
    ReadFailed,
    /// Attempting to write to the downlink failed.
    #[error("Writing to the downlink output failed: {0}")]
    WriteFailed(#[from] std::io::Error),
}

/// Allows a downlink to be driven by an agent task, without any knowledge of the types used
/// internally by the downlink. A [`DownlinkChannel`] is essentially a stream of event handlers.
/// However, the operation to get the next handler is split across two methods (one to wait for
/// an incoming message and the other to create an event handler, where appropriate). This split
/// is necessary to avoid holding a reference to the downlink lifecycle across an await point.
/// Otherwise, we would need to add a bound that the lifecycles must be [`Sync`] which
/// is undesirable as it would prevent the use of [`std::cell::RefCell`] in lifecycle fields.
///
/// #Type Arguments
///
/// * `Context` - The context within which the event handlers must be run (typically an agent type).
pub trait DownlinkChannel<Context> {
    /// Attach the downlink to its input and output streams. This can be called multiple times
    /// if the downlink restarts.
    fn connect(&mut self, context: &Context, output: ByteWriter, input: ByteReader);

    /// Whether the downlink can be restarted.
    fn can_restart(&self) -> bool;

    /// The address to which the downlink is connected.
    fn address(&self) -> &Address<Text>;

    /// Get the kind of the downlink.
    fn kind(&self) -> DownlinkKind;

    /// Await the next channel event. If this returns [`None`], the downlink has terminated. If an
    /// error is returned, the downlink has failed and should not longer be waited on.
    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>>;

    /// After a call to `await_ready` that does not return [`None`] this may return an event handler to
    /// be executed by the agent task. At any other time, it will return [`None`].
    fn next_event(&mut self, context: &Context) -> Option<LocalBoxEventHandler<'_, Context>>;

    /// Flush any pending outputs.
    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>>;
}

/// A downlink channel that can be used by dynamic dispatch. As the agent task cannot know about the
/// specific type of any opened downlinks, it views them through this interface.
pub type BoxDownlinkChannel<Context> = Box<dyn DownlinkChannel<Context> + Send>;

/// A downlink channel factory creates a particular type of downlink (value, map, etc.) attached to
/// the IO channels that are provided.
pub trait DownlinkChannelFactory<Context> {
    /// Create a new new downlink.
    ///
    /// # Arguments
    /// * `context` - The agent context to which the downlink will belong.
    /// * `tx` - The output channel for the downlink (this may be dropped).
    /// * `rx` - The input channel for the downlink.
    fn create(
        self,
        context: &Context,
        tx: ByteWriter,
        rx: ByteReader,
    ) -> BoxDownlinkChannel<Context>;

    /// The kind of the downlink that is created.
    fn kind(&self) -> DownlinkKind;

    /// Create a new downlink from a boxed instances of the factory.
    ///
    /// # Arguments
    /// * `context` - The agent context to which the downlink will belong.
    /// * `tx` - The output channel for the downlink (this may be dropped).
    /// * `rx` - The input channel for the downlink.
    fn create_box(
        self: Box<Self>,
        context: &Context,
        tx: ByteWriter,
        rx: ByteReader,
    ) -> BoxDownlinkChannel<Context>;
}

/// An opaque downlink channel factory that can be called by dynamic dispatch.
pub type BoxDownlinkChannelFactory<Context> =
    Box<dyn DownlinkChannelFactory<Context> + Send + 'static>;
