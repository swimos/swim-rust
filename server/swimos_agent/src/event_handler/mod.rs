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

use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
};

use bytes::BytesMut;
use frunk::{coproduct::CNil, Coproduct};
use static_assertions::assert_obj_safe;
use swimos_agent_protocol::{encoding::command::CommandMessageEncoder, CommandMessage};
use swimos_api::{
    address::Address,
    agent::WarpLaneKind,
    error::{
        AgentRuntimeError, CommanderRegistrationError, DownlinkRuntimeError,
        DynamicRegistrationError, LaneSpawnError,
    },
};
use swimos_form::{read::RecognizerReadable, write::StructuralWritable};
use swimos_model::Text;
use swimos_recon::parser::AsyncParseError;
use swimos_utilities::{never::Never, routing::RouteUri};
use thiserror::Error;
use tokio::time::Instant;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    agent_model::downlink::{
        BoxDownlinkChannelFactory, DownlinkChannelFactory, MapDownlinkHandle, ValueDownlinkHandle,
    },
    lanes::JoinLaneKind,
    meta::AgentMetadata,
    ReconDecoder,
};

use bitflags::bitflags;

pub use futures::future::Either;

#[cfg(test)]
pub(crate) mod check_step;
mod command;
mod handler_fn;
mod suspend;
#[cfg(test)]
mod tests;
mod try_handler;

pub use suspend::{run_after, run_schedule, run_schedule_async, HandlerFuture, Spawner, Suspend};
pub use try_handler::{TryHandler, TryHandlerAction, TryHandlerActionExt};

pub use command::SendCommand;
#[doc(hidden)]
pub use handler_fn::{
    CueFn0, CueFn1, EventConsumeFn, EventFn, GetFn, HandlerFn0, MapRemoveFn, MapUpdateBorrowFn,
    MapUpdateFn, RequestFn0, RequestFn1, TakeFn, UpdateBorrowFn, UpdateFn,
};

/// Instances of this trait allow [`HandlerAction`]s to request new downlinks and commanders to be opened in the agent.
pub trait LinkSpawner<Context> {
    /// Request a new downlink be opened in the agent task.
    ///
    /// # Arguments
    /// * `path` - The address of the lane to open a downlink to.
    /// * `make_channel` - Factory to create the downlink channel.
    /// * `on_done` - A callback that will create an event handler to be run when the connection completes (or fails).
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<Context>,
        on_done: DownlinkSpawnOnDone<Context>,
    );

    fn register_commander(&self, path: Address<Text>) -> Result<u16, CommanderRegistrationError>;
}

type SpawnHandler<Context> = BoxHandlerAction<'static, Context, ()>;

#[doc(hidden)]
pub type LaneSpawnOnDone<Context> =
    Box<dyn FnOnce(Result<u64, LaneSpawnError>) -> SpawnHandler<Context> + Send + 'static>;

#[doc(hidden)]
pub type DownlinkSpawnOnDone<Context> =
    Box<dyn FnOnce(Result<(), DownlinkRuntimeError>) -> SpawnHandler<Context> + Send + 'static>;

#[doc(hidden)]
pub type CommanderSpawnOnDone<Context> =
    Box<dyn FnOnce(u16) -> SpawnHandler<Context> + Send + 'static>;

/// Trait for contexts that can spawn a new lane into the agent task.
pub trait LaneSpawner<Context> {
    /// Spawn a new WARP lane into the agent task.
    ///
    /// # Arguments
    /// * `io` - IO channels, for the lane, connected to the runtime.
    /// * `kind` - The kind of the lane.
    /// * `on_done` - A callback that produces an event handler that will be executed after the lane is registered.
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<Context>,
    ) -> Result<(), DynamicRegistrationError>;
}

/// The context type passed to every call to [`HandlerAction::step`] that provides access to the
/// underlying. Some of the methods on this type are not intended for use in user supplied handler
/// implementations and so can only be used from this crate.
pub struct ActionContext<'a, Context> {
    spawner: &'a dyn Spawner<Context>,
    downlink: &'a dyn LinkSpawner<Context>,
    lanes: &'a dyn LaneSpawner<Context>,
    join_lane_init: &'a mut HashMap<u64, BoxJoinLaneInit<'static, Context>>,
    command_buffer: &'a mut BytesMut,
}

impl<'a, Context> Spawner<Context> for ActionContext<'a, Context> {
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self.spawner.spawn_suspend(fut);
    }

    fn schedule_timer(&self, at: Instant, id: u64) {
        self.spawner.schedule_timer(at, id);
    }
}

impl<'a, Context> ActionContext<'a, Context> {
    pub fn new(
        spawner: &'a dyn Spawner<Context>,
        downlink: &'a dyn LinkSpawner<Context>,
        lanes: &'a dyn LaneSpawner<Context>,
        join_lane_init: &'a mut HashMap<u64, BoxJoinLaneInit<'static, Context>>,
        command_buffer: &'a mut BytesMut,
    ) -> Self {
        ActionContext {
            spawner,
            downlink,
            lanes,
            join_lane_init,
            command_buffer,
        }
    }

    /// Get any join lane initializer that was registered using [`Self::register_join_lane_initializer`]. Typically,
    /// a join lane initializer will be during the `on_init` event of the agent and then retrieved each time a new
    /// downlink is opened for the lane.
    ///
    /// # Arguments
    /// * `lane_id` - The internal unique ID of the lane.
    #[doc(hidden)]
    pub(crate) fn join_lane_initializer(
        &self,
        lane_id: u64,
    ) -> Option<&BoxJoinLaneInit<'static, Context>> {
        self.join_lane_init.get(&lane_id)
    }

    /// Register a join lane initializer that can be retrieved later using the [`Self::join_lane_initializer`] method.
    #[doc(hidden)]
    pub(crate) fn register_join_lane_initializer(
        &mut self,
        lane_id: u64,
        factory: BoxJoinLaneInit<'static, Context>,
    ) {
        self.join_lane_init.insert(lane_id, factory);
    }

    /// Request that the runtime open a downlink the the specified remote lane.
    ///
    /// # Arguments
    /// * `path` - The address of the remote lane.
    /// * `make_channel` - A closure that will create the task that will run within the agent runtime to handle the
    /// downlink lifecycle.
    /// * `on_done` - A callback that will be executed when the downlink has started (or failed to start).
    #[doc(hidden)]
    pub(crate) fn start_downlink<F, OnDone, H>(
        &self,
        path: Address<Text>,
        make_channel: F,
        on_done: OnDone,
    ) where
        Context: 'static,
        F: DownlinkChannelFactory<Context> + Send + 'static,
        OnDone: FnOnce(Result<(), DownlinkRuntimeError>) -> H + Send + 'static,
        H: EventHandler<Context> + Send + 'static,
    {
        let on_done_boxed = move |result| {
            let event_handler: SpawnHandler<Context> = Box::new(on_done(result));
            event_handler
        };
        let handler: DownlinkSpawnOnDone<Context> = Box::new(on_done_boxed);
        self.downlink
            .spawn_downlink(path, Box::new(make_channel), handler)
    }

    pub(crate) fn register_commander(
        &mut self,
        path: Address<Text>,
    ) -> Result<u16, CommanderRegistrationError> {
        let id = self.downlink.register_commander(path.clone())?;
        let msg = CommandMessage::<_, String>::register(path, id);
        let mut encoder = CommandMessageEncoder::default();
        encoder
            .encode(msg, self.command_buffer)
            .expect("Encoding should be infallible.");
        Ok(id)
    }

    /// Attempt to attach a new lane to the agent runtime.
    ///
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `kind` - The kind of the lane.
    /// * `on_opened` - Callback providing an event handler to be executed by the agent when the request
    /// has completed.
    #[doc(hidden)]
    pub(crate) fn open_lane<F, H>(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_opened: F,
    ) -> Result<(), DynamicRegistrationError>
    where
        F: FnOnce(Result<(), LaneSpawnError>) -> H + Send + 'static,
        H: EventHandler<Context> + Send + 'static,
    {
        let f = move |result| wrap(on_opened, result);
        self.lanes.spawn_warp_lane(name, kind, Box::new(f))
    }

    /// Send a command message to a remote lane.
    ///
    /// # Arguments
    /// * `target` - Address of the target endpoint.
    /// * `command` - The body of the command message.
    /// * `overwrite_permitted` - Configures back-pressure relief for this message. If true, and the messages has not
    /// been sent before another message is send, it will be overwritten and never sent.
    #[doc(hidden)]
    pub(crate) fn send_ad_hoc_command<S, T>(
        &mut self,
        target: Address<S>,
        command: T,
        overwrite_permitted: bool,
    ) where
        S: AsRef<str>,
        T: StructuralWritable,
    {
        let ActionContext { command_buffer, .. } = self;
        let mut encoder = CommandMessageEncoder::default();
        let cmd = CommandMessage::ad_hoc(target, command, overwrite_permitted);
        encoder
            .encode(cmd, command_buffer)
            .expect("Encoding should be infallible.")
    }

    /// Send a command message to a remote lane.
    ///
    /// # Arguments
    /// * `target` - Registered ID of the remote lane endpoint.
    /// * `command` - The body of the command message.
    /// * `overwrite_permitted` - Configures back-pressure relief for this message. If true, and the messages has not
    /// been sent before another message is send, it will be overwritten and never sent.
    #[doc(hidden)]
    pub(crate) fn send_registered_command<T>(
        &mut self,
        target: u16,
        command: T,
        overwrite_permitted: bool,
    ) where
        T: StructuralWritable,
    {
        let ActionContext { command_buffer, .. } = self;
        let mut encoder = CommandMessageEncoder::default();
        let cmd = CommandMessage::<&str, T>::registered(target, command, overwrite_permitted);
        encoder
            .encode(cmd, command_buffer)
            .expect("Encoding should be infallible.")
    }
}

fn wrap<Context, F, H>(f: F, result: Result<u64, LaneSpawnError>) -> SpawnHandler<Context>
where
    F: FnOnce(Result<(), LaneSpawnError>) -> H + Send + 'static,
    H: EventHandler<Context> + Send + 'static,
{
    Box::new(f(result.map(|_| ())))
}

/// Trait to describe an action to be taken, within the context of an agent, when an event occurs. The
/// execution of an event handler can be suspended (so that it can trigger the execution of other handlers).
/// This could be expressed using generators from the standard library after this feature is stabilized.
/// A handler instance can be used exactly once. After it has returned a result or an error all subsequent
/// step executions must result in an error.
///
/// It should not generally be necessary to implement this trait in user code.
///
/// # Type Parameters
/// * `Context` - The context within which the handler executes. Typically, this will be a struct type where
///    each field is a lane of an agent.
pub trait HandlerAction<Context> {
    /// The result of executing the handler to completion.
    type Completion;

    /// Run one step of the handler. This can result in either the handler suspending execution, completing
    /// with a result or returning an error.
    ///
    /// # Arguments
    /// * `suspend` - Allows for futures to be suspended into the agent task. The future will result in another event handler
    ///    which will be executed by the agent task upon completion.
    /// * `meta` - Provides access to agent instance metadata.
    /// * `context` - The execution context of the handler (providing access to the lanes of the agent).
    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion>;

    /// Write a debug formatted description of the handler, potentially including information from
    /// the context (such as the names of lanes etc.).
    ///
    /// # Arguments
    ///
    /// * `context` - The execution context of the handler (providing access to the lanes of the agent).
    /// * `f` - Formatter to be written to.
    #[allow(unused_variables)]
    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("OpaqueHandler").finish()
    }
}

/// A [`HandlerAction`] that does not produce a result.
pub trait EventHandler<Context>: HandlerAction<Context, Completion = ()> {}

assert_obj_safe!(EventHandler<()>);

/// A [`HandlerAction`] that is called by dynamic dispatch.
pub type LocalBoxHandlerAction<'a, Context, T> =
    Box<dyn HandlerAction<Context, Completion = T> + 'a>;
///  An [event handler](crate::event_handler::EventHandler) that is called by dynamic dispatch.
pub type LocalBoxEventHandler<'a, Context> = LocalBoxHandlerAction<'a, Context, ()>;

/// A [`HandlerAction`] that is called by dynamic dispatch and has a `Send` bound.
pub type BoxHandlerAction<'a, Context, T> =
    Box<dyn HandlerAction<Context, Completion = T> + Send + 'a>;
///  An [event handler](crate::event_handler::EventHandler) that is called by dynamic dispatch and
/// has a `Send` bound.
pub type BoxEventHandler<'a, Context> = BoxHandlerAction<'a, Context, ()>;

impl<Context, H> EventHandler<Context> for H where H: HandlerAction<Context, Completion = ()> {}

impl<'a, H, Context> HandlerAction<Context> for &'a mut H
where
    H: HandlerAction<Context>,
{
    type Completion = H::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        (**self).step(action_context, meta, context)
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        (**self).describe(context, f)
    }
}

impl<H: ?Sized, Context> HandlerAction<Context> for Box<H>
where
    H: HandlerAction<Context>,
{
    type Completion = H::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        (**self).step(action_context, meta, context)
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        (**self).describe(context, f)
    }
}

/// Error type for fallible [`HandlerAction`]s. A handler produces an error when a fatal problem occurs and it
/// cannot produce its result. In most cases this will result in the agent terminating.
#[derive(Debug, Error)]
pub enum EventHandlerError {
    /// Handlers can only be used once. If a handler is stepped after it produces its value, this error will be raised.
    #[error("Event handler stepped after completion.")]
    SteppedAfterComplete,
    /// An incoming command message was invalid for the lane it was targetting.
    #[error("Invalid incoming message: {0}")]
    BadCommand(AsyncParseError),
    /// An incoming command message was incomplete and could not be deserialized.
    #[error("An incoming message was incomplete.")]
    IncompleteCommand,
    /// An error occurred in the agent runtime which prevented this handler from producing its result.
    #[error("An error occurred in the agent runtime.")]
    RuntimeError(#[from] AgentRuntimeError),
    /// A handler requested a join lane lifecycle with different type parameters than were used to register it.
    #[error("Invalid key or value type for a join lane lifecycle.")]
    BadJoinLifecycle(DowncastError),
    /// The `on_cue` lifecycle handler is mandatory for demand lanes. If it is not defined this error will be raised.
    #[error("The cue operation for a demand lane was undefined.")]
    DemandCueUndefined,
    /// If a GET request is made to a HTTP lane but it does not handle it, this error is raised. (This will not
    /// terminate the agent but will cause an error HTTP response to be sent).
    #[error("No GET handler was defined for an HTTP lane.")]
    HttpGetUndefined,
    /// An executing handler attempted to target a lane that does not exist.
    #[error("A command was received for a lane that does not exist: '{0}'")]
    /// Failed to register a dynamic lane.
    LaneNotFound(String),
    /// A dynamic lane could not be registered.
    #[error("An attempt to register a dynamic lane failed: {0}")]
    FailedLaneRegistration(DynamicRegistrationError),
    /// A commander could not be registered/
    #[error("An attempt to register a commander failed: {0}")]
    FailedCommanderRegistration(CommanderRegistrationError),
    /// An event handler failed in a user specified effect.
    #[error("An error occurred in a user specified effect: {0}")]
    EffectError(Box<dyn std::error::Error + Send>),
    /// The event handler has explicitly requested that the agent stop.
    #[error("The event handler has instructed the agent to stop.")]
    StopInstructed,
}

bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    #[doc(hidden)]
    pub(crate) struct ModificationFlags: u8 {
        /// The lane has data to write.
        const DIRTY = 0b01;
        /// The lane's event handler should be triggered.
        const TRIGGER_HANDLER = 0b10;
    }

}

/// When a handler completes or suspends it can indicate that is has modified the
/// state of an item.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct Modification {
    /// The ID of the item.
    pub(crate) item_id: u64,
    /// If this is true, lifecycle event handlers on the lane should be executed.
    pub(crate) flags: ModificationFlags,
}

impl Modification {
    pub(crate) fn of(item_id: u64) -> Self {
        Modification {
            item_id,
            flags: ModificationFlags::all(),
        }
    }

    pub(crate) fn no_trigger(item_id: u64) -> Self {
        Modification {
            item_id,
            flags: ModificationFlags::complement(ModificationFlags::TRIGGER_HANDLER),
        }
    }

    pub(crate) fn trigger_only(item_id: u64) -> Self {
        Modification {
            item_id,
            flags: ModificationFlags::TRIGGER_HANDLER,
        }
    }

    pub fn id(&self) -> u64 {
        self.item_id
    }
}

/// The result of running a single step of an event handler.
#[derive(Debug)]
pub enum StepResult<C> {
    /// The event handler has suspended.
    Continue {
        /// Indicates if an item has been modified,
        modified_item: Option<Modification>,
    },
    /// The handler has failed and will never now produce a result.
    Fail(EventHandlerError),
    /// The handler has completed successfully. All further attempts to step
    /// will result in an error.
    Complete {
        /// Indicates if an item has been modified.
        modified_item: Option<Modification>,
        /// The result of the handler.
        result: C,
    },
}

impl<C> StepResult<C> {
    /// Create a result indicating that the handler has more work to do.
    pub fn cont() -> Self {
        StepResult::Continue {
            modified_item: None,
        }
    }

    /// Create a result that produces a value. The handler should no longer be stepped after this.
    pub fn done(result: C) -> Self {
        Self::Complete {
            modified_item: None,
            result,
        }
    }

    /// Indicate that this handler is already complete and can no longer be stepped.
    pub fn after_done() -> Self {
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    }

    /// Determine if this result indicates that the handler has more work to do.
    pub fn is_cont(&self) -> bool {
        matches!(self, StepResult::Continue { .. })
    }

    /// Transform the result of this result (if it has one).
    pub fn map<F, D>(self, f: F) -> StepResult<D>
    where
        F: FnOnce(C) -> D,
    {
        match self {
            StepResult::Continue { modified_item } => StepResult::Continue { modified_item },
            StepResult::Fail(err) => StepResult::Fail(err),
            StepResult::Complete {
                modified_item,
                result,
            } => StepResult::Complete {
                modified_item,
                result: f(result),
            },
        }
    }
}

/// An event handler that executes a function and immediately completes with the result.
pub struct SideEffect<F>(Option<F>);

/// An event handler that drains an iterator into a vector, suspending after each item
pub struct SideEffects<I: Iterator> {
    eff: I,
    results: Vec<I::Item>,
    done: bool,
}

impl<F> From<F> for SideEffect<F> {
    fn from(f: F) -> Self {
        SideEffect(Some(f))
    }
}

impl<T: IntoIterator> From<T> for SideEffects<T::IntoIter> {
    fn from(it: T) -> Self {
        SideEffects {
            eff: it.into_iter(),
            results: vec![],
            done: false,
        }
    }
}

impl<Context, F, R> HandlerAction<Context> for SideEffect<F>
where
    F: FnOnce() -> R,
{
    type Completion = R;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(f) = self.0.take() {
            StepResult::done(f())
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("SideEffect")
            .field("result_type", &type_name::<R>())
            .finish()
    }
}

impl<Context, I> HandlerAction<Context> for SideEffects<I>
where
    I: Iterator,
{
    type Completion = Vec<I::Item>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let SideEffects { eff, results, done } = self;
        if *done {
            StepResult::after_done()
        } else if let Some(result) = eff.next() {
            results.push(result);
            StepResult::cont()
        } else {
            *done = true;
            StepResult::done(std::mem::take(results))
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("SideEffects")
            .field("item_type", &type_name::<I::Item>())
            .finish()
    }
}

/// Type that is returned by the `map` method on the [`HandlerActionExt`] trait.
pub struct Map<H, F>(Option<(H, F)>);

/// Type that is returned by the `and_then` method on the [`HandlerActionExt`] trait.
#[derive(Debug, Default)]
pub enum AndThen<H1, H2, F> {
    First {
        first: H1,
        next: F,
    },
    Second(H2),
    #[default]
    Done,
}

/// Type that is returned by the `and_then_contextual` method on the [`HandlerActionExt`] trait.
#[derive(Debug, Default)]
pub enum AndThenContextual<H1, H2, F> {
    First {
        first: H1,
        next: F,
    },
    Second(H2),
    #[default]
    Done,
}

/// Type that is returned by the `and_then_try` method on the [`HandlerActionExt`] trait.
#[derive(Debug, Default)]
pub enum AndThenTry<H1, H2, F> {
    First {
        first: H1,
        next: F,
    },
    Second(H2),
    #[default]
    Done,
}

/// Type that is returned by the `followed_by` method on the [`HandlerActionExt`] trait.
#[derive(Debug, Default)]
pub enum FollowedBy<H1, H2> {
    First {
        first: H1,
        next: H2,
    },
    Second(H2),
    #[default]
    Done,
}

impl<H, F> Default for Map<H, F> {
    fn default() -> Self {
        Map(None)
    }
}

impl<H, F> Map<H, F> {
    fn new(handler: H, f: F) -> Self {
        Map(Some((handler, f)))
    }
}

impl<H1, H2, F> AndThen<H1, H2, F> {
    fn new(first: H1, f: F) -> Self {
        AndThen::First { first, next: f }
    }
}

impl<H1, H2, F> AndThenContextual<H1, H2, F> {
    fn new(first: H1, f: F) -> Self {
        AndThenContextual::First { first, next: f }
    }
}

impl<H1, H2, F> AndThenTry<H1, H2, F> {
    fn new(first: H1, f: F) -> Self {
        AndThenTry::First { first, next: f }
    }
}

impl<H1, H2> FollowedBy<H1, H2> {
    fn new(first: H1, second: H2) -> Self {
        FollowedBy::First {
            first,
            next: second,
        }
    }
}

/// An alternative to [`FnOnce`] that allows for named implementations.
#[doc(hidden)]
pub trait HandlerTrans<In> {
    type Out;
    fn transform(self, input: In) -> Self::Out;
}

impl<In, Out, F> HandlerTrans<In> for F
where
    F: FnOnce(In) -> Out,
{
    type Out = Out;

    fn transform(self, input: In) -> Self::Out {
        self(input)
    }
}

/// Transformation within a context.
#[doc(hidden)]
pub trait ContextualTrans<Context, In> {
    type Out;
    fn transform(self, context: &Context, input: In) -> Self::Out;
}

impl<Context, In, Out, F> ContextualTrans<Context, In> for F
where
    F: FnOnce(&Context, In) -> Out,
{
    type Out = Out;

    fn transform(self, context: &Context, input: In) -> Self::Out {
        self(context, input)
    }
}

impl<Context, H, F, T> HandlerAction<Context> for Map<H, F>
where
    H: HandlerAction<Context>,
    F: HandlerTrans<H::Completion, Out = T>,
{
    type Completion = T;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some((mut action, f)) = self.0.take() {
            match action.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    self.0 = Some((action, f));
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(e) => StepResult::Fail(e),
                StepResult::Complete {
                    modified_item,
                    result,
                } => StepResult::Complete {
                    modified_item,
                    result: f.transform(result),
                },
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut dbg = f.debug_struct("Map");
        if let Map(Some((h, _))) = self {
            dbg.field("input", &Described::new(context, h));
        } else {
            dbg.field("input", &CONSUMED);
        }
        dbg.field("result_type", &type_name::<T>()).finish()
    }
}

/// Utility type with a [`std::fmt::Debug`] implementation that calls the [`HandlerAction::describe`] method
/// on an event handler, providing a reference to its context. This is to allow implementations of
/// describe functions on nested handlers to format their inner handlers.
#[derive(Clone, Copy)]
pub struct Described<'a, Context, H> {
    context: &'a Context,
    handler: &'a H,
}

impl<'a, Context, H> Described<'a, Context, H> {
    pub fn new(context: &'a Context, handler: &'a H) -> Self {
        Described { context, handler }
    }
}

impl<'a, Context, H> std::fmt::Debug for Described<'a, Context, H>
where
    H: HandlerAction<Context>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Described { context, handler } = self;
        handler.describe(context, f)
    }
}

impl<Context, H1, H2, F> HandlerAction<Context> for AndThenContextual<H1, H2, F>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    F: ContextualTrans<Context, H1::Completion, Out = H2>,
{
    type Completion = H2::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThenContextual::First { mut first, next } => {
                match first.step(action_context, meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete {
                        modified_item: dirty_lane,
                        result,
                    } => {
                        let second = next.transform(context, result);
                        *self = AndThenContextual::Second(second);
                        StepResult::Continue {
                            modified_item: dirty_lane,
                        }
                    }
                    StepResult::Continue {
                        modified_item: dirty_lane,
                    } => {
                        *self = AndThenContextual::First { first, next };
                        StepResult::Continue {
                            modified_item: dirty_lane,
                        }
                    }
                }
            }
            AndThenContextual::Second(mut second) => {
                let step_result = second.step(action_context, meta, context);
                if step_result.is_cont() {
                    *self = AndThenContextual::Second(second);
                }
                step_result
            }
            _ => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            AndThenContextual::First { first, .. } => f
                .debug_struct("AndThenContextual")
                .field("stage", &"First")
                .field("first", &Described::new(context, first))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThenContextual::Second(second) => f
                .debug_struct("AndThenContextual")
                .field("stage", &"Second")
                .field("second", &Described::new(context, second))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThenContextual::Done => f.debug_tuple("AndThenContextual").field(&CONSUMED).finish(),
        }
    }
}

impl<Context, H1, H2, F> HandlerAction<Context> for AndThen<H1, H2, F>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    F: HandlerTrans<H1::Completion, Out = H2>,
{
    type Completion = H2::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThen::First { mut first, next } => match first.step(action_context, meta, context) {
                StepResult::Fail(e) => StepResult::Fail(e),
                StepResult::Complete {
                    modified_item: dirty_lane,
                    result,
                } => {
                    let second = next.transform(result);
                    *self = AndThen::Second(second);
                    StepResult::Continue {
                        modified_item: dirty_lane,
                    }
                }
                StepResult::Continue {
                    modified_item: dirty_lane,
                } => {
                    *self = AndThen::First { first, next };
                    StepResult::Continue {
                        modified_item: dirty_lane,
                    }
                }
            },
            AndThen::Second(mut second) => {
                let step_result = second.step(action_context, meta, context);
                if step_result.is_cont() {
                    *self = AndThen::Second(second);
                }
                step_result
            }
            _ => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            AndThen::First { first, .. } => f
                .debug_struct("AndThen")
                .field("stage", &"First")
                .field("first", &Described::new(context, first))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThen::Second(second) => f
                .debug_struct("AndThen")
                .field("stage", &"Second")
                .field("second", &Described::new(context, second))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThen::Done => f.debug_tuple("AndThen").field(&CONSUMED).finish(),
        }
    }
}

impl<Context, H1, H2, F> HandlerAction<Context> for AndThenTry<H1, H2, F>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    F: HandlerTrans<H1::Completion, Out = Result<H2, EventHandlerError>>,
{
    type Completion = H2::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThenTry::First { mut first, next } => {
                match first.step(action_context, meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete {
                        modified_item: dirty_lane,
                        result,
                    } => match next.transform(result) {
                        Ok(second) => {
                            *self = AndThenTry::Second(second);
                            StepResult::Continue {
                                modified_item: dirty_lane,
                            }
                        }
                        Err(e) => StepResult::Fail(e),
                    },
                    StepResult::Continue {
                        modified_item: dirty_lane,
                    } => {
                        *self = AndThenTry::First { first, next };
                        StepResult::Continue {
                            modified_item: dirty_lane,
                        }
                    }
                }
            }
            AndThenTry::Second(mut second) => {
                let step_result = second.step(action_context, meta, context);
                if step_result.is_cont() {
                    *self = AndThenTry::Second(second);
                }
                step_result
            }
            _ => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            AndThenTry::First { first, .. } => f
                .debug_struct("AndThenTry")
                .field("stage", &"First")
                .field("first", &Described::new(context, first))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThenTry::Second(second) => f
                .debug_struct("AndThenTry")
                .field("stage", &"Second")
                .field("second", &Described::new(context, second))
                .field("second_type", &type_name::<H2>())
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            AndThenTry::Done => f.debug_tuple("AndThenTry").field(&CONSUMED).finish(),
        }
    }
}

impl<Context, H1, H2> HandlerAction<Context> for FollowedBy<H1, H2>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
{
    type Completion = H2::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            FollowedBy::First { mut first, next } => {
                match first.step(action_context, meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete {
                        modified_item: dirty_lane,
                        ..
                    } => {
                        *self = FollowedBy::Second(next);
                        StepResult::Continue {
                            modified_item: dirty_lane,
                        }
                    }
                    StepResult::Continue {
                        modified_item: dirty_lane,
                    } => {
                        *self = FollowedBy::First { first, next };
                        StepResult::Continue {
                            modified_item: dirty_lane,
                        }
                    }
                }
            }
            FollowedBy::Second(mut second) => {
                let step_result = second.step(action_context, meta, context);
                if step_result.is_cont() {
                    *self = FollowedBy::Second(second);
                }
                step_result
            }
            _ => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            FollowedBy::First { first, next } => f
                .debug_struct("FollowedBy")
                .field("stage", &"First")
                .field("first", &Described::new(context, first))
                .field("next", &Described::new(context, next))
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            FollowedBy::Second(second) => f
                .debug_struct("FollowedBy")
                .field("stage", &"Second")
                .field("second", &Described::new(context, second))
                .field("result_type", &type_name::<H2::Completion>())
                .finish(),
            FollowedBy::Done => f.debug_tuple("FollowedBy").field(&CONSUMED).finish(),
        }
    }
}

/// An event handler that immediately returns a constant value.
pub struct ConstHandler<T>(Option<T>);

/// A handler that returns nothing.
pub type UnitHandler = ConstHandler<()>;

impl<T> From<T> for ConstHandler<T> {
    fn from(value: T) -> Self {
        ConstHandler(Some(value))
    }
}

impl<T: Default> Default for ConstHandler<T> {
    fn default() -> Self {
        ConstHandler(Some(T::default()))
    }
}

impl<T, Context> HandlerAction<Context> for ConstHandler<T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(value) = self.0.take() {
            StepResult::done(value)
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let ConstHandler(value) = self;
        f.debug_struct("ConstHandler")
            .field("value_type", &type_name::<T>())
            .field("consumed", &value.is_none())
            .finish()
    }
}

impl<Context> HandlerAction<Context> for CNil {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        match *self {}
    }

    fn describe(&self, _context: &Context, _f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match *self {}
    }
}

impl<H, T, Context> HandlerAction<Context> for Coproduct<H, T>
where
    H: HandlerAction<Context, Completion = ()>,
    T: HandlerAction<Context, Completion = ()>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match self {
            Coproduct::Inl(h) => h.step(action_context, meta, context),
            Coproduct::Inr(t) => t.step(action_context, meta, context),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Coproduct::Inl(h) => f
                .debug_tuple("Coproduct")
                .field(&"L")
                .field(&Described::new(context, h))
                .finish(),
            Coproduct::Inr(h) => f
                .debug_tuple("Coproduct")
                .field(&"R")
                .field(&Described::new(context, h))
                .finish(),
        }
    }
}

/// An event handler that will get the agent instance metadata.
#[derive(Default, Debug)]
pub struct GetAgentUri {
    done: bool,
}

impl<Context> HandlerAction<Context> for GetAgentUri {
    type Completion = RouteUri;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let GetAgentUri { done } = self;
        if *done {
            StepResult::after_done()
        } else {
            *done = true;
            StepResult::done(meta.agent_uri().clone())
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("GetAgentUri").finish()
    }
}

/// Get a parameter from the route URI of the running agent.
pub struct GetParameter<S> {
    key: Option<S>,
}

impl<S> GetParameter<S> {
    pub fn new(key: S) -> Self {
        GetParameter { key: Some(key) }
    }
}

const CONSUMED: &str = "<<CONSUMED>>";

impl<Context, S: AsRef<str>> HandlerAction<Context> for GetParameter<S> {
    type Completion = Option<String>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let GetParameter { key } = self;
        if let Some(key) = key.take() {
            StepResult::done(meta.get_param(key.as_ref()).map(ToString::to_string))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let body = if let GetParameter { key: Some(name) } = self {
            name.as_ref()
        } else {
            CONSUMED
        };
        f.debug_tuple("GetParameter").field(&body).finish()
    }
}

/// An event handler that will attempt to decode a [readable](`swimos_form::read::StructuralReadable`) type
/// from a buffer, immediately returning the result or an error.
pub struct Decode<'a, T: RecognizerReadable> {
    decoder: Option<&'a mut ReconDecoder<T>>,
    buffer: BytesMut,
}

impl<'a, T: RecognizerReadable> Decode<'a, T> {
    pub fn new(decoder: &'a mut ReconDecoder<T>, buffer: BytesMut) -> Self {
        Decode {
            decoder: Some(decoder),
            buffer,
        }
    }
}

impl<'a, T: RecognizerReadable, Context> HandlerAction<Context> for Decode<'a, T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let Decode { decoder, buffer } = self;
        if let Some(decoder) = decoder.take() {
            decoder.reset();
            match decoder.decode_eof(buffer) {
                Ok(Some(value)) => StepResult::done(value),
                Ok(_) => StepResult::Fail(EventHandlerError::IncompleteCommand),
                Err(e) => StepResult::Fail(EventHandlerError::BadCommand(e)),
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Decode {
            buffer, decoder, ..
        } = self;
        let content = if decoder.is_none() {
            CONSUMED
        } else {
            std::str::from_utf8(buffer.as_ref()).unwrap_or("<<BAD UTF8>>")
        };
        f.debug_struct("Decode")
            .field("target_type", &type_name::<T>())
            .field("content", &content)
            .finish()
    }
}

impl<Context, H1, H2> HandlerAction<Context> for Either<H1, H2>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context, Completion = H1::Completion>,
{
    type Completion = H1::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match self {
            Either::Left(h1) => h1.step(action_context, meta, context),
            Either::Right(h2) => h2.step(action_context, meta, context),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Either::Left(h) => f
                .debug_tuple("Either")
                .field(&"L")
                .field(&Described::new(context, h))
                .finish(),
            Either::Right(h) => f
                .debug_tuple("Either")
                .field(&"R")
                .field(&Described::new(context, h))
                .finish(),
        }
    }
}

/// Adds combinators to the [`HandlerAction`] trait.
pub trait HandlerActionExt<Context>: HandlerAction<Context> {
    /// Create a new handler that runs this handler and then transforms the result.
    fn map<F>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: HandlerTrans<Self::Completion>,
    {
        Map::new(self, f)
    }

    /// Create a new handler which applies a function to the result of this handler and then executes
    /// an additional handler returned by the function.
    fn and_then<F, H2>(self, f: F) -> AndThen<Self, H2, F>
    where
        Self: Sized,
        F: HandlerTrans<Self::Completion, Out = H2>,
        H2: HandlerAction<Context>,
    {
        AndThen::new(self, f)
    }

    /// Create a new handler which applies a function to the result of this handler and then executes
    /// an additional handler returned by the function. The functional also receives access to the
    /// context.
    fn and_then_contextual<F, H2>(self, f: F) -> AndThenContextual<Self, H2, F>
    where
        Self: Sized,
        F: ContextualTrans<Context, Self::Completion, Out = H2>,
        H2: HandlerAction<Context>,
    {
        AndThenContextual::new(self, f)
    }

    /// Create a new handler which applies a function to the result of this handler and then executes
    /// an additional handler returned by the function or returns an error if the function fails.
    fn and_then_try<F, H2>(self, f: F) -> AndThenTry<Self, H2, F>
    where
        Self: Sized,
        F: HandlerTrans<Self::Completion, Out = Result<H2, EventHandlerError>>,
        H2: HandlerAction<Context>,
    {
        AndThenTry::new(self, f)
    }

    /// Create a new handler that executes this handler and another in sequence.
    fn followed_by<H2>(self, after: H2) -> FollowedBy<Self, H2>
    where
        Self: Sized,
        H2: HandlerAction<Context>,
    {
        FollowedBy::new(self, after)
    }

    /// Create a new handler that executes this handler and then performs a side effect.
    fn followed_by_eff<F, R>(self, eff: F) -> FollowedBy<Self, SideEffect<F>>
    where
        Self: Sized,
        F: FnOnce() -> R,
    {
        FollowedBy::new(self, eff.into())
    }

    /// Create a new handler that runs this handler then discards its result.
    fn discard(self) -> Discard<Self>
    where
        Self: Sized,
    {
        Discard::new(self)
    }

    /// Attach an annotation to the handler. This will only show up in the description of the
    /// handler and will not affect how it executes or the final result.
    ///
    /// # Arguments
    /// * `annotation` - The annotation value.
    fn annotated<S: Debug>(self, annotation: S) -> Annotated<S, Self>
    where
        Self: Sized,
    {
        Annotated::new(annotation, self)
    }

    /// `BoxHandlerAction` without the `Send` requirement.
    fn boxed_local<'a>(self) -> LocalBoxHandlerAction<'a, Context, Self::Completion>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }

    /// An owned dynamically typed [`HandlerAction`] where you can't statically type your handler
    /// or need to add some indirection. For a boxed event handler without the `Send` requirement,
    /// see [`HandlerActionExt::boxed_local`]
    fn boxed<'a>(self) -> BoxHandlerAction<'a, Context, Self::Completion>
    where
        Self: Sized + Send + 'a,
    {
        Box::new(self)
    }
}

impl<Context, H: HandlerAction<Context>> HandlerActionExt<Context> for H {}

/// [`HandlerAction`] that runs a sequence of [`EventHandler`]s.
#[derive(Debug, Default)]
pub enum Sequentially<I, Item> {
    Init(I),
    Running(I, Item),
    #[default]
    Done,
}

impl<I: Iterator> Sequentially<I, I::Item> {
    pub fn new<II: IntoIterator<IntoIter = I>>(it: II) -> Self {
        Sequentially::Init(it.into_iter())
    }
}

impl<I, H, Context> HandlerAction<Context> for Sequentially<I, H>
where
    I: Iterator<Item = H>,
    H: EventHandler<Context>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        loop {
            match std::mem::take(self) {
                Sequentially::Init(mut it) => {
                    if let Some(h) = it.next() {
                        *self = Sequentially::Running(it, h);
                    } else {
                        *self = Sequentially::Done;
                        break StepResult::done(());
                    }
                }
                Sequentially::Running(mut it, mut h) => {
                    let result = h.step(action_context, meta, context);
                    break match result {
                        StepResult::Continue { modified_item } => {
                            *self = Sequentially::Running(it, h);
                            StepResult::Continue { modified_item }
                        }
                        StepResult::Fail(e) => {
                            *self = Sequentially::Done;
                            StepResult::Fail(e)
                        }
                        StepResult::Complete { modified_item, .. } => {
                            if let Some(h2) = it.next() {
                                *self = Sequentially::Running(it, h2);
                                StepResult::Continue { modified_item }
                            } else {
                                *self = Sequentially::Done;
                                StepResult::Complete {
                                    modified_item,
                                    result: (),
                                }
                            }
                        }
                    };
                }
                Sequentially::Done => {
                    break StepResult::after_done();
                }
            }
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Sequentially::Init(_) => f
                .debug_struct("Sequentially")
                .field("handler_type", &type_name::<H>())
                .finish(),
            Sequentially::Running(_, current) => f
                .debug_struct("Sequentially")
                .field("handler_type", &type_name::<H>())
                .field("current", &Described::new(context, current))
                .finish(),
            Sequentially::Done => f.debug_tuple("Sequentially").field(&CONSUMED).finish(),
        }
    }
}

/// Event handler that runs another handler and discards its result.
#[derive(Debug)]
pub struct Discard<H>(H);

impl<H> Discard<H> {
    pub fn new(handler: H) -> Discard<H> {
        Discard(handler)
    }
}

impl<Context, H: HandlerAction<Context>> HandlerAction<Context> for Discard<H> {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Discard(inner) = self;
        inner.step(action_context, meta, context).map(|_| ())
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Discard(h) = self;
        f.debug_tuple("Discard")
            .field(&Described::new(context, h))
            .finish()
    }
}

/// Shorthand for a handler action that will open a value downlink.
pub trait OpenValueDownlink<Context, T>:
    HandlerAction<Context, Completion = ValueDownlinkHandle<T>>
{
}

impl<Context, T, H> OpenValueDownlink<Context, T> for H where
    H: HandlerAction<Context, Completion = ValueDownlinkHandle<T>>
{
}

/// Shorthand for a handler action that will open a map downlink.
pub trait OpenMapDownlink<Context, K, V>:
    HandlerAction<Context, Completion = MapDownlinkHandle<K, V>>
{
}

impl<Context, K, V, H> OpenMapDownlink<Context, K, V> for H where
    H: HandlerAction<Context, Completion = MapDownlinkHandle<K, V>>
{
}

impl<Context, H> HandlerAction<Context> for Option<H>
where
    H: HandlerAction<Context>,
{
    type Completion = Option<H::Completion>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(inner) = self {
            inner.step(action_context, meta, context).map(Option::Some)
        } else {
            StepResult::done(None)
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Some(inner) => f
                .debug_struct("Option")
                .field("defined", &true)
                .field("inner", &Described::new(context, inner))
                .finish(),
            None => f
                .debug_struct("Option")
                .field("defined", &false)
                .field("inner_type", &type_name::<H>())
                .finish(),
        }
    }
}

/// Join lane lifecycle are registered within the [`ActionContext`] when an agent starts. When a new downlink
/// is opened for that lane, it will make a request for the appropriate lifecycle. If the types associated with
/// the lifecycle and the lane do not match, this error will be raised. If the lifecycle has been created by
/// the derive macros, this will never occur.
#[derive(Debug, Error)]
pub enum DowncastError {
    /// The link key type for a join map lane was incorrect.
    #[error("Expected a key of type {expected_type:?} but received type {:?}", (**key).type_id())]
    LinkKey {
        key: Box<dyn Any + Send>,
        expected_type: TypeId,
    },
    /// The key type for a join value or map lane was incorrect.
    #[error("Expected key type {expected_type:?} but received type {actual_type:?}")]
    Key {
        actual_type: TypeId,
        expected_type: TypeId,
    },
    /// The value type for a join value or map lane was incorrect.
    #[error("Expected value type {expected_type:?} but received type {actual_type:?}")]
    Value {
        actual_type: TypeId,
        expected_type: TypeId,
    },
}

#[doc(hidden)]
pub trait JoinLaneInitializer<Context>: Send {
    fn try_create_action(
        &self,
        link_key: Box<dyn Any + Send>,
        key_type: TypeId,
        value_type: TypeId,
        address: Address<Text>,
    ) -> Result<Box<dyn EventHandler<Context> + Send + 'static>, DowncastError>;

    fn kind(&self) -> JoinLaneKind;
}

static_assertions::assert_obj_safe!(JoinLaneInitializer<()>);

#[doc(hidden)]
pub type BoxJoinLaneInit<'a, Context> = Box<dyn JoinLaneInitializer<Context> + Send + 'a>;

/// Causes the agent to stop. If this is encountered during the `on_start` event of an agent it will
/// fail to start at all. Otherwise, execution of the event handler will terminate and the agent will
/// begin to shutdown. The 'on_stop' handler will still be run. If a [`Stop`] is encountered in
/// the 'on_stop' handler, the agent will stop immediately.
#[derive(Debug, Clone, Copy, Default)]
pub struct Stop;

impl<Context> HandlerAction<Context> for Stop {
    type Completion = Never;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        StepResult::Fail(EventHandlerError::StopInstructed)
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("Stop").finish()
    }
}

enum JoinState<Context, H1: HandlerAction<Context>, H2: HandlerAction<Context>> {
    Init(H1, H2),
    FirstDone(H1::Completion, H2),
    AfterDone,
}

/// The [`HandlerAction`] returned by the [`join`] function.
pub struct Join<Context, H1: HandlerAction<Context>, H2: HandlerAction<Context>> {
    state: JoinState<Context, H1, H2>,
}

/// Create a [`HandlerAction`] that runs two other actions and produces a tuple of their results.
pub fn join<Context, H1, H2>(first: H1, second: H2) -> Join<Context, H1, H2>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
{
    Join {
        state: JoinState::Init(first, second),
    }
}

impl<Context, H1, H2> HandlerAction<Context> for Join<Context, H1, H2>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
{
    type Completion = (H1::Completion, H2::Completion);

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Join { state } = self;
        match std::mem::replace(state, JoinState::AfterDone) {
            JoinState::Init(mut h1, h2) => match h1.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    *state = JoinState::Init(h1, h2);
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(err) => StepResult::Fail(err),
                StepResult::Complete {
                    modified_item,
                    result,
                } => {
                    *state = JoinState::FirstDone(result, h2);
                    StepResult::Continue { modified_item }
                }
            },
            JoinState::FirstDone(v1, mut h2) => match h2.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    *state = JoinState::FirstDone(v1, h2);
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(err) => StepResult::Fail(err),
                StepResult::Complete {
                    modified_item,
                    result,
                } => StepResult::Complete {
                    modified_item,
                    result: (v1, result),
                },
            },
            JoinState::AfterDone => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Join { state } = self;
        match state {
            JoinState::Init(h1, h2) => f
                .debug_struct("Join")
                .field("state", &"First")
                .field("first_handler", &Described::new(context, h1))
                .field("second_handler", &Described::new(context, h2))
                .finish(),
            JoinState::FirstDone(_, h2) => f
                .debug_struct("Join")
                .field("state", &"Second")
                .field("first_handler_type", &type_name::<H1>())
                .field("second_handler", &Described::new(context, h2))
                .finish(),
            JoinState::AfterDone => f
                .debug_struct("Join")
                .field("state", &CONSUMED)
                .field("first_handler_type", &type_name::<H1>())
                .field("second_handler_type", &type_name::<H2>())
                .finish(),
        }
    }
}

enum Join3State<Context, H1, H2, H3>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    H3: HandlerAction<Context>,
{
    Init(H1, H2, H3),
    FirstDone(H1::Completion, H2, H3),
    SecondDone(H1::Completion, H2::Completion, H3),
    AfterDone,
}

/// The [`HandlerAction`] returned by the [`join3`] function.
pub struct Join3<Context, H1, H2, H3>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    H3: HandlerAction<Context>,
{
    state: Join3State<Context, H1, H2, H3>,
}

/// Create a [`HandlerAction`] that runs three other actions and produces a tuple of their results.
pub fn join3<Context, H1, H2, H3>(first: H1, second: H2, third: H3) -> Join3<Context, H1, H2, H3>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    H3: HandlerAction<Context>,
{
    Join3 {
        state: Join3State::Init(first, second, third),
    }
}

impl<Context, H1, H2, H3> HandlerAction<Context> for Join3<Context, H1, H2, H3>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
    H3: HandlerAction<Context>,
{
    type Completion = (H1::Completion, H2::Completion, H3::Completion);

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let Join3 { state } = self;
        match std::mem::replace(state, Join3State::AfterDone) {
            Join3State::Init(mut h1, h2, h3) => match h1.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    *state = Join3State::Init(h1, h2, h3);
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(err) => StepResult::Fail(err),
                StepResult::Complete {
                    modified_item,
                    result,
                } => {
                    *state = Join3State::FirstDone(result, h2, h3);
                    StepResult::Continue { modified_item }
                }
            },
            Join3State::FirstDone(v1, mut h2, h3) => match h2.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    *state = Join3State::FirstDone(v1, h2, h3);
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(err) => StepResult::Fail(err),
                StepResult::Complete {
                    modified_item,
                    result,
                } => {
                    *state = Join3State::SecondDone(v1, result, h3);
                    StepResult::Continue { modified_item }
                }
            },
            Join3State::SecondDone(v1, v2, mut h3) => {
                match h3.step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        *state = Join3State::SecondDone(v1, v2, h3);
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => StepResult::Fail(err),
                    StepResult::Complete {
                        modified_item,
                        result,
                    } => StepResult::Complete {
                        modified_item,
                        result: (v1, v2, result),
                    },
                }
            }
            Join3State::AfterDone => StepResult::after_done(),
        }
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Join3 { state } = self;
        match state {
            Join3State::Init(h1, h2, h3) => f
                .debug_struct("Join3")
                .field("state", &"First")
                .field("first_handler", &Described::new(context, h1))
                .field("second_handler", &Described::new(context, h2))
                .field("third_handler", &Described::new(context, h3))
                .finish(),
            Join3State::FirstDone(_, h2, h3) => f
                .debug_struct("Join3")
                .field("state", &"Second")
                .field("first_handler_type", &type_name::<H1>())
                .field("second_handler", &Described::new(context, h2))
                .field("third_handler", &Described::new(context, h3))
                .finish(),
            Join3State::SecondDone(_, _, h3) => f
                .debug_struct("Join3")
                .field("state", &"Third")
                .field("first_handler_type", &type_name::<H1>())
                .field("second_handler_type", &type_name::<H2>())
                .field("third_handler", &Described::new(context, h3))
                .finish(),
            Join3State::AfterDone => f
                .debug_struct("Join3")
                .field("state", &CONSUMED)
                .field("first_handler_type", &type_name::<H1>())
                .field("second_handler_type", &type_name::<H2>())
                .field("third_handler_type", &type_name::<H3>())
                .finish(),
        }
    }
}

/// An event handler that fails with the provided error.
pub struct Fail<T, E> {
    error: Option<E>,
    _type: PhantomData<T>,
}

impl<T, E> Fail<T, E> {
    pub fn new(error: E) -> Self {
        Fail {
            error: Some(error),
            _type: PhantomData,
        }
    }
}

impl<Context, T, E> HandlerAction<Context> for Fail<T, E>
where
    E: std::error::Error + Send + 'static,
{
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(e) = self.error.take() {
            StepResult::Fail(EventHandlerError::EffectError(Box::new(e)))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Fail { error, .. } = self;
        f.debug_struct("Fail")
            .field("dummy_type", &type_name::<T>())
            .field("error", error)
            .finish()
    }
}

/// Computes a value from the parameters passed to the agent.
pub struct WithParameters<F> {
    f: Option<F>,
}

impl<F> WithParameters<F> {
    pub fn new(f: F) -> Self {
        WithParameters { f: Some(f) }
    }
}

impl<Context, F, T> HandlerAction<Context> for WithParameters<F>
where
    F: FnOnce(&HashMap<String, String>) -> T,
{
    type Completion = T;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let WithParameters { f } = self;
        if let Some(f) = f.take() {
            StepResult::done(f(meta.get_params()))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("WithParameters").finish()
    }
}

/// Schedule the agent's `on_timer` event to be called.
#[derive(Debug)]
pub struct ScheduleTimerEvent {
    at: Option<Instant>,
    id: u64,
}

impl ScheduleTimerEvent {
    /// # Arguments
    /// * `at` - The time at which the event should trigger. If this is in the past, the event will trigger immediately.
    /// * `id` - The ID to be passed to the event handler.
    pub fn new(at: Instant, id: u64) -> ScheduleTimerEvent {
        ScheduleTimerEvent { at: Some(at), id }
    }
}

impl<Context> HandlerAction<Context> for ScheduleTimerEvent {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let ScheduleTimerEvent { at, id } = self;
        if let Some(t) = at.take() {
            action_context.schedule_timer(t, *id);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }

    fn describe(&self, _context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

/// Adds an annotation to an event handler. This will be visible in the description message for
/// the handler but has not effect on how the handler executes or its final result.
pub struct Annotated<S, H> {
    annotation: S,
    handler: H,
}

impl<S, H> Annotated<S, H> {
    pub fn new(annotation: S, handler: H) -> Self {
        Annotated {
            annotation,
            handler,
        }
    }
}

impl<S, Context, H> HandlerAction<Context> for Annotated<S, H>
where
    S: Debug,
    H: HandlerAction<Context>,
{
    type Completion = H::Completion;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        self.handler.step(action_context, meta, context)
    }

    fn describe(&self, context: &Context, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let Annotated {
            annotation,
            handler,
        } = self;
        f.debug_struct("Annotated")
            .field("annotation", annotation)
            .field("handler", &Described::new(context, handler))
            .finish()
    }
}
