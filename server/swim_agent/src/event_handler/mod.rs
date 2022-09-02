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

use std::{cell::RefCell, marker::PhantomData};

use bytes::BytesMut;
use frunk::{coproduct::CNil, Coproduct};
use futures::{future::Either, stream::BoxStream, FutureExt};
use static_assertions::assert_obj_safe;
use swim_api::{agent::AgentContext, error::AgentRuntimeError};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::address::Address;
use swim_recon::parser::{AsyncParseError, RecognizerDecoder};
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};
use thiserror::Error;
use tokio_util::codec::Decoder;

use crate::{
    agent_model::downlink::{handlers::BoxDownlinkChannel, RegisterHostedDownlink},
    meta::AgentMetadata,
};

mod suspend;
#[cfg(test)]
mod tests;

pub use suspend::{HandlerFuture, Spawner, Suspend};

pub type WriteStream = BoxStream<'static, Result<(), std::io::Error>>;

pub trait DownlinkSpawner<Context> {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Context>,
        dl_writer: WriteStream,
    ) -> Result<(), AgentRuntimeError>;
}

impl<Context> DownlinkSpawner<Context>
    for RefCell<Vec<(BoxDownlinkChannel<Context>, WriteStream)>>
{
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Context>,
        dl_writer: WriteStream,
    ) -> Result<(), AgentRuntimeError> {
        self.borrow_mut().push((dl_channel, dl_writer));
        Ok(())
    }
}

impl<F, Context> DownlinkSpawner<Context> for F
where
    F: Fn(BoxDownlinkChannel<Context>, WriteStream) -> Result<(), AgentRuntimeError>,
{
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Context>,
        dl_writer: WriteStream,
    ) -> Result<(), AgentRuntimeError> {
        (*self)(dl_channel, dl_writer)
    }
}

pub struct ActionContext<'a, Context> {
    spawner: &'a dyn Spawner<Context>,
    agent_context: &'a dyn AgentContext,
    downlink: &'a dyn DownlinkSpawner<Context>,
}

impl<'a, Context> Clone for ActionContext<'a, Context> {
    fn clone(&self) -> Self {
        Self {
            spawner: self.spawner,
            agent_context: self.agent_context,
            downlink: self.downlink,
        }
    }
}

impl<'a, Context> Copy for ActionContext<'a, Context> {}

impl<'a, Context> Spawner<Context> for ActionContext<'a, Context> {
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self.spawner.spawn_suspend(fut)
    }
}

impl<'a, Context> DownlinkSpawner<Context> for ActionContext<'a, Context> {
    fn spawn_downlink(
        &self,
        dl_channel: BoxDownlinkChannel<Context>,
        dl_writer: BoxStream<'static, Result<(), std::io::Error>>,
    ) -> Result<(), AgentRuntimeError> {
        self.downlink.spawn_downlink(dl_channel, dl_writer)
    }
}

impl<'a, Context> ActionContext<'a, Context> {
    pub fn new(
        spawner: &'a dyn Spawner<Context>,
        agent_context: &'a dyn AgentContext,
        downlink: &'a dyn DownlinkSpawner<Context>,
    ) -> Self {
        ActionContext {
            spawner,
            agent_context,
            downlink,
        }
    }

    pub(crate) fn start_downlink<S, F, G, OnDone, H>(
        &self,
        path: Address<S>,
        make_channel: F,
        make_write_stream: G,
        on_done: OnDone,
    ) where
        Context: 'static,
        S: AsRef<str>,
        F: FnOnce(ByteReader) -> BoxDownlinkChannel<Context> + Send + 'static,
        G: FnOnce(ByteWriter) -> WriteStream + Send + 'static,
        OnDone: FnOnce(Result<(), AgentRuntimeError>) -> H + Send + 'static,
        H: EventHandler<Context> + Send + 'static,
    {
        let Address { host, node, lane } = path;
        let external = self.agent_context.open_downlink_new(
            host.as_ref().map(AsRef::as_ref),
            node.as_ref(),
            lane.as_ref(),
        );
        let fut = external
            .map(move |result| {
                match result {
                    Ok((writer, reader)) => {
                        let channel = make_channel(reader);
                        let write_stream = make_write_stream(writer);
                        HandlerActionExt::and_then(
                            RegisterHostedDownlink::new(channel, write_stream),
                            on_done,
                        )
                        .boxed()
                        //RegisterHostedDownlink::new(channel, write_stream).and_then(on_done).boxed()
                    }
                    Err(e) => on_done(Err(e)).boxed(),
                }
            })
            .boxed();
        self.spawn_suspend(fut);
    }
}

/// Trait to desribe an action to be taken, within the context of an agent, when an event ocurrs. The
/// execution of an event handler can be suspended (so that it can trigger the exection of other handlers).
/// This could be expressed using generators from the standard library after this feature is stabilized.
/// A handler instance can be used exactly once. After it has returned a result or an error all subsequent
/// step executions must result in an error.
///
/// It should not generally be necessary to implement this trait in user code.
///
/// #Type Parameters
/// * `Context` - The context within which the handler executes. Typically, this will be a struct type where
/// each field is a lane of an agent.
pub trait HandlerAction<Context> {
    /// The result of executing the handler to completion.
    type Completion;

    /// Run one step of the handler. This can result in either the handler suspending execution, completing
    /// with a result or returning an error.
    ///
    /// # Arguments
    /// * `suspend` - Allows for futures to be suspended into the agent task. The future will result in another event handler
    /// which will be executed by the agent task upon completion.
    /// * `meta` - Provides access to agent instance metadata.
    /// * `context` - The execution context of the handler (providing access to the lanes of the agent).
    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion>;
}

pub trait EventHandler<Context>: HandlerAction<Context, Completion = ()> {}

assert_obj_safe!(EventHandler<()>);

pub type BoxEventHandler<'a, Context> = Box<dyn EventHandler<Context> + 'a>;

impl<Context, H> EventHandler<Context> for H where H: HandlerAction<Context, Completion = ()> {}

impl<'a, H, Context> HandlerAction<Context> for &'a mut H
where
    H: HandlerAction<Context>,
{
    type Completion = H::Completion;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        (*self).step(action_context, meta, context)
    }
}

impl<H: ?Sized, Context> HandlerAction<Context> for Box<H>
where
    H: HandlerAction<Context>,
{
    type Completion = H::Completion;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        (**self).step(action_context, meta, context)
    }
}

/// Error type for fallible event handlers.
#[derive(Debug, Error)]
pub enum EventHandlerError {
    #[error("Event handler stepped after completion.")]
    SteppedAfterComplete,
    #[error("Invalid incoming message: {0}")]
    BadCommand(AsyncParseError),
    #[error("An incoming message was incomplete.")]
    IncompleteCommand,
    #[error("An error ocurred in the agent runtime.")]
    RuntimeError(#[from] AgentRuntimeError),
}

/// When a handler completes or suspends it can inidcate that is has modified the
/// state of a lane.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Modification {
    /// The ID of the lane.
    pub lane_id: u64,
    /// If this is true, lifecycle event handlers on the lane should be executed.
    pub trigger_handler: bool,
}

impl Modification {
    pub fn of(lane_id: u64) -> Self {
        Modification {
            lane_id,
            trigger_handler: true,
        }
    }

    pub fn no_trigger(lane_id: u64) -> Self {
        Modification {
            lane_id,
            trigger_handler: false,
        }
    }
}

/// The result of running a single step of an event handler.
#[derive(Debug)]
pub enum StepResult<C> {
    /// The event handler has suspended.
    Continue {
        /// Indicates if a lane has been modified,
        modified_lane: Option<Modification>,
    },
    /// The handler has failed and will never now produce a result.
    Fail(EventHandlerError),
    /// The handler has completed successfully. All further attempts to step
    /// will result in an error.
    Complete {
        /// Indicates if a lane has been modified.
        modified_lane: Option<Modification>,
        /// The result of the handler.
        result: C,
    },
}

impl<C> StepResult<C> {
    pub fn cont() -> Self {
        StepResult::Continue {
            modified_lane: None,
        }
    }

    pub fn done(result: C) -> Self {
        Self::Complete {
            modified_lane: None,
            result,
        }
    }

    pub fn after_done() -> Self {
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    }

    fn is_cont(&self) -> bool {
        matches!(self, StepResult::Continue { .. })
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
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(f) = self.0.take() {
            StepResult::done(f())
        } else {
            StepResult::after_done()
        }
    }
}

impl<Context, I> HandlerAction<Context> for SideEffects<I>
where
    I: Iterator,
{
    type Completion = Vec<I::Item>;

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
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
}

/// Type that is returned by the `map` method on the [`HandlerActionExt`] trait.
pub struct Map<H, F>(Option<(H, F)>);

/// Type that is returned by the `and_then` method on the [`HandlerActionExt`] trait.
pub enum AndThen<H1, H2, F> {
    First { first: H1, next: F },
    Second(H2),
    Done,
}

/// Type that is returned by the `and_then_try` method on the [`HandlerActionExt`] trait.
pub enum AndThenTry<H1, H2, F> {
    First { first: H1, next: F },
    Second(H2),
    Done,
}

/// Type that is returned by the `followed_by` method on the [`HandlerActionExt`] trait.
pub enum FollowedBy<H1, H2> {
    First { first: H1, next: H2 },
    Second(H2),
    Done,
}

impl<H, F> Default for Map<H, F> {
    fn default() -> Self {
        Map(None)
    }
}

impl<H1, H2, F> Default for AndThen<H1, H2, F> {
    fn default() -> Self {
        AndThen::Done
    }
}

impl<H1, H2, F> Default for AndThenTry<H1, H2, F> {
    fn default() -> Self {
        AndThenTry::Done
    }
}

impl<H1, H2> Default for FollowedBy<H1, H2> {
    fn default() -> Self {
        FollowedBy::Done
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

impl<Context, H, F, T> HandlerAction<Context> for Map<H, F>
where
    H: HandlerAction<Context>,
    F: HandlerTrans<H::Completion, Out = T>,
{
    type Completion = T;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some((mut action, f)) = self.0.take() {
            match action.step(action_context, meta, context) {
                StepResult::Continue { modified_lane } => {
                    self.0 = Some((action, f));
                    StepResult::Continue { modified_lane }
                }
                StepResult::Fail(e) => StepResult::Fail(e),
                StepResult::Complete {
                    modified_lane,
                    result,
                } => StepResult::Complete {
                    modified_lane,
                    result: f.transform(result),
                },
            }
        } else {
            StepResult::after_done()
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
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThen::First { mut first, next } => match first.step(action_context, meta, context) {
                StepResult::Fail(e) => StepResult::Fail(e),
                StepResult::Complete {
                    modified_lane: dirty_lane,
                    result,
                } => {
                    let second = next.transform(result);
                    *self = AndThen::Second(second);
                    StepResult::Continue {
                        modified_lane: dirty_lane,
                    }
                }
                StepResult::Continue {
                    modified_lane: dirty_lane,
                } => {
                    *self = AndThen::First { first, next };
                    StepResult::Continue {
                        modified_lane: dirty_lane,
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
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThenTry::First { mut first, next } => {
                match first.step(action_context, meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete {
                        modified_lane: dirty_lane,
                        result,
                    } => match next.transform(result) {
                        Ok(second) => {
                            *self = AndThenTry::Second(second);
                            StepResult::Continue {
                                modified_lane: dirty_lane,
                            }
                        }
                        Err(e) => StepResult::Fail(e),
                    },
                    StepResult::Continue {
                        modified_lane: dirty_lane,
                    } => {
                        *self = AndThenTry::First { first, next };
                        StepResult::Continue {
                            modified_lane: dirty_lane,
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
}

impl<Context, H1, H2> HandlerAction<Context> for FollowedBy<H1, H2>
where
    H1: HandlerAction<Context>,
    H2: HandlerAction<Context>,
{
    type Completion = H2::Completion;

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            FollowedBy::First { mut first, next } => {
                match first.step(action_context, meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete {
                        modified_lane: dirty_lane,
                        ..
                    } => {
                        *self = FollowedBy::Second(next);
                        StepResult::Continue {
                            modified_lane: dirty_lane,
                        }
                    }
                    StepResult::Continue {
                        modified_lane: dirty_lane,
                    } => {
                        *self = FollowedBy::First { first, next };
                        StepResult::Continue {
                            modified_lane: dirty_lane,
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
}

/// An event handler that immediately returns a constant value.
pub struct ConstHandler<T>(Option<T>);

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
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        if let Some(value) = self.0.take() {
            StepResult::done(value)
        } else {
            StepResult::after_done()
        }
    }
}

impl<Context> HandlerAction<Context> for CNil {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
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
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match self {
            Coproduct::Inl(h) => h.step(action_context, meta, context),
            Coproduct::Inr(t) => t.step(action_context, meta, context),
        }
    }
}

/// An event handler that will get the agent instance metadata.
#[derive(Default, Debug)]
pub struct GetAgentUri {
    done: bool,
}

impl<Context> HandlerAction<Context> for GetAgentUri {
    type Completion = RelativeUri;

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
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
}

/// An event handler that will attempt to decode a [`StructuralReadable`] type from a buffer, immediately
/// returning the result or an error.
pub struct Decode<T> {
    _target_type: PhantomData<fn() -> T>,
    buffer: BytesMut,
    complete: bool,
}

impl<T> Decode<T> {
    pub fn new(buffer: BytesMut) -> Self {
        Decode {
            _target_type: PhantomData,
            buffer,
            complete: false,
        }
    }
}

impl<Context, T: RecognizerReadable> HandlerAction<Context> for Decode<T> {
    type Completion = T;

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let Decode {
            buffer, complete, ..
        } = self;
        if *complete {
            StepResult::after_done()
        } else {
            let mut decoder = RecognizerDecoder::new(T::make_recognizer());
            *complete = true;
            match decoder.decode_eof(buffer) {
                Ok(Some(value)) => StepResult::done(value),
                Ok(_) => StepResult::Fail(EventHandlerError::IncompleteCommand),
                Err(e) => StepResult::Fail(EventHandlerError::BadCommand(e)),
            }
        }
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
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match self {
            Either::Left(h1) => h1.step(action_context, meta, context),
            Either::Right(h2) => h2.step(action_context, meta, context),
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
}

impl<Context, H: HandlerAction<Context>> HandlerActionExt<Context> for H {}

pub trait EventHandlerExt<Context>: EventHandler<Context> {
    fn boxed<'a>(self) -> BoxEventHandler<'a, Context>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

impl<Context, H: EventHandler<Context>> EventHandlerExt<Context> for H {}

pub struct Fail<T, E>(Option<Result<T, E>>);

impl<T, E> Fail<T, E> {
    pub fn new(result: Result<T, E>) -> Self {
        Fail(Some(result))
    }
}

impl<T, E, Context> HandlerAction<Context> for Fail<T, E>
where
    EventHandlerError: From<E>,
{
    type Completion = T;

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        match self.0.take() {
            Some(Err(e)) => StepResult::Fail(e.into()),
            Some(Ok(t)) => StepResult::done(t),
            _ => StepResult::after_done(),
        }
    }
}
