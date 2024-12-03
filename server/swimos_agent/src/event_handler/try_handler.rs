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

use std::any::type_name;

use crate::{event_handler::Described, AgentMetadata};

use super::{ActionContext, EventHandlerError, HandlerAction, StepResult};
#[cfg(feature = "diverge-check")]
use std::hash::Hasher;

/// An alternative view of a [`HandlerAction`] that produces a [`Result`]. This trait is a
/// convenience that makes type inference easier for combinators that rely on
/// the structure of the result (for example, handling the error).
pub trait TryHandlerAction<Context>: HandlerAction<Context> {
    type Ok;
    type Error;

    /// Step the handler. See [`HandlerAction::step`].
    fn try_step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Result<Self::Ok, Self::Error>>;
}

pub trait TryHandlerActionExt<Context>: TryHandlerAction<Context> {
    /// Create a handler that passes any errors up to the agent and continues only if
    /// the result is [`Ok`].
    fn try_handler(self) -> TryHandler<Self>
    where
        Self: Sized,
        Self::Error: std::error::Error + Send + 'static,
    {
        TryHandler::new(self)
    }

    /// Create a new handler which applies a function to the Ok result of this handler and then executes
    /// an additional handler returned by the function.
    fn and_then_ok<F, H2>(self, f: F) -> AndThenOk<Self, H2, F>
    where
        Self: Sized,
        F: FnOnce(Self::Ok) -> H2,
        H2: HandlerAction<Context>,
    {
        AndThenOk::First {
            first: self,
            next: f,
        }
    }
}

impl<Context, H, Ok, Error> TryHandlerAction<Context> for H
where
    H: HandlerAction<Context, Completion = Result<Ok, Error>>,
{
    type Ok = Ok;

    type Error = Error;

    fn try_step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Result<Self::Ok, Self::Error>> {
        self.step(action_context, meta, context)
    }
}

impl<Context, H> TryHandlerActionExt<Context> for H where H: TryHandlerAction<Context> {}

#[doc(hidden)]
pub struct TryHandler<H>(H);

impl<H> TryHandler<H> {
    fn new(handler: H) -> Self {
        TryHandler(handler)
    }
}

impl<Context, H> HandlerAction<Context> for TryHandler<H>
where
    H: TryHandlerAction<Context>,
    H::Error: std::error::Error + Send + 'static,
{
    type Completion = H::Ok;

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        match self.0.try_step(action_context, meta, context) {
            StepResult::Continue { modified_item } => StepResult::Continue { modified_item },
            StepResult::Fail(err) => StepResult::Fail(err),
            StepResult::Complete {
                modified_item,
                result: Ok(value),
            } => StepResult::Complete {
                modified_item,
                result: value,
            },
            StepResult::Complete {
                result: Err(error), ..
            } => StepResult::Fail(EventHandlerError::EffectError(Box::new(error))),
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let TryHandler(handler) = self;
        f.debug_tuple("TryHandler")
            .field(&Described::new(context, handler))
            .finish()
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        self.0.has_identity()
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &Context, hasher: &mut dyn Hasher) {
        self.0.identity_hash(context, hasher);
    }
}

/// Type that is returned by the `and_then_ok` method on the [`TryHandlerActionExt`] trait.
#[derive(Debug, Default)]
#[doc(hidden)]
pub enum AndThenOk<H1, H2, F> {
    First {
        first: H1,
        next: F,
    },
    Second(H2),
    #[default]
    Done,
}

impl<C, H1, H2, F> HandlerAction<C> for AndThenOk<H1, H2, F>
where
    H1: TryHandlerAction<C, Ok = H2>,
    H2: HandlerAction<C>,
    F: FnOnce(H1::Ok, H2),
{
    type Completion = Result<H2::Completion, H1::Error>;

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThenOk::First { mut first, next } => {
                match first.try_step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        *self = AndThenOk::First { first, next };
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => {
                        *self = AndThenOk::Done;
                        StepResult::Fail(err)
                    }
                    StepResult::Complete {
                        modified_item,
                        result: Ok(result),
                    } => {
                        *self = AndThenOk::Second(result);
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Complete {
                        modified_item,
                        result: Err(error),
                    } => {
                        *self = AndThenOk::Done;
                        StepResult::Complete {
                            modified_item,
                            result: Err(error),
                        }
                    }
                }
            }
            AndThenOk::Second(mut second) => match second.step(action_context, meta, context) {
                StepResult::Continue { modified_item } => {
                    *self = AndThenOk::Second(second);
                    StepResult::Continue { modified_item }
                }
                StepResult::Fail(err) => {
                    *self = AndThenOk::Done;
                    StepResult::Fail(err)
                }
                StepResult::Complete {
                    modified_item,
                    result,
                } => {
                    *self = AndThenOk::Done;
                    StepResult::Complete {
                        modified_item,
                        result: Ok(result),
                    }
                }
            },
            AndThenOk::Done => StepResult::after_done(),
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        match self {
            AndThenOk::First { first, .. } => f
                .debug_struct("AndThenOk")
                .field("state", &"First")
                .field("result_type", &type_name::<H2::Completion>())
                .field("error_type", &type_name::<H1::Error>())
                .field("first", &Described::new(context, first))
                .finish(),
            AndThenOk::Second(second) => f
                .debug_struct("AndThenOk")
                .field("state", &"Second")
                .field("result_type", &type_name::<H2::Completion>())
                .field("error_type", &type_name::<H1::Error>())
                .field("second", &Described::new(context, second))
                .finish(),
            AndThenOk::Done => f
                .debug_struct("AndThenOk")
                .field("result_type", &type_name::<H2::Completion>())
                .field("error_type", &type_name::<H1::Error>())
                .field("state", &"Done")
                .finish(),
        }
    }

    #[cfg(feature = "diverge-check")]
    fn has_identity(&self) -> bool {
        match self {
            AndThenOk::First { first, .. } => first.has_identity(),
            AndThenOk::Second(second) => second.has_identity(),
            AndThenOk::Done => false,
        }
    }

    #[cfg(feature = "diverge-check")]
    fn identity_hash(&self, context: &C, hasher: &mut dyn Hasher) {
        match self {
            AndThenOk::First { first, .. } => first.identity_hash(context, hasher),
            AndThenOk::Second(second) => second.identity_hash(context, hasher),
            AndThenOk::Done => {}
        }
    }
}
