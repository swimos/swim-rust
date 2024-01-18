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

use std::any::type_name;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use either::Either;

use swim_recon::parser::ParseError;
use wasm_ir::wpc::EnvAccess;

use crate::agent::{AgentContext, MutableValueLikeItem};
use crate::prelude::lanes::ItemProjection;

#[derive(Debug)]
pub struct HandlerError {
    handler: &'static str,
    cause: HandlerErrorKind,
}

impl HandlerError {
    pub fn new(handler: &'static str, cause: HandlerErrorKind) -> HandlerError {
        HandlerError { handler, cause }
    }
}

impl Error for HandlerError {
    fn cause(&self) -> Option<&dyn Error> {
        Some(&self.cause as &dyn Error)
    }
}

impl Display for HandlerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let HandlerError { handler, cause } = self;
        write!(
            f,
            "Handler '{handler}' produced an error. Caused by: {cause}"
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerErrorKind {
    #[error("{0}")]
    Parse(#[from] ParseError),
    #[error("{0}")]
    Custom(String),
}

#[derive(Debug)]
pub enum EventHandlerError {
    SteppedAfterComplete,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Modification {
    pub item_id: u64,
    pub trigger_handler: bool,
}

impl Modification {
    pub fn of(item_id: u64) -> Self {
        Modification {
            item_id,
            trigger_handler: true,
        }
    }

    pub fn no_trigger(item_id: u64) -> Self {
        Modification {
            item_id,
            trigger_handler: false,
        }
    }
}

#[derive(Debug)]
pub enum StepResult {
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
    },
}

impl StepResult {
    pub fn cont() -> Self {
        StepResult::Continue {
            modified_item: None,
        }
    }

    pub fn done() -> Self {
        StepResult::Complete {
            modified_item: None,
        }
    }
}

pub trait HandlerEffect<A> {
    fn step<H>(&mut self, context: &AgentContext<A, H>, agent: &mut A) -> StepResult
    where
        H: EnvAccess;
}

pub struct Done;

impl<A> HandlerEffect<A> for Done {
    fn step<H>(&mut self, _context: &AgentContext<A, H>, _agent: &mut A) -> StepResult
    where
        H: EnvAccess,
    {
        StepResult::done()
    }
}

impl<A, L, R> HandlerEffect<A> for Either<L, R>
where
    L: HandlerEffect<A>,
    R: HandlerEffect<A>,
{
    fn step<H>(&mut self, context: &AgentContext<A, H>, agent: &mut A) -> StepResult
    where
        H: EnvAccess,
    {
        match self {
            Either::Left(handler) => handler.step(context, agent),
            Either::Right(handler) => handler.step(context, agent),
        }
    }
}

pub struct HandlerContext<A>(PhantomData<A>);

impl<A> Clone for HandlerContext<A> {
    fn clone(&self) -> Self {
        HandlerContext(self.0.clone())
    }
}

impl<A> Default for HandlerContext<A> {
    fn default() -> Self {
        HandlerContext(PhantomData::default())
    }
}

impl<A> Debug for HandlerContext<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HandlerContext({})", type_name::<A>())
    }
}

impl<A> HandlerContext<A>
where
    A: 'static,
{
    pub fn set_value<I, T>(&self, projection: ItemProjection<A, I>, to: T) -> impl HandlerEffect<A>
    where
        I: MutableValueLikeItem<T>,
        T: Send + 'static,
    {
        I::update_handler(projection, to)
    }
}
