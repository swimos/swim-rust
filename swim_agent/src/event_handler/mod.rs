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

use frunk::{coproduct::CNil, Coproduct};
use swim_recon::parser::AsyncParseError;
use swim_utilities::routing::uri::RelativeUri;
use thiserror::Error;

use crate::meta::AgentMetadata;

pub trait EventHandler<Context> {

    type Completion;

    fn step(&mut self, meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion>;

    fn and_then<F, H2>(self, f: F) -> AndThen<Self, H2, F>
    where
        Self: Sized,
        F: FnOnce(Self::Completion) -> Result<H2, EventHandlerError>,
        H2: EventHandler<Context>,
    {
        AndThen::new(self, f)
    }

    fn followed_by<H2>(self, after: H2) -> FollowedBy<Self, H2>
    where
        Self: Sized,
        H2: EventHandler<Context>,
    {
        FollowedBy::new(self, after)
    }

    fn followed_by_eff<F, R>(self, eff: F) -> FollowedBy<Self, SideEffect<F>>
    where
        Self: Sized,
        F: FnOnce() -> R,
    {
        FollowedBy::new(self, eff.into())
    }
}

impl<'a, H, Context> EventHandler<Context> for &'a mut H
where
    H: EventHandler<Context>,
{
    type Completion = H::Completion;

    fn step(&mut self, meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion> {
        (*self).step(meta, context)
    }
}

#[derive(Debug, Error)]
pub enum EventHandlerError {
    #[error("Event handler stepped after completion.")]
    SteppedAfterComplete,
    #[error("Invalid incoming message: {0}")]
    BadCommand(AsyncParseError),
}

pub enum StepResult<C> {
    Continue {
        modified_lane: Option<u64>,
    },
    Fail(EventHandlerError),
    Complete{
        modified_lane: Option<u64>,
        result: C,
    },
}

impl<C> StepResult<C> {

    pub fn cont() -> Self {
        StepResult::Continue { modified_lane: None }
    }

    pub fn done(result: C) -> Self {
        Self::Complete { modified_lane: None, result }
    }

    pub fn after_done() -> Self {
        StepResult::Fail(EventHandlerError::SteppedAfterComplete)
    }

    fn is_cont(&self) -> bool {
        matches!(self, StepResult::Continue { .. })
    }

}

pub struct SideEffect<F>(Option<F>);
pub struct SideEffects<I: Iterator> {
    eff: I,
    results: Vec<I::Item>,
    done: bool
}

impl<F> From<F> for SideEffect<F> {
    fn from(f: F) -> Self {
        SideEffect(Some(f))
    }
}

impl<T: IntoIterator> From<T> for SideEffects<T::IntoIter> {
    fn from(it: T) -> Self {
        SideEffects { eff: it.into_iter(), results: vec![], done: false }
    }
}

impl<Context, F, R> EventHandler<Context> for SideEffect<F>
where
    F: FnOnce() -> R,
{
    type Completion = R;

    fn step(&mut self, _meta: AgentMetadata, _context: &Context) -> StepResult<Self::Completion> {
        if let Some(f) = self.0.take() {
            StepResult::done(f())
        } else {
            StepResult::after_done()
        }
    }
}

impl<Context, I> EventHandler<Context> for SideEffects<I>
where
    I: Iterator,
{
    type Completion = Vec<I::Item>;

    fn step(&mut self, _meta: AgentMetadata, _context: &Context) -> StepResult<Self::Completion> {
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

pub enum AndThen<H1, H2, F> {
    First {
        first: H1,
        next: F,
    },
    Second(H2),
    Done
}


pub enum FollowedBy<H1, H2> {
    First {
        first: H1,
        next: H2,
    },
    Second(H2),
    Done
}

impl<H1, H2, F> Default for AndThen<H1, H2, F> {
    fn default() -> Self {
        AndThen::Done
    }
}

impl<H1, H2> Default for FollowedBy<H1, H2> {
    fn default() -> Self {
        FollowedBy::Done
    }
}


impl<H1, H2, F> AndThen<H1, H2, F> {

    fn new(first: H1, f: F) -> Self {
        AndThen::First { first, next: f }
    }

}

impl<H1, H2> FollowedBy<H1, H2> {

    fn new(first: H1, second: H2) -> Self {
        FollowedBy::First { first, next: second }
    }

}

impl<Context, H1, H2, F> EventHandler<Context> for AndThen<H1, H2, F>
where
    H1: EventHandler<Context>,
    H2: EventHandler<Context>,
    F: FnOnce(H1::Completion) -> Result<H2, EventHandlerError>,
{
    type Completion = H2::Completion;

    fn step(&mut self, meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            AndThen::First { mut first, next } => {
                match first.step(meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete { modified_lane: dirty_lane, result } => {
                        match next(result) {
                            Ok(second) => {
                                *self = AndThen::Second(second);
                                StepResult::Continue { modified_lane: dirty_lane }
                            }
                            Err(e) => StepResult::Fail(e),
                        }
                    }
                    StepResult::Continue { modified_lane: dirty_lane } => {
                        *self = AndThen::First { first, next };
                        StepResult::Continue { modified_lane: dirty_lane }
                    }
                }
            },
            AndThen::Second(mut second) => {
                let step_result = second.step(meta, context);
                if step_result.is_cont() {
                    *self = AndThen::Second(second);
                }
                step_result
            },
            _ => StepResult::after_done(),
        }
    }
}

impl<Context, H1, H2> EventHandler<Context> for FollowedBy<H1, H2>
where
    H1: EventHandler<Context>,
    H2: EventHandler<Context>,
{
    type Completion = H2::Completion;

    fn step(&mut self, meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion> {
        match std::mem::take(self) {
            FollowedBy::First { mut first, next } => {
                match first.step(meta, context) {
                    StepResult::Fail(e) => StepResult::Fail(e),
                    StepResult::Complete { modified_lane: dirty_lane, .. } => {
                        *self = FollowedBy::Second(next);
                        StepResult::Continue { modified_lane: dirty_lane }
                    }
                    StepResult::Continue { modified_lane: dirty_lane } => {
                        *self = FollowedBy::First { first, next };
                        StepResult::Continue { modified_lane: dirty_lane }
                    }
                }
            },
            FollowedBy::Second(mut second) => {
                let step_result = second.step(meta, context);
                if step_result.is_cont() {
                    *self = FollowedBy::Second(second);
                }
                step_result
            },
            _ => StepResult::after_done(),
        }
    }
}

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

impl<T, Context> EventHandler<Context> for ConstHandler<T> {
    type Completion = T;

    fn step(&mut self, _meta: AgentMetadata, _context: &Context) -> StepResult<Self::Completion> {
        if let Some(value) = self.0.take() {
            StepResult::done(value)
        } else {
            StepResult::after_done()
        }
    }
}

impl<Context> EventHandler<Context> for CNil {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, _context: &Context) -> StepResult<Self::Completion> {
        match *self {}
    }
}

impl<H, T, Context> EventHandler<Context> for Coproduct<H, T>
where
    H: EventHandler<Context, Completion = ()>,
    T: EventHandler<Context, Completion = ()>,
{
    type Completion = ();

    fn step(&mut self, meta: AgentMetadata, context: &Context) -> StepResult<Self::Completion> {
        match self {
            Coproduct::Inl(h) => h.step(meta, context),
            Coproduct::Inr(t) => t.step(meta, context),
        }
    }
}

pub struct GetAgentUri;

impl<Context> EventHandler<Context> for GetAgentUri {
    type Completion = RelativeUri;

    fn step(&mut self, meta: AgentMetadata, _context: &Context) -> StepResult<Self::Completion> {
        StepResult::done(meta.agent_uri().clone())
    }
}
