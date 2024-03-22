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

use std::time::Duration;

use futures::{
    future::{BoxFuture, Either},
    stream::FuturesUnordered,
    Future, FutureExt, Stream, StreamExt,
};
use static_assertions::assert_obj_safe;

use crate::meta::AgentMetadata;

use super::{
    ActionContext, BoxEventHandler, EventHandler, HandlerAction, HandlerActionExt, StepResult,
    UnitHandler,
};

#[cfg(test)]
mod tests;

pub type HandlerFuture<Context> = BoxFuture<'static, BoxEventHandler<'static, Context>>;

/// Trait for suspend handler futures into the task for an agent.
pub trait Spawner<Context> {
    /// Suspend a future and hand it over to the task running the agent. The future will
    /// result in an event handler that will be executed by the agent task after the
    /// future completes.
    fn spawn_suspend(&self, fut: HandlerFuture<Context>);
}

impl<F, Context> Spawner<Context> for F
where
    F: Fn(HandlerFuture<Context>),
{
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self(fut)
    }
}

assert_obj_safe!(Spawner<()>);

impl<Context> Spawner<Context> for FuturesUnordered<HandlerFuture<Context>> {
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self.push(fut);
    }
}

/// A handler action that will suspend a future into the agent task.
pub struct Suspend<Fut> {
    future: Option<Fut>,
}

impl<F> Suspend<F> {
    /// #Arguments
    /// * `future` - The future to be suspended.
    pub fn new(future: F) -> Self {
        Suspend {
            future: Some(future),
        }
    }
}

impl<Context, Fut, H> HandlerAction<Context> for Suspend<Fut>
where
    Fut: Future<Output = H> + Send + 'static,
    H: EventHandler<Context> + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let Suspend { future } = self;
        if let Some(future) = future.take() {
            action_context.spawn_suspend(
                future
                    .map(|h| {
                        let boxed: BoxEventHandler<Context> = Box::new(h);
                        boxed
                    })
                    .boxed(),
            );
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

pub fn run_after<Context, H>(
    delay: Duration,
    handler: H,
) -> impl EventHandler<Context> + Send + 'static
where
    H: EventHandler<Context> + Send + 'static,
{
    let fut = tokio::time::sleep(delay).map(move |_| handler);
    Suspend::new(fut)
}

pub fn run_schedule<Context, I, H>(schedule: I) -> impl EventHandler<Context> + Send + 'static
where
    Context: 'static,
    I: IntoIterator<Item = (Duration, H)>,
    I::IntoIter: Send + 'static,
    H: EventHandler<Context> + Send + 'static,
{
    let mut it = schedule.into_iter();
    if let Some((delay, handler)) = it.next() {
        let sched_handler = handler.and_then(move |_| {
            let h: Box<dyn EventHandler<Context> + Send> = Box::new(run_schedule(it));
            h
        });
        Either::Left(run_after(delay, sched_handler))
    } else {
        Either::Right(UnitHandler::default())
    }
}

pub fn run_schedule_async<Context, S, H>(
    mut schedule: S,
) -> impl EventHandler<Context> + Send + 'static
where
    Context: 'static,
    S: Stream<Item = H> + Send + Unpin + 'static,
    H: EventHandler<Context> + Send + 'static,
{
    Suspend::new(async move {
        match schedule.next().await {
            Some(h) => Either::Left(run_schedule_async(schedule).boxed().followed_by(h)),
            None => Either::Right(UnitHandler::default()),
        }
    })
}
