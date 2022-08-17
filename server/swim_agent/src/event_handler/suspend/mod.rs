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

use futures::{future::BoxFuture, stream::FuturesUnordered, Future, FutureExt};
use static_assertions::assert_obj_safe;

use crate::meta::AgentMetadata;

use super::{BoxEventHandler, HandlerAction, EventHandler, StepResult};

#[cfg(test)]
mod tests;

pub type HandlerFuture<Context> = BoxFuture<'static, BoxEventHandler<'static, Context>>;

pub trait Spawner<Context> {

    fn spawn_suspend(&self, fut: HandlerFuture<Context>);

}

assert_obj_safe!(Spawner<()>);

impl<Context> Spawner<Context> for FuturesUnordered<HandlerFuture<Context>> {
    fn spawn_suspend(&self, fut: HandlerFuture<Context>) {
        self.push(fut);
    }
}

pub struct Suspend<Fut> {
    future: Option<Fut>,
}

impl<F> Suspend<F> {

    pub fn new(future: F) -> Self {
        Suspend { future: Some(future) }
    }

}

impl<Context, Fut, H> HandlerAction<Context> for Suspend<Fut>
where
    Fut: Future<Output = H> + Send +'static,
    H: EventHandler<Context> + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        suspend: &dyn Spawner<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let Suspend { future } = self;
        if let Some(future) = future.take() {
            suspend.spawn_suspend(future.map(|h| {
                let boxed: BoxEventHandler<Context> = Box::new(h);
                boxed
            }).boxed());
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}