// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::{AgentContext, Eff};
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::{FutureExt, Stream, StreamExt};
use std::future::Future;
use std::sync::Arc;
use swim_runtime::time::clock::Clock;
use tokio::sync::mpsc;
use tokio::time::Duration;
use url::Url;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;

#[cfg(test)]
mod tests;

/// [`AgentContext`] implementation that dispatches effects to the scheduler through an MPSC
/// channel.
#[derive(Debug)]
pub(super) struct ContextImpl<Agent, Clk> {
    agent_ref: Arc<Agent>,
    url: Url,
    scheduler: mpsc::Sender<Eff>,
    clock: Clk,
    stop_signal: trigger::Receiver,
}

impl<Agent, Clk: Clone> Clone for ContextImpl<Agent, Clk> {
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            url: self.url.clone(),
            scheduler: self.scheduler.clone(),
            clock: self.clock.clone(),
            stop_signal: self.stop_signal.clone(),
        }
    }
}

impl<Agent, Clk> ContextImpl<Agent, Clk> {
    pub(super) fn new(
        agent_ref: Arc<Agent>,
        url: Url,
        scheduler: mpsc::Sender<Eff>,
        clock: Clk,
        stop_signal: trigger::Receiver,
    ) -> Self {
        ContextImpl {
            agent_ref,
            url,
            scheduler,
            clock,
            stop_signal,
        }
    }
}

impl<Agent, Clk> AgentContext<Agent> for ContextImpl<Agent, Clk>
where
    Agent: Send + Sync + 'static,
    Clk: Clock,
{
    fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        let clock = self.clock.clone();
        let schedule_effect = schedule
            .zip(effects)
            .then(move |(dur, eff)| {
                let delay_fut = clock.delay(dur);
                async move {
                    delay_fut.await;
                    eff.await;
                }
            })
            .take_until_completes(self.stop_signal.clone())
            .never_error()
            .forward(drain())
            .map(|_| ()) //Never is an empty type so we can drop the errors.
            .boxed();

        let mut sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            let _ = sender.send(schedule_effect).await;
        })
    }

    fn agent(&self) -> &Agent {
        self.agent_ref.as_ref()
    }

    fn node_url(&self) -> &Url {
        &self.url
    }

    fn agent_stop_event(&self) -> trigger::Receiver {
        self.stop_signal.clone()
    }
}
