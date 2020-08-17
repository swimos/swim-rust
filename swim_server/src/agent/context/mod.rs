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
use crate::routing::ServerRouter;
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::{FutureExt, Stream, StreamExt};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use swim_runtime::time::clock::Clock;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
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
    schedule_count: Arc<AtomicU64>,
    clock: Clk,
    stop_signal: trigger::Receiver,
}

const SCHEDULE: &str = "Schedule";
const SCHED_TRIGGERED: &str = "Schedule triggered";
const SCHED_STOPPED: &str = "Scheduler unexpectedly stopped";
const WAITING: &str = "Schedule waiting";

impl<Agent, Clk: Clone> Clone for ContextImpl<Agent, Clk> {
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            url: self.url.clone(),
            scheduler: self.scheduler.clone(),
            schedule_count: self.schedule_count.clone(),
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
            schedule_count: Default::default(),
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
        let index = self.schedule_count.fetch_add(1, Ordering::Relaxed);

        let clock = self.clock.clone();
        let schedule_effect = schedule
            .zip(effects)
            .then(move |(dur, eff)| {
                event!(Level::TRACE, WAITING, ?dur);
                let delay_fut = clock.delay(dur);
                async move {
                    delay_fut.await;
                    event!(Level::TRACE, SCHED_TRIGGERED);
                    eff.await;
                }
            })
            .take_until(self.stop_signal.clone())
            .never_error()
            .forward(drain())
            .map(|_| ()) //Never is an empty type so we can drop the errors.
            .instrument(span!(Level::DEBUG, SCHEDULE, index))
            .boxed();

        let mut sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            if sender.send(schedule_effect).await.is_err() {
                event!(Level::ERROR, SCHED_STOPPED)
            }
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

/// A context, scoped to an agent, to provide shared functionality to each of its lanes.
pub trait AgentExecutionContext {
    type Router: ServerRouter + 'static;

    /// Create a handle to the envelope router for the agent.
    fn router_handle(&self) -> Self::Router;

    /// Provide a channel to dispatch events to the agent scheduler.
    fn spawner(&self) -> mpsc::Sender<Eff>;
}
