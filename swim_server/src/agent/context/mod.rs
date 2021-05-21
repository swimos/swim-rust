// Copyright 2015-2021 SWIM.AI inc.
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
use crate::meta::log::NodeLogger;
use crate::meta::MetaContext;
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use swim_client::interface::SwimClient;
use swim_common::routing::Router;
use swim_common::warp::path::Path;
use swim_runtime::time::clock::Clock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::Duration;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::future::SwimStreamExt;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

#[cfg(test)]
mod tests;

/// [`AgentContext`] implementation that dispatches effects to the scheduler through an MPSC
/// channel.
#[derive(Debug)]
pub(super) struct ContextImpl<Agent, Clk, R: Router + Clone + 'static> {
    agent_ref: Arc<Agent>,
    routing_context: RoutingContext<R>,
    schedule_context: SchedulerContext<Clk>,
    meta_context: Arc<MetaContext>,
    client: SwimClient<Path>,
    agent_uri: RelativeUri,
}

const SCHEDULE: &str = "Schedule";
const SCHED_TRIGGERED: &str = "Schedule triggered";
const SCHED_STOPPED: &str = "Scheduler unexpectedly stopped";
const WAITING: &str = "Schedule waiting";

impl<Agent, Clk, R: Router + Clone + 'static> ContextImpl<Agent, Clk, R> {
    pub(super) fn new(
        agent_ref: Arc<Agent>,
        routing_context: RoutingContext<R>,
        schedule_context: SchedulerContext<Clk>,
        meta_context: MetaContext,
        client: SwimClient<Path>,
        agent_uri: RelativeUri,
    ) -> Self {
        ContextImpl {
            agent_ref,
            routing_context,
            schedule_context,
            meta_context: Arc::new(meta_context),
            client,
            agent_uri,
        }
    }
}

impl<Agent, Clk, R: Router + Clone + 'static> Clone for ContextImpl<Agent, Clk, R>
where
    Clk: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            routing_context: self.routing_context.clone(),
            schedule_context: self.schedule_context.clone(),
            client: self.client.clone(),
            meta_context: self.meta_context.clone(),
            agent_uri: self.agent_uri.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct RoutingContext<R: Router + Clone + 'static> {
    uri: RelativeUri,
    router: R,
    parameters: HashMap<String, String>,
}

impl<R: Router + Clone + 'static> RoutingContext<R> {
    pub(super) fn new(uri: RelativeUri, router: R, parameters: HashMap<String, String>) -> Self {
        RoutingContext {
            uri,
            router,
            parameters,
        }
    }
}

impl<R: Router + Clone + 'static> Clone for RoutingContext<R> {
    fn clone(&self) -> Self {
        RoutingContext {
            uri: self.uri.clone(),
            router: self.router.clone(),
            parameters: self.parameters.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct SchedulerContext<Clk> {
    scheduler: mpsc::Sender<Eff>,
    schedule_count: Arc<AtomicU64>,
    clock: Clk,
    stop_signal: trigger::Receiver,
}

impl<Clk: Clock> SchedulerContext<Clk> {
    pub(super) fn new(scheduler: Sender<Eff>, clock: Clk, stop_signal: trigger::Receiver) -> Self {
        SchedulerContext {
            scheduler,
            schedule_count: Default::default(),
            clock,
            stop_signal,
        }
    }

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

        let sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            if sender.send(schedule_effect).await.is_err() {
                event!(Level::ERROR, SCHED_STOPPED)
            }
        })
    }
}

impl<Clk: Clone> Clone for SchedulerContext<Clk> {
    fn clone(&self) -> Self {
        SchedulerContext {
            scheduler: self.scheduler.clone(),
            schedule_count: self.schedule_count.clone(),
            clock: self.clock.clone(),
            stop_signal: self.stop_signal.clone(),
        }
    }
}

impl<Agent, Clk, R: Router + Clone + 'static> AgentContext<Agent> for ContextImpl<Agent, Clk, R>
where
    Agent: Send + Sync + 'static,
    Clk: Clock,
{
    type LocalRouter = R;

    fn client(&self) -> SwimClient<Path> {
        self.client.clone()
    }

    fn local_router(&self) -> Self::LocalRouter {
        self.routing_context.router.clone()
    }

    fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        self.schedule_context.schedule(effects, schedule)
    }

    fn agent(&self) -> &Agent {
        self.agent_ref.as_ref()
    }

    fn node_uri(&self) -> &RelativeUri {
        &self.routing_context.uri
    }

    fn agent_stop_event(&self) -> trigger::Receiver {
        self.schedule_context.stop_signal.clone()
    }

    fn parameter(&self, key: &str) -> Option<&String> {
        self.routing_context.parameters.get(key)
    }

    fn parameters(&self) -> HashMap<String, String> {
        self.routing_context.parameters.clone()
    }

    fn logger(&self) -> NodeLogger {
        self.meta_context.node_logger()
    }
}

/// A context, scoped to an agent, to provide shared functionality to each of its lanes.
pub trait AgentExecutionContext {
    type Router: Router + 'static;

    /// Create a handle to the envelope router for the agent.
    fn router_handle(&self) -> Self::Router;

    /// Provide a channel to dispatch events to the agent scheduler.
    fn spawner(&self) -> mpsc::Sender<Eff>;

    /// Return the relative uri of this agent.
    fn uri(&self) -> &RelativeUri;
}

impl<Agent, Clk, RouterInner> AgentExecutionContext for ContextImpl<Agent, Clk, RouterInner>
where
    RouterInner: Router + Clone + 'static,
{
    type Router = RouterInner;

    fn router_handle(&self) -> Self::Router {
        self.routing_context.router.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.schedule_context.scheduler.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.agent_uri
    }
}
