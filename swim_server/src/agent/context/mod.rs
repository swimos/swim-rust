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
use crate::meta::metric::NodeMetricAggregator;
use crate::meta::MetaContext;
use crate::routing::ServerRouter;
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
pub(super) struct ContextImpl<Agent, Clk, Router> {
    agent_ref: Arc<Agent>,
    routing_context: RoutingContext<Router>,
    schedule_context: SchedulerContext<Clk>,
    meta_context: Arc<MetaContext>,
}

const SCHEDULE: &str = "Schedule";
const SCHED_TRIGGERED: &str = "Schedule triggered";
const SCHED_STOPPED: &str = "Scheduler unexpectedly stopped";
const WAITING: &str = "Schedule waiting";

impl<Agent, Clk, Router> ContextImpl<Agent, Clk, Router> {
    pub(super) fn new(
        agent_ref: Arc<Agent>,
        routing_context: RoutingContext<Router>,
        schedule_context: SchedulerContext<Clk>,
        meta_context: MetaContext,
    ) -> Self {
        ContextImpl {
            agent_ref,
            routing_context,
            schedule_context,
            meta_context: Arc::new(meta_context),
        }
    }
}

impl<Agent, Clk, Router> Clone for ContextImpl<Agent, Clk, Router>
where
    Clk: Clone,
    Router: Clone,
{
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            routing_context: self.routing_context.clone(),
            schedule_context: self.schedule_context.clone(),
            meta_context: self.meta_context.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct RoutingContext<Router> {
    uri: RelativeUri,
    router: Router,
    parameters: HashMap<String, String>,
}

impl<Router> RoutingContext<Router> {
    pub(super) fn new(
        uri: RelativeUri,
        router: Router,
        parameters: HashMap<String, String>,
    ) -> Self {
        RoutingContext {
            uri,
            router,
            parameters,
        }
    }
}

impl<Router: Clone> Clone for RoutingContext<Router> {
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

impl<Agent, Clk, Router> AgentContext<Agent> for ContextImpl<Agent, Clk, Router>
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
    type Router: ServerRouter + 'static;

    /// Create a handle to the envelope router for the agent.
    fn router_handle(&self) -> Self::Router;

    /// Provide a channel to dispatch events to the agent scheduler.
    fn spawner(&self) -> mpsc::Sender<Eff>;

    /// Provides an observer factory that can be used to create observers that register events that
    /// happen on nodes, lanes, and uplinks.
    fn metrics(&self) -> NodeMetricAggregator;
}

impl<Agent, Clk, RouterInner> AgentExecutionContext for ContextImpl<Agent, Clk, RouterInner>
where
    RouterInner: ServerRouter + Clone + 'static,
{
    type Router = RouterInner;

    fn router_handle(&self) -> Self::Router {
        self.routing_context.router.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.schedule_context.scheduler.clone()
    }

    fn metrics(&self) -> NodeMetricAggregator {
        self.meta_context.metrics()
    }
}
