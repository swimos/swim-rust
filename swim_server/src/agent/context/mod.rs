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

use crate::agent::{AgentContext, Eff};
use crate::meta::log::NodeLogger;
use crate::meta::MetaContext;
use crate::scheduler::SchedulerContext;
use futures::future::BoxFuture;
use futures::Stream;
use server_store::agent::NodeStore;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use swim_async_runtime::time::clock::Clock;
use swim_client::interface::ClientContext;
use swim_metrics::NodeMetricAggregator;
use swim_model::path::Path;
use swim_runtime::routing::TaggedRouter;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::time::AtomicInstant;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// [`AgentContext`] implementation that dispatches effects to the scheduler through an MPSC
/// channel.
#[derive(Debug)]
pub(super) struct ContextImpl<Agent, Clk, Store> {
    agent_ref: Arc<Agent>,
    routing_context: RoutingContext,
    schedule_context: SchedulerContext<Clk>,
    meta_context: Arc<MetaContext>,
    client_context: ClientContext<Path>,
    agent_uri: RelativeUri,
    pub(crate) uplinks_idle_since: Arc<AtomicInstant>,
    store: Store,
}

impl<Agent, Clk, Store> ContextImpl<Agent, Clk, Store> {
    pub(super) fn new(
        agent_ref: Arc<Agent>,
        routing_context: RoutingContext,
        schedule_context: SchedulerContext<Clk>,
        meta_context: MetaContext,
        client_context: ClientContext<Path>,
        agent_uri: RelativeUri,
        store: Store,
    ) -> Self {
        ContextImpl {
            agent_ref,
            routing_context,
            schedule_context,
            meta_context: Arc::new(meta_context),
            client_context,
            agent_uri,
            uplinks_idle_since: Arc::new(AtomicInstant::new(Instant::now().into_std())),
            store,
        }
    }
}

impl<Agent, Clk, Store> Clone for ContextImpl<Agent, Clk, Store>
where
    Clk: Clone,
    Store: Clone,
{
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            routing_context: self.routing_context.clone(),
            schedule_context: self.schedule_context.clone(),
            client_context: self.client_context.clone(),
            meta_context: self.meta_context.clone(),
            agent_uri: self.agent_uri.clone(),
            uplinks_idle_since: self.uplinks_idle_since.clone(),
            store: self.store.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct RoutingContext {
    uri: RelativeUri,
    router: TaggedRouter,
    parameters: HashMap<String, String>,
}

impl RoutingContext {
    pub(super) fn new(
        uri: RelativeUri,
        router: TaggedRouter,
        parameters: HashMap<String, String>,
    ) -> Self {
        RoutingContext {
            uri,
            router,
            parameters,
        }
    }
}

impl Clone for RoutingContext {
    fn clone(&self) -> Self {
        RoutingContext {
            uri: self.uri.clone(),
            router: self.router.clone(),
            parameters: self.parameters.clone(),
        }
    }
}

impl<Agent, Clk, Store> AgentContext<Agent> for ContextImpl<Agent, Clk, Store>
where
    Agent: Send + Sync + 'static,
    Clk: Clock,
    Store: NodeStore,
{
    fn downlinks_context(&self) -> ClientContext<Path> {
        self.client_context.clone()
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
        self.schedule_context.stop_rx()
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
    type Store: NodeStore;

    /// Create a handle to the envelope router for the agent.
    fn router_handle(&self) -> TaggedRouter;

    /// Provide a channel to dispatch events to the agent scheduler.
    fn spawner(&self) -> mpsc::Sender<Eff>;

    /// Return the relative uri of this agent.
    fn uri(&self) -> &RelativeUri;

    /// Provides an observer factory that can be used to create observers that register events that
    /// happen on nodes, lanes, and uplinks.
    fn metrics(&self) -> NodeMetricAggregator;

    /// Return the time since the last outgoing message.
    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant>;

    /// Provides a handle to the store engine for this node.
    fn store(&self) -> Self::Store;
}

impl<Agent, Clk, Store> AgentExecutionContext for ContextImpl<Agent, Clk, Store>
where
    Store: NodeStore,
{
    type Store = Store;

    fn router_handle(&self) -> TaggedRouter {
        self.routing_context.router.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.schedule_context.schedule_tx()
    }

    fn uri(&self) -> &RelativeUri {
        &self.agent_uri
    }

    fn metrics(&self) -> NodeMetricAggregator {
        self.meta_context.metrics()
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.uplinks_idle_since
    }

    fn store(&self) -> Store {
        self.store.clone()
    }
}
