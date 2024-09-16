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

#[cfg(test)]
mod tests;

use crate::connectors::suspend_connector;
use crate::{Connector, ConnectorInitError};
use swimos_agent::{
    agent_lifecycle::{
        item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop, HandlerContext,
    },
    event_handler::{
        ActionContext, EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler,
    },
    AgentMetadata,
};
use swimos_utilities::trigger;

/// An [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) implementation that serves as an adapter for
/// a [connector](Connector).
#[derive(Debug, Clone, Copy)]
pub struct ConnectorLifecycle<C>(C);

impl<C> ConnectorLifecycle<C> {
    pub fn new(connector: C) -> Self {
        ConnectorLifecycle(connector)
    }
}

impl<A, C> OnInit<A> for ConnectorLifecycle<C>
where
    C: Connector<A> + Send,
{
    fn initialize(
        &self,
        _action_context: &mut ActionContext<A>,
        _meta: AgentMetadata,
        _context: &A,
    ) {
    }
}

impl<A, C> OnStart<A> for ConnectorLifecycle<C>
where
    C: Connector<A> + Send,
    A: 'static,
{
    fn on_start(&self) -> impl EventHandler<A> + '_ {
        let ConnectorLifecycle(connector) = self;
        let handler_context: HandlerContext<A> = HandlerContext::default();
        let (tx, rx) = trigger::trigger();
        let suspend = handler_context
            .effect(|| connector.create_stream())
            .try_handler()
            .and_then(move |stream| {
                handler_context.suspend(async move {
                    handler_context
                        .value(rx.await.map_err(|_| ConnectorInitError))
                        .try_handler()
                        .followed_by(suspend_connector(stream))
                })
            });
        connector.on_start(tx).followed_by(suspend)
    }
}

impl<A, C> OnStop<A> for ConnectorLifecycle<C>
where
    C: Connector<A> + Send,
{
    fn on_stop(&self) -> impl EventHandler<A> + '_ {
        self.0.on_stop()
    }
}

impl<A, C> ItemEvent<A> for ConnectorLifecycle<C>
where
    C: Connector<A>,
{
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &A,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}
