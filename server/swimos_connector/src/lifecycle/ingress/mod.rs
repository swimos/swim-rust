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

use swimos_agent::{
    agent_lifecycle::{
        item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop,
        on_timer::OnTimer, HandlerContext,
    },
    event_handler::{
        ActionContext, EventHandler, HandlerActionExt, TryHandlerActionExt, UnitHandler,
    },
    AgentMetadata,
};
use swimos_utilities::trigger;

use crate::{
    connector::suspend_connector, error::ConnectorInitError, ConnectorAgent, IngressConnector,
};

#[cfg(test)]
mod tests;

/// An [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) implementation that serves as an adapter for
/// an [ingress connector](IngressConnector).
#[derive(Debug, Clone, Copy)]
pub struct IngressConnectorLifecycle<C>(C);

impl<C> IngressConnectorLifecycle<C> {
    pub fn new(connector: C) -> Self {
        IngressConnectorLifecycle(connector)
    }
}

impl<C> OnInit<ConnectorAgent> for IngressConnectorLifecycle<C>
where
    C: IngressConnector + Send,
{
    fn initialize(
        &self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) {
    }
}

impl<C> OnStart<ConnectorAgent> for IngressConnectorLifecycle<C>
where
    C: IngressConnector + Send,
{
    fn on_start(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        let IngressConnectorLifecycle(connector) = self;
        let handler_context: HandlerContext<ConnectorAgent> = HandlerContext::default();
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

impl<C> OnStop<ConnectorAgent> for IngressConnectorLifecycle<C>
where
    C: IngressConnector + Send,
{
    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        self.0.on_stop()
    }
}

impl<C> OnTimer<ConnectorAgent> for IngressConnectorLifecycle<C>
where
    C: IngressConnector + Send,
{
    fn on_timer(&self, _timer_id: u64) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<C> ItemEvent<ConnectorAgent> for IngressConnectorLifecycle<C>
where
    C: IngressConnector,
{
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &ConnectorAgent,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}
