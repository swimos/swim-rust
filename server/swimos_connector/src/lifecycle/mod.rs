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

use std::{cell::OnceCell, collections::HashMap};

use bitflags::bitflags;
use futures::{FutureExt, TryFutureExt};
use swimos_agent::{
    agent_lifecycle::{
        item_event::{dynamic_handler, DynamicHandler, DynamicLifecycle, ItemEvent},
        on_init::OnInit,
        on_start::OnStart,
        on_stop::OnStop,
        on_timer::OnTimer,
        HandlerContext,
    },
    event_handler::{
        ActionContext, BoxHandlerAction, EventHandler, HandlerActionExt, TryHandlerActionExt,
        UnitHandler,
    },
    lanes::map::MapLaneEvent,
    AgentMetadata,
};
use swimos_model::Value;
use swimos_utilities::trigger;

use crate::{
    connector::{suspend_connector, EgressConnector, EgressConnectorSender},
    error::ConnectorInitError,
    Connector, ConnectorAgent,
};

/// An [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) implementation that serves as an adapter for
/// a [connector](Connector).
#[derive(Debug, Clone, Copy)]
pub struct ConnectorLifecycle<C>(C);

impl<C> ConnectorLifecycle<C> {
    pub fn new(connector: C) -> Self {
        ConnectorLifecycle(connector)
    }
}

impl<C> OnInit<ConnectorAgent> for ConnectorLifecycle<C>
where
    C: Connector + Send,
{
    fn initialize(
        &self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) {
    }
}

impl<C> OnStart<ConnectorAgent> for ConnectorLifecycle<C>
where
    C: Connector + Send,
{
    fn on_start(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        let ConnectorLifecycle(connector) = self;
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

impl<C> OnStop<ConnectorAgent> for ConnectorLifecycle<C>
where
    C: Connector + Send,
{
    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        self.0.on_stop()
    }
}

impl<C> OnTimer<ConnectorAgent> for ConnectorLifecycle<C>
where
    C: Connector + Send,
{
    fn on_timer(&self, _timer_id: u64) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

impl<C> ItemEvent<ConnectorAgent> for ConnectorLifecycle<C>
where
    C: Connector,
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

pub struct EgressConnectorLifecycle<C: EgressConnector> {
    lifecycle: C,
    sender: OnceCell<C::Sender>,
}

impl<C> OnInit<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector + Send,
{
    fn initialize(
        &self,
        _action_context: &mut ActionContext<ConnectorAgent>,
        _meta: AgentMetadata,
        _context: &ConnectorAgent,
    ) {
    }
}

impl<C> OnStop<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector + Send,
{
    fn on_stop(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        UnitHandler::default()
    }
}

bitflags! {

    #[derive(Default, Debug, Copy, Clone)]
    pub struct EgressFlags: u64 {
        /// Initialization has completed.
        const INITIALIZED = 0b1;
    }

}

impl<C> OnStart<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector + Send,
{
    fn on_start(&self) -> impl EventHandler<ConnectorAgent> + '_ {
        let EgressConnectorLifecycle { lifecycle, sender } = self;
        let (tx, rx) = trigger::trigger();
        let context: HandlerContext<ConnectorAgent> = Default::default();
        let on_start = lifecycle.on_start(tx);
        let create_sender = context
            .with_parameters(|params| lifecycle.make_sender(params))
            .try_handler()
            .and_then(move |egress_sender| {
                context.effect(move || {
                    let _ = sender.set(egress_sender);
                })
            });
        let await_init = context.suspend(async move {
            context
                .value(rx.await)
                .try_handler()
                .followed_by(ConnectorAgent::set_flags(EgressFlags::INITIALIZED.bits()))
        });
        await_init.followed_by(on_start).followed_by(create_sender)
    }
}

fn is_initialized(agent: &ConnectorAgent) -> bool {
    EgressFlags::from_bits_retain(agent.read_flags()).contains(EgressFlags::INITIALIZED)
}

impl<C> ItemEvent<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector,
{
    type ItemEventHandler<'a> = DynamicHandler<'a, ConnectorAgent, Self>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &ConnectorAgent,
        item_name: &'a str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        if is_initialized(context) {
            dynamic_handler(context, self, item_name)
        } else {
            None
        }
    }
}

impl<C> DynamicLifecycle<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector,
{
    type ValueHandler<'a> = BoxHandlerAction<'static, ConnectorAgent, ()>
    where
        Self: 'a;

    type MapHandler<'a> = BoxHandlerAction<'static, ConnectorAgent, ()>
    where
        Self: 'a;

    fn value_lane<'a>(
        &'a self,
        lane_name: &'a str,
        _previous: Value,
        value: &Value,
    ) -> Self::ValueHandler<'a> {
        let EgressConnectorLifecycle { sender, .. } = self;
        if let Some(sender) = sender.get() {
            let context: HandlerContext<ConnectorAgent> = Default::default();
            let fut = sender
                .send(lane_name, None, value)
                .into_future()
                .map(move |h: Result<_, _>| context.value(h).try_handler().and_then(|h| h));
            Some(context.suspend(fut))
        } else {
            None
        }
        .discard()
        .boxed()
    }

    fn map_lane<'a>(
        &'a self,
        lane_name: &'a str,
        event: MapLaneEvent<Value, Value>,
        contents: &HashMap<Value, Value>,
    ) -> Self::MapHandler<'a> {
        let EgressConnectorLifecycle { sender, .. } = self;
        let kv = match &event {
            MapLaneEvent::Clear(_) => None,
            MapLaneEvent::Update(k, _) => {
                let v = contents.get(k);
                Some((k, v))
            }
            MapLaneEvent::Remove(k, _) => Some((k, None)),
        };
        sender
            .get()
            .and_then(|sender| {
                let context: HandlerContext<ConnectorAgent> = Default::default();
                kv.map(|(k, v)| {
                    let fut = sender
                        .send(lane_name, Some(k), v.unwrap_or(&Value::Extant))
                        .into_future()
                        .map(move |h: Result<_, _>| context.value(h).try_handler().and_then(|h| h));
                    context.suspend(fut)
                })
            })
            .discard()
            .boxed()
    }
}
