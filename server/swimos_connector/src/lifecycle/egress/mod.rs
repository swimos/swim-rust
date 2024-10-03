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
    config::SimpleDownlinkConfig,
    event_handler::{
        ActionContext, EventHandler, HandlerActionExt, LocalBoxHandlerAction, Sequentially,
        TryHandlerActionExt, UnitHandler,
    },
    lanes::map::MapLaneEvent,
    AgentMetadata,
};
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_model::Value;
use swimos_utilities::trigger;

use crate::{
    connector::{EgressConnector, EgressConnectorSender},
    error::ConnectorInitError,
    ConnectorAgent, InitializationContext, MessageSource, SendResult,
};

#[cfg(test)]
mod tests;

/// An [agent lifecycle](swimos_agent::agent_lifecycle::AgentLifecycle) implementation that serves as an adapter for
/// an [egress connector](EgressConnector).
pub struct EgressConnectorLifecycle<C: EgressConnector> {
    lifecycle: C,
    sender: OnceCell<C::Sender>,
}

impl<C: EgressConnector> EgressConnectorLifecycle<C> {
    pub fn new(lifecycle: C) -> Self {
        EgressConnectorLifecycle {
            lifecycle,
            sender: OnceCell::new(),
        }
    }
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
        self.lifecycle.on_stop()
    }
}

bitflags! {

    #[derive(Default, Debug, Copy, Clone)]
    struct EgressFlags: u64 {
        /// Initialization has completed.
        const INITIALIZED = 0b1;
    }

}

#[derive(Default, Debug)]
struct DownlinkCollector {
    value_downlinks: Vec<Address<String>>,
    map_downlinks: Vec<Address<String>>,
}

impl InitializationContext for DownlinkCollector {
    fn open_event_downlink(&mut self, address: Address<String>) {
        self.value_downlinks.push(address);
    }

    fn open_map_downlink(&mut self, address: Address<String>) {
        self.map_downlinks.push(address);
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
        let mut collector = DownlinkCollector::default();
        let on_start = lifecycle.on_start(tx);
        lifecycle.open_downlinks(&mut collector);
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
        let open_downlinks = self.open_downlinks(collector);
        await_init
            .followed_by(on_start)
            .followed_by(create_sender)
            .followed_by(open_downlinks)
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
    type ValueHandler<'a> = LocalBoxHandlerAction<'a, ConnectorAgent, ()>
    where
        Self: 'a;

    type MapHandler<'a> = LocalBoxHandlerAction<'a, ConnectorAgent, ()>
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
            handle_value_event(sender, MessageSource::Lane(lane_name), context, value)
        } else {
            UnitHandler::default().boxed_local()
        }
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
        if let (Some(sender), Some((k, v))) = (sender.get(), kv) {
            let context: HandlerContext<ConnectorAgent> = Default::default();
            handle_map_event(sender, MessageSource::Lane(lane_name), context, k, v)
        } else {
            UnitHandler::default().boxed_local()
        }
    }
}

impl<C> OnTimer<ConnectorAgent> for EgressConnectorLifecycle<C>
where
    C: EgressConnector + Send,
{
    fn on_timer(&self, timer_id: u64) -> impl EventHandler<ConnectorAgent> + '_ {
        let EgressConnectorLifecycle { sender, .. } = self;
        if let Some(sender) = sender.get() {
            let context: HandlerContext<ConnectorAgent> = Default::default();
            match sender.timer_event(timer_id) {
                Some(SendResult::Suspend(f)) => {
                    let fut = f
                        .into_future()
                        .map(move |h: Result<_, _>| context.value(h).try_handler().and_then(|h| h));
                    context.suspend(fut).boxed_local()
                }
                Some(SendResult::RequestCallback(delay, id)) => {
                    context.schedule_timer_event(delay, id).boxed_local()
                }
                _ => UnitHandler::default().boxed_local(),
            }
        } else {
            UnitHandler::default().boxed_local()
        }
    }
}

impl<C> EgressConnectorLifecycle<C>
where
    C: EgressConnector,
{
    fn open_downlinks(
        &self,
        collector: DownlinkCollector,
    ) -> impl EventHandler<ConnectorAgent> + '_ {
        let EgressConnectorLifecycle { sender, .. } = self;
        let context: HandlerContext<ConnectorAgent> = Default::default();
        context
            .effect(|| sender.get().ok_or(ConnectorInitError))
            .try_handler()
            .and_then(move |sender: &C::Sender| {
                let DownlinkCollector {
                    value_downlinks,
                    map_downlinks,
                } = collector;
                let mut open_value_dls = vec![];
                let mut open_map_dls = vec![];
                for address in value_downlinks {
                    let open_dl = context
                        .event_downlink_builder::<Value>(
                            address.host.as_deref(),
                            &address.node,
                            &address.lane,
                            SimpleDownlinkConfig::default(),
                        )
                        .with_shared_state((address, sender.clone()))
                        .on_event(handle_value_event_dl)
                        .done()
                        .discard();
                    open_value_dls.push(open_dl);
                }
                for address in map_downlinks {
                    let open_dl = context
                        .map_event_downlink_builder::<Value, Value>(
                            address.host.as_deref(),
                            &address.node,
                            &address.lane,
                            SimpleDownlinkConfig::default(),
                        )
                        .with_shared_state((address, sender.clone()))
                        .on_event(handle_map_event_dl)
                        .done()
                        .discard();

                    open_map_dls.push(open_dl);
                }
                Sequentially::new(open_value_dls).followed_by(Sequentially::new(open_map_dls))
            })
    }
}

fn handle_value_event_dl<S, E>(
    state: &(Address<String>, S),
    context: HandlerContext<ConnectorAgent>,
    value: Value,
) -> impl EventHandler<ConnectorAgent> + '_
where
    S: EgressConnectorSender<E>,
    E: std::error::Error + Send + 'static,
{
    let (addr, sender) = state;
    handle_value_event(sender, MessageSource::Downlink(addr), context, &value)
}

fn handle_map_event_dl<S, E>(
    state: &(Address<String>, S),
    context: HandlerContext<ConnectorAgent>,
    message: MapMessage<Value, Value>,
) -> impl EventHandler<ConnectorAgent> + '_
where
    S: EgressConnectorSender<E>,
    E: std::error::Error + Send + 'static,
{
    let (addr, sender) = state;
    let kv = match &message {
        MapMessage::Update { key, value } => Some((key, Some(value))),
        MapMessage::Remove { key } => Some((key, None)),
        _ => None,
    };
    kv.map(move |(k, v)| handle_map_event(sender, MessageSource::Downlink(addr), context, k, v))
        .discard()
}

fn handle_map_event<'a, S, E>(
    sender: &'a S,
    source: MessageSource<'_>,
    context: HandlerContext<ConnectorAgent>,
    key: &Value,
    value: Option<&Value>,
) -> LocalBoxHandlerAction<'a, ConnectorAgent, ()>
where
    S: EgressConnectorSender<E>,
    E: std::error::Error + Send + 'static,
{
    match sender.send(source, Some(key), value.unwrap_or(&Value::Extant)) {
        Some(SendResult::Suspend(f)) => {
            let fut = f
                .into_future()
                .map(move |h: Result<_, _>| context.value(h).try_handler().and_then(|h| h));
            context.suspend(fut).boxed_local()
        }
        Some(SendResult::RequestCallback(delay, id)) => {
            context.schedule_timer_event(delay, id).boxed_local()
        }
        _ => UnitHandler::default().boxed_local(),
    }
}

fn handle_value_event<'a, S, E>(
    sender: &'a S,
    source: MessageSource<'_>,
    context: HandlerContext<ConnectorAgent>,
    value: &Value,
) -> LocalBoxHandlerAction<'a, ConnectorAgent, ()>
where
    S: EgressConnectorSender<E>,
    E: std::error::Error + Send + 'static,
{
    match sender.send(source, None, value) {
        Some(SendResult::Suspend(f)) => {
            let fut = f
                .into_future()
                .map(move |h: Result<_, _>| context.value(h).try_handler().and_then(|h| h));
            context.suspend(fut).boxed_local()
        }
        Some(SendResult::RequestCallback(delay, id)) => {
            context.schedule_timer_event(delay, id).boxed_local()
        }
        Some(SendResult::Fail(err)) => context.fail(err).boxed_local(),
        _ => UnitHandler::default().boxed_local(),
    }
}
