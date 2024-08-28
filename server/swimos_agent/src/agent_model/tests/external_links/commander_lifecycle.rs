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

use std::sync::Arc;

use parking_lot::Mutex;
use swimos_api::address::Address;
use swimos_model::Text;
use swimos_utilities::trigger;

use crate::{
    agent_lifecycle::{
        item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop, AgentLifecycle,
        HandlerContext,
    },
    commander::Commander,
    event_handler::{ActionContext, EventHandler, HandlerActionExt, UnitHandler},
};

use super::empty_agent::EmptyAgent;

pub struct CommanderLifecycle {
    address: Address<Text>,
    stop_rx: Arc<Mutex<Option<trigger::Receiver>>>,
}

impl CommanderLifecycle {
    pub fn new(address: Address<Text>, stop_rx: trigger::Receiver) -> Self {
        CommanderLifecycle {
            address,
            stop_rx: Arc::new(Mutex::new(Some(stop_rx))),
        }
    }
}

impl CommanderLifecycle {
    fn take_stop_rx(&self) -> trigger::Receiver {
        self.stop_rx.lock().take().expect("Already taken.")
    }
}

static_assertions::assert_impl_one!(CommanderLifecycle: AgentLifecycle<EmptyAgent>);

impl OnInit<EmptyAgent> for CommanderLifecycle {
    fn initialize(
        &self,
        _action_context: &mut ActionContext<EmptyAgent>,
        _meta: crate::AgentMetadata,
        _context: &EmptyAgent,
    ) {
    }
}

impl OnStart<EmptyAgent> for CommanderLifecycle {
    fn on_start(&self) -> impl EventHandler<EmptyAgent> + '_ {
        let context: HandlerContext<EmptyAgent> = Default::default();

        let stop_rx = self.take_stop_rx();
        let stop_handler = context.suspend(async move {
            assert!(stop_rx.await.is_ok());
            context.stop()
        });

        let Address { host, node, lane } = &self.address;
        let create = context.create_commander(host.as_ref(), node, lane);

        let commands = create.and_then(move |commander: Commander<EmptyAgent>| {
            commander
                .send(7)
                .followed_by(context.suspend(async move { commander.send(22) }))
        });

        stop_handler.followed_by(commands)
    }
}

impl OnStop<EmptyAgent> for CommanderLifecycle {
    fn on_stop(&self) -> impl EventHandler<EmptyAgent> + '_ {
        UnitHandler::default()
    }
}

impl ItemEvent<EmptyAgent> for CommanderLifecycle {
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &EmptyAgent,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}
