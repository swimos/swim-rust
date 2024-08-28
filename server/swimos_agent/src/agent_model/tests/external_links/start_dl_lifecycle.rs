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

use std::cell::RefCell;

use futures::{
    future::{pending, ready, BoxFuture},
    FutureExt,
};
use swimos_api::{address::Address, agent::DownlinkKind};
use swimos_model::Text;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

use crate::{
    agent_lifecycle::{item_event::ItemEvent, on_init::OnInit, on_start::OnStart, on_stop::OnStop},
    agent_model::downlink::{
        BoxDownlinkChannel, DownlinkChannel, DownlinkChannelError, DownlinkChannelEvent,
        DownlinkChannelFactory,
    },
    event_handler::{
        ActionContext, DownlinkSpawnOnDone, EventHandler, HandlerAction, LocalBoxEventHandler,
        StepResult, UnitHandler,
    },
    AgentMetadata,
};

use super::empty_agent::EmptyAgent;

pub struct StartDownlinkLifecycle {
    address: Address<Text>,
    on_done: RefCell<Option<DownlinkSpawnOnDone<EmptyAgent>>>,
}

impl StartDownlinkLifecycle {
    pub fn new(address: Address<Text>, on_done: DownlinkSpawnOnDone<EmptyAgent>) -> Self {
        StartDownlinkLifecycle {
            address,
            on_done: RefCell::new(Some(on_done)),
        }
    }
}

impl OnInit<EmptyAgent> for StartDownlinkLifecycle {
    fn initialize(
        &self,
        _action_context: &mut ActionContext<EmptyAgent>,
        _meta: AgentMetadata,
        _context: &EmptyAgent,
    ) {
    }
}

impl OnStart<EmptyAgent> for StartDownlinkLifecycle {
    fn on_start(&self) -> impl EventHandler<EmptyAgent> + '_ {
        let StartDownlinkLifecycle { address, on_done } = self;
        StartDl {
            address: address.clone(),
            on_done: on_done.borrow_mut().take(),
        }
    }
}

impl OnStop<EmptyAgent> for StartDownlinkLifecycle {
    fn on_stop(&self) -> impl EventHandler<EmptyAgent> + '_ {
        UnitHandler::default()
    }
}

impl ItemEvent<EmptyAgent> for StartDownlinkLifecycle {
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

pub struct StartDl {
    address: Address<Text>,
    on_done: Option<DownlinkSpawnOnDone<EmptyAgent>>,
}

impl HandlerAction<EmptyAgent> for StartDl {
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<EmptyAgent>,
        _meta: AgentMetadata,
        _context: &EmptyAgent,
    ) -> StepResult<Self::Completion> {
        let StartDl { address, on_done } = self;
        if let Some(on_done) = on_done.take() {
            action_context.start_downlink(address.clone(), TestFac(address.clone()), on_done);
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

struct TestFac(Address<Text>);
struct TestChan(Address<Text>);

impl DownlinkChannel<EmptyAgent> for TestChan {
    fn connect(&mut self, _context: &EmptyAgent, _output: ByteWriter, _input: ByteReader) {}

    fn can_restart(&self) -> bool {
        false
    }

    fn address(&self) -> &Address<Text> {
        &self.0
    }

    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn await_ready(
        &mut self,
    ) -> BoxFuture<'_, Option<Result<DownlinkChannelEvent, DownlinkChannelError>>> {
        pending().boxed()
    }

    fn next_event(
        &mut self,
        _context: &EmptyAgent,
    ) -> Option<LocalBoxEventHandler<'_, EmptyAgent>> {
        None
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), std::io::Error>> {
        ready(Ok(())).boxed()
    }
}

impl DownlinkChannelFactory<EmptyAgent> for TestFac {
    fn create(
        self,
        _context: &EmptyAgent,
        _tx: ByteWriter,
        _rx: ByteReader,
    ) -> BoxDownlinkChannel<EmptyAgent> {
        Box::new(TestChan(self.0))
    }

    fn create_box(
        self: Box<Self>,
        context: &EmptyAgent,
        tx: ByteWriter,
        rx: ByteReader,
    ) -> BoxDownlinkChannel<EmptyAgent> {
        (*self).create(context, tx, rx)
    }

    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }
}
