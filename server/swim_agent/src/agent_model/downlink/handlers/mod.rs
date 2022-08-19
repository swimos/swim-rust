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

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc;

use crate::{downlink_lifecycle::value::ValueDownlinkLifecycle, event_handler::{EventHandlerError, HandlerActionExt}, agent_lifecycle::lane_event::LaneEvent, agent_model::HandlerRunner};

use super::DownlinkMessage;

pub struct ValueDownlinkEndpoint<T, LC> {
    receiver: mpsc::Receiver<DownlinkMessage<T>>,
    lifecycle: LC,
    current: Option<T>,
}

pub struct ValueDownlinkEvent<T, LC> {
    endpoint: ValueDownlinkEndpoint<T, LC>,
    event: DownlinkMessage<T>,
}


impl<T, LC> ValueDownlinkEndpoint<T, LC>
{

    fn with(self, event: DownlinkMessage<T>) -> ValueDownlinkEvent<T, LC> {
        ValueDownlinkEvent { endpoint: self, event }
    }

    pub async fn next_event<Context>(mut self) -> Option<ValueDownlinkEvent<T, LC>>
    where
        LC: ValueDownlinkLifecycle<T, Context>,
     {
        let ValueDownlinkEndpoint { receiver, .. } = &mut self;
        receiver.recv().await.map(move |ev| self.with(ev))
    }

}

type NextEvent<Context, AgentLifecycle> = BoxFuture<'static, Option<Box<dyn DownlinkEvent<Context, AgentLifecycle> + 'static>>>;

struct ConsumptionResult<Context, AgentLifecycle> {
    pub result: Result<(), EventHandlerError>,
    pub next: NextEvent<Context, AgentLifecycle>,
}

trait DownlinkEvent<Context, AgentLifecycle> {

    fn consume(self, handler_runner: HandlerRunner<Context, AgentLifecycle>) -> ConsumptionResult<Context, AgentLifecycle>;

}

impl<Context, AgentLifecycle, T, LC> DownlinkEvent<Context, AgentLifecycle> for ValueDownlinkEvent<T, LC>
where
    Context: 'static,
    LC: 'static,
    T: Send + 'static,
    AgentLifecycle: for<'b> LaneEvent<'b, Context>,
    LC: ValueDownlinkLifecycle<T, Context>, 
{
    fn consume(self, mut handler_runner: HandlerRunner<Context, AgentLifecycle>) -> ConsumptionResult<Context, AgentLifecycle> {
        let ValueDownlinkEvent { mut endpoint, event } = self;
        let ValueDownlinkEndpoint { lifecycle, current, ..} = &mut endpoint;
        let result = match event {
            DownlinkMessage::Linked => {
                let handler = lifecycle.on_linked();
                handler_runner.run_handler_in(handler)
            },
            DownlinkMessage::Synced => {
                if let Some(value) = current {
                   let handler = lifecycle.on_synced(value);
                   handler_runner.run_handler_in(handler)
                } else {
                    todo!()
                }
            },
            DownlinkMessage::Event(value) => {
                let prev = current.take();
                let current_ref = &*current.insert(value);
                let handler = lifecycle.on_event(current_ref)
                    .followed_by(lifecycle.on_set(prev, current_ref));
                handler_runner.run_handler_in(handler)
            },
            DownlinkMessage::Unlinked => {
                let handler = lifecycle.on_unlinked();
                handler_runner.run_handler_in(handler)
            },
        };
        
        let next = endpoint.next_event().map(|maybe_ev| 
            maybe_ev.map(|ev| {
                let boxed: Box<dyn DownlinkEvent<Context, AgentLifecycle> + 'static> = Box::new(ev);
                boxed
            })
        ).boxed();
        ConsumptionResult {
            result,
            next,
        }
    }
}