// Copyright 2015-2020 SWIM.AI inc.
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

use crate::configuration::downlink::DownlinkParams;
use crate::downlink::buffered::{BufferedDownlink, BufferedReceiver};
use crate::downlink::dropping::{DroppingDownlink, DroppingReceiver};
use crate::downlink::queue::{QueueDownlink, QueueReceiver};
use crate::downlink::typed::SchemaViolations;
use crate::downlink::{
    buffered, dropping, queue, Command, DownlinkError, DownlinkState, Event, Message, Operation,
    Response, StateMachine,
};
use crate::router::RoutingError;
use futures::Stream;
use std::num::NonZeroUsize;
use swim_common::model::schema::{Schema, StandardSchema};
use swim_common::model::Value;
use swim_common::sink::item::ItemSender;
use tracing::{error, instrument, trace};

#[cfg(test)]
mod tests;

/// Create an event downlink with a queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Snk>(
    schema: StandardSchema,
    violations: SchemaViolations,
    update_stream: Updates,
    cmd_sink: Snk,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> (QueueDownlink<Value, Value>, QueueReceiver<Value>)
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        EventStateMachine::new(schema, violations),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
}

/// Create an event downlink with a dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Snk>(
    schema: StandardSchema,
    violations: SchemaViolations,
    update_stream: Updates,
    cmd_sink: Snk,
    config: &DownlinkParams,
) -> (DroppingDownlink<Value, Value>, DroppingReceiver<Value>)
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    dropping::make_downlink(
        EventStateMachine::new(schema, violations),
        update_stream,
        cmd_sink,
        &config,
    )
}

/// Create an event downlink with an buffering multiplexing topic.
pub fn create_buffered_downlink<Updates, Snk>(
    schema: StandardSchema,
    violations: SchemaViolations,
    update_stream: Updates,
    cmd_sink: Snk,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> (BufferedDownlink<Value, Value>, BufferedReceiver<Value>)
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    buffered::make_downlink(
        EventStateMachine::new(schema, violations),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
}

struct EventStateMachine {
    schema: StandardSchema,
    violations: SchemaViolations,
}

impl EventStateMachine {
    fn new(schema: StandardSchema, violations: SchemaViolations) -> Self {
        EventStateMachine { schema, violations }
    }
}

impl StateMachine<(), Value, Value> for EventStateMachine {
    type Ev = Value;
    type Cmd = Value;

    fn init_state(&self) {}

    fn dl_start_state(&self) -> DownlinkState {
        DownlinkState::Linked
    }

    #[instrument(skip(self, downlink_state, _state, op))]
    fn handle_operation(
        &self,
        downlink_state: &mut DownlinkState,
        _state: &mut (),
        op: Operation<Value, Value>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        match op {
            Operation::Start => {
                if *downlink_state == DownlinkState::Linked {
                    trace!("Downlink linked");
                    Ok(Response::none())
                } else {
                    trace!("Downlink linking");
                    Ok(Response::for_command(Command::Link))
                }
            }

            Operation::Message(message) => match message {
                Message::Linked => {
                    trace!("Downlink linked");
                    Ok(Response::none())
                }

                Message::Action(value) => {
                    if self.schema.matches(&value) {
                        Ok(Response::for_event(Event::Remote(value)))
                    } else {
                        match self.violations {
                            SchemaViolations::Ignore => Ok(Response::none()),
                            SchemaViolations::Report => {
                                Err(DownlinkError::SchemaViolation(value, self.schema.clone()))
                            }
                        }
                    }
                }

                Message::Unlinked => {
                    trace!("Downlink unlinked");
                    Ok(Response::none().then_terminate())
                }

                Message::BadEnvelope(_) => Err(DownlinkError::MalformedMessage),

                _ => Ok(Response::none()),
            },
            Operation::Error(e) => {
                if e.is_fatal() {
                    error!("Fatal operation error occurred: {:?}", e);

                    Err(e.into())
                } else {
                    *downlink_state = DownlinkState::Unlinked;
                    Ok(Response::for_command(Command::Link))
                }
            }
            _ => Ok(Response::none()),
        }
    }
}
