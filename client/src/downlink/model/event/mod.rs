use crate::configuration::downlink::DownlinkParams;
use crate::downlink::buffered::BufferedReceiver;
use crate::downlink::dropping::DroppingReceiver;
use crate::downlink::queue::QueueReceiver;
use crate::downlink::{
    buffered, dropping, queue, Command, DownlinkError, DownlinkState, Event, Message, Operation,
    Response, StateMachine,
};
use crate::router::RoutingError;
use common::model::schema::{Schema, StandardSchema};
use common::model::Value;
use common::sink::item::ItemSender;
use futures::Stream;
use std::num::NonZeroUsize;
use tracing::{trace};

/// Create an event downlink with a queue based multiplexing topic.
pub fn create_queue_downlink<Updates, Snk>(
    schema: StandardSchema,
    update_stream: Updates,
    cmd_sink: Snk,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> QueueReceiver<Value>
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    // let cmd_sink = item::drop_all::drop_all();

    queue::make_downlink(
        EventStateMachine::new(schema),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
    .1
}

/// Create an event downlink with a dropping multiplexing topic.
pub fn create_dropping_downlink<Updates, Snk>(
    schema: StandardSchema,
    update_stream: Updates,
    cmd_sink: Snk,
    config: &DownlinkParams,
) -> DroppingReceiver<Value>
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    // let cmd_sink = item::drop_all::drop_all();

    dropping::make_downlink(
        EventStateMachine::new(schema),
        update_stream,
        cmd_sink,
        &config,
    )
    .1
}

/// Create an event downlink with an buffering multiplexing topic.
pub fn create_buffered_downlink<Updates, Snk>(
    schema: StandardSchema,
    update_stream: Updates,
    cmd_sink: Snk,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> BufferedReceiver<Value>
where
    Updates: Stream<Item = Result<Message<Value>, RoutingError>> + Send + 'static,
    Snk: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    // let cmd_sink = item::drop_all::drop_all();

    buffered::make_downlink(
        EventStateMachine::new(schema),
        update_stream,
        cmd_sink,
        queue_size,
        &config,
    )
    .1
}

struct EventStateMachine {
    schema: StandardSchema,
}

impl EventStateMachine {
    fn new(schema: StandardSchema) -> Self {
        EventStateMachine { schema }
    }
}

impl StateMachine<DownlinkState, Value, Value> for EventStateMachine {
    type Ev = Value;
    type Cmd = Value;

    fn init_state(&self) -> DownlinkState {
        DownlinkState::Unlinked
    }

    fn dl_start_state(&self) -> DownlinkState {
        DownlinkState::Linked
    }

    fn handle_operation(
        &self,
        _downlink_state: &mut DownlinkState,
        state: &mut DownlinkState,
        op: Operation<Value, Value>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        match op {
            Operation::Start => {
                if *state == DownlinkState::Linked {
                    trace!("Downlink linked");
                    Ok(Response::none())
                } else {
                    trace!("Downlink linking");
                    Ok(Response::for_command(Command::Link))
                }
            }

            Operation::Message(message) => match message {
                Message::Linked => Ok(Response::none()),
                Message::Synced => Ok(Response::none()),
                Message::Action(value) => {
                    if self.schema.matches(&value) {
                        Ok(Response::for_event(Event(value, true)))
                    } else {
                        Ok(Response::none())
                    }
                }
                Message::Unlinked => Ok(Response::none()),
                Message::BadEnvelope(_) => Ok(Response::none()),
            },

            _ => Ok(Response::none()),
        }
    }
}
