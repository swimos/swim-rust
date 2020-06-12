use crate::configuration::downlink::DownlinkParams;
use crate::downlink::{
    raw, Command, DownlinkError, DownlinkState, Operation, Response, StateMachine,
};
use crate::router::RoutingError;
use common::model::schema::{Schema, StandardSchema};
use common::model::Value;
use common::sink::item::ItemSender;
use tokio::sync::mpsc::Sender;

#[cfg(test)]
mod tests;

pub fn create_downlink<Commands>(
    schema: StandardSchema,
    cmd_sender: Commands,
    config: &DownlinkParams,
) -> raw::Sender<Sender<Value>>
where
    Commands: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    let upd_stream = futures::stream::pending();

    raw::create_downlink(
        CommandStateMachine::new(schema),
        upd_stream,
        cmd_sender,
        config.buffer_size,
        config.yield_after,
        config.on_invalid,
    )
    .split()
    .0
}
struct CommandStateMachine {
    schema: StandardSchema,
}

impl CommandStateMachine {
    fn new(schema: StandardSchema) -> Self {
        CommandStateMachine { schema }
    }
}

impl StateMachine<(), (), Value> for CommandStateMachine {
    type Ev = ();
    type Cmd = Value;

    fn init_state(&self) {}

    fn dl_start_state(&self) -> DownlinkState {
        DownlinkState::Unlinked
    }

    fn handle_operation(
        &self,
        _downlink_state: &mut DownlinkState,
        _state: &mut (),
        op: Operation<(), Value>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        match op {
            Operation::Action(value) => {
                if self.schema.matches(&value) {
                    Ok(Response::for_command(Command::Action(value)))
                } else {
                    Ok(Response::none())
                }
            }

            _ => Ok(Response::none()),
        }
    }
}
