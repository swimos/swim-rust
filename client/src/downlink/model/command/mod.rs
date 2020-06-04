use crate::configuration::downlink::DownlinkParams;
use crate::downlink::buffered::BufferedDownlink;
use crate::downlink::dropping::DroppingDownlink;
use crate::downlink::model::map::MapModification;
use crate::downlink::queue::QueueDownlink;
use crate::downlink::{
    buffered, dropping, queue, BasicResponse, BasicStateMachine, Command, DownlinkError, Message,
};
use crate::router::RoutingError;
use common::model::schema::{Schema, StandardSchema};
use common::model::Value;
use common::sink::item::ItemSender;
use futures::StreamExt;
use futures_util::future::ready;
use futures_util::stream::once;
use std::num::NonZeroUsize;

pub enum CommandValue {
    Value(Value),
    Map(MapModification<Value>),
}

pub fn create_queue_downlink<Commands>(
    schema: Option<StandardSchema>,
    cmd_sender: Commands,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> QueueDownlink<CommandValue, ()>
where
    Commands: ItemSender<Command<CommandValue>, RoutingError> + Send + 'static,
{
    let init = once(ready(Ok(Message::Synced)));
    let upd_stream = init.chain(futures::stream::pending());

    queue::make_downlink(
        CommandStateMachine::new(schema.unwrap()),
        upd_stream,
        cmd_sender,
        queue_size,
        &config,
    )
    .0
}

pub fn create_dropping_downlink<Commands>(
    schema: Option<StandardSchema>,
    cmd_sender: Commands,
    config: &DownlinkParams,
) -> DroppingDownlink<CommandValue, ()>
where
    Commands: ItemSender<Command<CommandValue>, RoutingError> + Send + 'static,
{
    let init = once(ready(Ok(Message::Synced)));
    let upd_stream = init.chain(futures::stream::pending());

    dropping::make_downlink(
        CommandStateMachine::new(schema.unwrap()),
        upd_stream,
        cmd_sender,
        &config,
    )
    .0
}

pub fn create_buffered_downlink<Commands>(
    schema: Option<StandardSchema>,
    cmd_sender: Commands,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> BufferedDownlink<CommandValue, ()>
where
    Commands: ItemSender<Command<CommandValue>, RoutingError> + Send + 'static,
{
    let init = once(ready(Ok(Message::Synced)));
    let upd_stream = init.chain(futures::stream::pending());

    buffered::make_downlink(
        CommandStateMachine::new(schema.unwrap()),
        upd_stream,
        cmd_sender,
        queue_size,
        &config,
    )
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

impl BasicStateMachine<(), (), CommandValue> for CommandStateMachine {
    type Ev = ();
    type Cmd = CommandValue;

    fn init(&self) -> () {}

    fn on_sync(&self, state: &()) -> Self::Ev {}

    fn handle_message_unsynced(&self, state: &mut (), message: ()) -> Result<(), DownlinkError> {
        Ok(())
    }

    fn handle_message(
        &self,
        state: &mut (),
        message: (),
    ) -> Result<Option<Self::Ev>, DownlinkError> {
        Ok(Some(()))
    }

    fn handle_action(
        &self,
        state: &mut (),
        action: CommandValue,
    ) -> BasicResponse<Self::Ev, Self::Cmd> {
        match action {
            CommandValue::Value(value) => {
                if self.schema.matches(&value) {
                    BasicResponse::of((), CommandValue::Value(value))
                } else {
                    //Todo
                    unimplemented!();
                }
            }
            CommandValue::Map(value) => BasicResponse::of((), CommandValue::Map(value)),
        }
    }
}
