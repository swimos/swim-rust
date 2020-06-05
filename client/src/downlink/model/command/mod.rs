use crate::configuration::downlink::DownlinkParams;
use crate::downlink::{raw, BasicResponse, BasicStateMachine, Command, DownlinkError, Message};
use crate::router::RoutingError;
use common::model::schema::{Schema, StandardSchema};
use common::model::Value;
use common::sink::item::ItemSender;
use futures::StreamExt;
use futures_util::future::ready;
use futures_util::stream::once;
use tokio::sync::mpsc::Sender;

pub fn create_downlink<Commands>(
    schema: Option<StandardSchema>,
    cmd_sender: Commands,
    config: &DownlinkParams,
) -> raw::Sender<Sender<Value>>
where
    Commands: ItemSender<Command<Value>, RoutingError> + Send + 'static,
{
    let init = once(ready(Ok(Message::Synced)));
    let upd_stream = init.chain(futures::stream::pending());

    raw::create_downlink(
        CommandStateMachine::new(schema.unwrap()),
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

impl BasicStateMachine<(), (), Value> for CommandStateMachine {
    type Ev = ();
    type Cmd = Value;

    fn init(&self) {}

    fn on_sync(&self, _state: &()) -> Self::Ev {}

    fn handle_message_unsynced(&self, _state: &mut (), _message: ()) -> Result<(), DownlinkError> {
        Ok(())
    }

    fn handle_message(
        &self,
        _state: &mut (),
        _message: (),
    ) -> Result<Option<Self::Ev>, DownlinkError> {
        Ok(Some(()))
    }

    fn handle_action(&self, _state: &mut (), action: Value) -> BasicResponse<Self::Ev, Self::Cmd> {
        if self.schema.matches(&action) {
            BasicResponse::of((), action)
        } else {
            BasicResponse::none()
        }
    }
}
