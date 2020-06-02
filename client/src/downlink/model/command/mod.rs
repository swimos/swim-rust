use crate::configuration::downlink::DownlinkParams;
use crate::downlink::model::map::MapAction;
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::queue::{QueueDownlink, QueueReceiver};
use crate::downlink::{
    queue, BasicResponse, BasicStateMachine, Command, DownlinkError, TransitionError,
};
use crate::router::RoutingError;
use common::model::schema::StandardSchema;
use common::sink::item::ItemSender;
use std::num::NonZeroUsize;

pub enum CommandAction {
    ValueAction(Action),
    MapAction(MapAction),
}

pub fn create_queue_downlink<Commands>(
    schema: Option<StandardSchema>,
    cmd_sender: Commands,
    queue_size: NonZeroUsize,
    config: &DownlinkParams,
) -> QueueDownlink<CommandAction, ()>
where
    Commands: ItemSender<Command<SharedValue>, RoutingError> + Send + 'static,
{
    queue::make_downlink(
        CommandStateMachine::new(schema.unwrap()),
        futures::stream::pending(),
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

impl BasicStateMachine<(), (), CommandAction> for CommandStateMachine {
    type Ev = ();
    type Cmd = SharedValue;

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
        action: CommandAction,
    ) -> BasicResponse<Self::Ev, Self::Cmd> {
        // match action {
        //     CommandAction::ValueAction(action) => match action {
        //         Action::Get(resp) => match resp.send_ok(state.state.clone()) {
        //             Err(_) => BasicResponse::none().with_error(TransitionError::ReceiverDropped),
        //             _ => BasicResponse::none(),
        //         },
        //         Action::Set(set_value, maybe_resp) => {
        //             apply_set(state, &self.schema, set_value, maybe_resp, |_| ())
        //         }
        //         Action::Update(upd_fn, maybe_resp) => {
        //             let new_value = upd_fn(state.state.as_ref());
        //             apply_set(state, &self.schema, new_value, maybe_resp, |s| s.clone())
        //         }
        //         Action::TryUpdate(upd_fn, maybe_resp) => try_apply_set(
        //             state,
        //             &self.schema,
        //             upd_fn(state.state.as_ref()),
        //             maybe_resp,
        //         ),
        //     },
        //     CommandAction::MapAction(map_action) => {
        //         unimplemented!();
        //     }
        // }
        unimplemented!()
    }
}
