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

#[cfg(test)]
mod tests;

use crate::agent::lane::LaneModel;
use futures::Stream;
use std::any::{type_name, Any, TypeId};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{event, Level};

/// Model for a lane that can receive commands and optionally produce responses.
///
/// #Type Parameters
///
/// * `Command` - The type of commands that the lane can handle.
/// * `Response` - The type of messages that will be received by a subscriber to the lane.
pub struct ActionLane<Command, Response> {
    sender: mpsc::Sender<Action<Command, Response>>,
    id: Arc<()>,
}

#[derive(Debug)]
pub struct Action<Command, Response> {
    pub(crate) command: Command,
    pub(crate) responder: Option<oneshot::Sender<Response>>,
}

impl<Command, Response> Action<Command, Response> {
    pub(crate) fn forget(command: Command) -> Self {
        Action {
            command,
            responder: None,
        }
    }

    pub(crate) fn new(command: Command, responder: oneshot::Sender<Response>) -> Self {
        Action {
            command,
            responder: Some(responder),
        }
    }

    pub fn destruct(self) -> (Command, Option<oneshot::Sender<Response>>) {
        let Action { command, responder } = self;
        (command, responder)
    }
}

impl<Command, Response> ActionLane<Command, Response>
where
    Command: Send + Sync + 'static,
{
    pub(crate) fn new(sender: mpsc::Sender<Action<Command, Response>>) -> Self {
        ActionLane {
            sender,
            id: Default::default(),
        }
    }
}

impl<Command, Response> Clone for ActionLane<Command, Response> {
    fn clone(&self) -> Self {
        ActionLane {
            sender: self.sender.clone(),
            id: self.id.clone(),
        }
    }
}

/// Handle to send commands to a [`ActionLane`].
/// #Type Parameters
///
/// * `Command` - The type of commands that the lane can handle.
#[derive(Clone, Debug)]
pub struct Commander<Command, Response>(mpsc::Sender<Action<Command, Response>>);

const SENDING_COMMAND: &str = "Sending command";

impl<Command: Debug, Response> ActionLane<Command, Response> {
    /// Create a [`Commander`] that can send multiple commands to the lane.
    pub fn commander(&self) -> Commander<Command, Response> {
        Commander(self.sender.clone())
    }
}

impl<Command: Debug, Response> Commander<Command, Response> {
    /// Asynchronously send a command to the lane.
    pub async fn command(&mut self, cmd: Command) {
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);
        let Commander(tx) = self;
        if tx.send(Action::forget(cmd)).await.is_err() {
            panic!("Lane commanded after the agent stopped.")
        }
    }

    pub async fn command_and_await(&mut self, cmd: Command) -> oneshot::Receiver<Response> {
        let (resp_tx, resp_rx) = oneshot::channel();
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);
        let Commander(tx) = self;
        if tx.send(Action::new(cmd, resp_tx)).await.is_err() {
            panic!("Lane commanded after the agent stopped.")
        }
        resp_rx
    }
}

/// Create a new action lane model and a stream of the received commands.
///
/// #Arguments
///
/// * `buffer_size` - Buffer size for the MPSC channel that transmits the commands.
pub fn make_lane_model<Command, Response>(
    buffer_size: NonZeroUsize,
) -> (
    ActionLane<Command, Response>,
    impl Stream<Item = Action<Command, Response>> + Send + 'static,
)
where
    Command: Send + Sync + 'static,
    Response: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = ActionLane::new(tx);
    (lane, rx)
}

impl<Command, Response> LaneModel for ActionLane<Command, Response> {
    type Event = Command;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

/// An action lane model that produces no response.
pub type CommandLane<Command> = ActionLane<Command, ()>;

struct TypeOf<T: ?Sized>(PhantomData<T>);

fn type_of<T: ?Sized>() -> TypeOf<T> {
    TypeOf(PhantomData)
}

impl<T: ?Sized> Debug for TypeOf<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", type_name::<T>())
    }
}

impl<Command, Response: Any> Debug for ActionLane<Command, Response> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let id_resp = TypeId::of::<Response>();
        let id_unit = TypeId::of::<()>();
        if id_resp == id_unit {
            f.debug_tuple("CommandLane")
                .field(&type_of::<Command>())
                .finish()
        } else {
            f.debug_tuple("ActionLane")
                .field(&type_of::<fn(Command) -> Response>())
                .finish()
        }
    }
}
