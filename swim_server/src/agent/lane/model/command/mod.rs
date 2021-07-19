// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::lane::model::type_of;
use crate::agent::lane::LaneModel;
use crate::agent::model::COMMANDED_AFTER_STOP;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};
use utilities::sync::trigger;

#[cfg(test)]
mod tests;

/// Model for a lane that can receive commands and broadcasts responses to all subscribers.
///
/// #Type Parameters
///
/// * `T` - The type of commands that the lane can handle.
pub struct CommandLane<T> {
    sender: mpsc::Sender<Command<T>>,
    id: Arc<()>,
}

#[derive(Debug)]
pub struct Command<T> {
    pub(crate) command: T,
    pub(crate) responder: Option<trigger::Sender>,
}

impl<T> Command<T> {
    pub(crate) fn forget(command: T) -> Self {
        Command {
            command,
            responder: None,
        }
    }

    pub(crate) fn new(command: T, responder: trigger::Sender) -> Self {
        Command {
            command,
            responder: Some(responder),
        }
    }

    pub fn destruct(self) -> (T, Option<trigger::Sender>) {
        let Command { command, responder } = self;
        (command, responder)
    }

}

impl<T> CommandLane<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(sender: mpsc::Sender<Command<T>>) -> Self {
        CommandLane {
            sender,
            id: Default::default(),
        }
    }
}

impl<T> Clone for CommandLane<T> {
    fn clone(&self) -> Self {
        CommandLane {
            sender: self.sender.clone(),
            id: self.id.clone(),
        }
    }
}

/// Handle to send commands to a [`CommandLane`].
/// # Type Parameters
///
/// * `T` - The type of commands that the lane can handle.
#[derive(Clone, Debug)]
pub struct Commander<T>(pub(crate) mpsc::Sender<Command<T>>);

const SENDING_COMMAND: &str = "Sending command";

impl<T: Debug> CommandLane<T> {
    /// Create a [`Commander`] that can send multiple commands to the lane.
    pub fn commander(&self) -> Commander<T> {
        Commander(self.sender.clone())
    }
}

impl<T: Debug> Commander<T> {
    /// Asynchronously send a command to the lane.
    pub async fn command(&mut self, cmd: T) {
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);
        let Commander(tx) = self;
        if tx.send(Command::forget(cmd)).await.is_err() {
            event!(Level::ERROR, COMMANDED_AFTER_STOP);
        }
    }

    pub async fn command_and_await(
        &mut self,
        cmd: T,
    ) -> Result<trigger::Receiver, SendError<Command<T>>> {
        let (resp_tx, resp_rx) = trigger::trigger();
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);
        let Commander(tx) = self;
        if let Err(err) = tx.send(Command::new(cmd, resp_tx)).await {
            event!(Level::ERROR, COMMANDED_AFTER_STOP);
            Err(err)
        } else {
            Ok(resp_rx)
        }
    }
}

type CommandStream<T> = ReceiverStream<Command<T>>;

/// Create a new public command lane model and a stream of the received commands.
///
/// # Arguments
///
/// * `buffer_size` - Buffer size for the MPSC channel that transmits the commands.
pub fn make_public_lane_model<T>(
    buffer_size: NonZeroUsize,
) -> (
    CommandLane<T>,
    CommandStream<T>,
    Commander<T>,
    CommandStream<T>,
)
where
    T: Send + Sync + 'static,
{
    let (event_tx, event_rx) = mpsc::channel(buffer_size.get());
    let (local_commands_tx, local_commands_rx) = mpsc::channel(buffer_size.get());
    let lane = CommandLane::new(local_commands_tx);
    (
        lane,
        ReceiverStream::new(event_rx),
        Commander(event_tx),
        ReceiverStream::new(local_commands_rx),
    )
}

/// Create a new private command lane model and a stream of the received commands.
///
/// # Arguments
///
/// * `buffer_size` - Buffer size for the MPSC channel that transmits the commands.
pub fn make_private_lane_model<T>(
    buffer_size: NonZeroUsize,
) -> (CommandLane<T>, ReceiverStream<Command<T>>)
where
    T: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = CommandLane::new(tx);
    (lane, ReceiverStream::new(rx))
}

impl<T> LaneModel for CommandLane<T> {
    type Event = T;

    fn same_lane(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.id, &other.id)
    }
}

impl<T> Debug for CommandLane<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CommandLane")
            .field(&type_of::<fn(T) -> T>())
            .finish()
    }
}
