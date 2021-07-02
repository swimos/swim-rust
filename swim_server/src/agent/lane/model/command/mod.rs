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
use futures::Stream;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::routing::RoutingAddr;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

#[cfg(test)]
mod tests;

/// Model for a lane that can receive commands and broadcasts responses to all subscribers.
///
/// #Type Parameters
///
/// * `T` - The type of commands that the lane can handle.
pub struct CommandLane<T> {
    sender: mpsc::Sender<Command<T>>,
    feedback_tx: mpsc::Sender<T>,
    id: Arc<()>,
}

#[derive(Debug)]
pub struct Command<T> {
    pub(crate) command: T,
    pub(crate) responder: Option<oneshot::Sender<T>>,
}

impl<T> Command<T> {
    pub(crate) fn forget(command: T) -> Self {
        Command {
            command,
            responder: None,
        }
    }

    pub(crate) fn new(command: T, responder: oneshot::Sender<T>) -> Self {
        Command {
            command,
            responder: Some(responder),
        }
    }

    pub fn destruct(self) -> (T, Option<oneshot::Sender<T>>) {
        let Command { command, responder } = self;
        (command, responder)
    }
}

impl<T> CommandLane<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(
        sender: mpsc::Sender<Command<T>>,
        feedback_tx: mpsc::Sender<T>,
    ) -> Self {
        CommandLane {
            sender,
            feedback_tx,
            id: Default::default(),
        }
    }
}

impl<T> Clone for CommandLane<T> {
    fn clone(&self) -> Self {
        CommandLane {
            sender: self.sender.clone(),
            feedback_tx: self.feedback_tx.clone(),
            id: self.id.clone(),
        }
    }
}

/// Handle to send commands to a [`CommandLane`].
/// # Type Parameters
///
/// * `T` - The type of commands that the lane can handle.
#[derive(Clone, Debug)]
pub struct Commander<T>(mpsc::Sender<Command<T>>, mpsc::Sender<T>);

const SENDING_COMMAND: &str = "Sending command";

impl<T: Debug> CommandLane<T> {
    /// Create a [`Commander`] that can send multiple commands to the lane.
    pub fn commander(&self) -> Commander<T> {
        Commander(self.sender.clone(), self.feedback_tx.clone())
    }
}

impl<T: Debug> Commander<T> {
    /// Asynchronously send a command to the lane.
    pub async fn command(&mut self, cmd: T) {
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);

        let (resp_tx, resp_rx) = oneshot::channel();
        let Commander(tx, feedback_tx) = self;
        if tx.send(Command::new(cmd, resp_tx)).await.is_err() {
            event!(Level::ERROR, COMMANDED_AFTER_STOP);
        }
        let val = resp_rx.await.unwrap();
        eprintln!("val = {:#?}", val);
        feedback_tx
            .send(val)
            .await
            .unwrap();
    }

    pub async fn command_and_await(
        &mut self,
        cmd: T,
    ) -> Result<oneshot::Receiver<T>, SendError<Command<T>>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        event!(Level::TRACE, SENDING_COMMAND, ?cmd);
        let Commander(tx, feedback_tx) = self;
        if let Err(err) = tx.send(Command::new(cmd, resp_tx)).await {
            event!(Level::ERROR, COMMANDED_AFTER_STOP);
            Err(err)
        } else {
            Ok(resp_rx)
        }
    }
}

/// Create a new command lane model and a stream of the received commands.
///
/// #Arguments
///
/// * `buffer_size` - Buffer size for the MPSC channel that transmits the commands.
pub fn make_lane_model<T>(
    buffer_size: NonZeroUsize,
) -> (
    CommandLane<T>,
    impl Stream<Item = Command<T>> + Send + 'static,
    (mpsc::Sender<T>, mpsc::Receiver<T>),
)
where
    T: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let (feedback_tx, feedback_rx) = mpsc::channel(buffer_size.get());
    let lane = CommandLane::new(tx, feedback_tx.clone());
    (lane, ReceiverStream::new(rx), (feedback_tx, feedback_rx))
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
