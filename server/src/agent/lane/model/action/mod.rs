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

use crate::agent::lane::LaneModel;
use futures::Stream;
use std::any::{type_name, Any, TypeId};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;

/// Model for a lane that can receive commands and optionally produce responses. It is entirely
/// stateless so has no fields.
pub struct ActionLane<Command, Response> {
    sender: mpsc::Sender<Command>,
    _handler_type: PhantomData<fn(Command) -> Response>,
}

impl<Command, Response> Clone for ActionLane<Command, Response> {
    fn clone(&self) -> Self {
        ActionLane {
            sender: self.sender.clone(),
            _handler_type: PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Commander<Command>(mpsc::Sender<Command>);

impl<Command, Response> ActionLane<Command, Response> {
    pub fn commander(&self) -> Commander<Command> {
        Commander(self.sender.clone())
    }
}

impl<Command> Commander<Command> {
    pub async fn command(&mut self, cmd: Command) {
        let Commander(tx) = self;
        if tx.send(cmd).await.is_err() {
            panic!("Lane commanded after the agent stopped.")
        }
    }
}

/// Create a new action lane model and a stream of the received commands..
pub fn make_lane_model<Command, Response>(
    buffer_size: NonZeroUsize,
) -> (
    ActionLane<Command, Response>,
    impl Stream<Item = Command> + Send + 'static,
)
where
    Command: Send + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size.get());
    let lane = ActionLane {
        sender: tx,
        _handler_type: PhantomData,
    };
    (lane, rx)
}

impl<Command, Response> LaneModel for ActionLane<Command, Response> {
    type Event = Response;
}

/// An action lane model that produces no response.
pub type CommandLane<Command> = ActionLane<Command, ()>;

impl<Command, Response: Any> Debug for ActionLane<Command, Response> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let cmd_name = type_name::<Command>();
        let resp_name = type_name::<Response>();
        let id_resp = TypeId::of::<Response>();
        let id_unit = TypeId::of::<()>();
        if id_resp == id_unit {
            write!(f, "CommandLane({})", cmd_name)
        } else {
            write!(f, "ActionLane({} -> {})", cmd_name, resp_name)
        }
    }
}
