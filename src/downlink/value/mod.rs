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

use super::*;
use std::fmt::Debug;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::model::Value;

/// Create a value downlink with back-pressure (it will only process set messages as rapidly
/// as it can write commands to the output).
pub fn create_back_pressure_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: mpsc::Sender<Command<Arc<Value>>>,
    buffer_size: usize,
) -> Downlink<Err, mpsc::Sender<Value>, mpsc::Receiver<Event<Arc<Value>>>>
where
    Err: From<item::MpscErr<Event<Arc<Value>>>>
        + From<item::MpscErr<Command<Arc<Value>>>>
        + Send
        + Debug
        + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
{
    let cmd_sink = item::for_mpsc_sender::<Command<Arc<Value>>, Err>(cmd_sender);
    super::create_downlink(Arc::new(init), update_stream, cmd_sink, buffer_size)
}

fn transform_err<T, Err: From<item::WatchErr<T>>>(
    result: Result<(), item::WatchErr<T>>,
) -> Result<(), Err> {
    result.map_err(|e| e.into())
}

/// Create a value downlink without back-pressure (it will process set operations as rapidly as it
/// can and some outgoing message will be dropped).
pub fn create_dropping_downlink<Err, Updates, Commands>(
    init: Value,
    update_stream: Updates,
    cmd_sender: watch::Sender<Command<Arc<Value>>>,
    buffer_size: usize,
) -> Downlink<Err, mpsc::Sender<Value>, mpsc::Receiver<Event<Arc<Value>>>>
where
    Err: From<item::MpscErr<Event<Arc<Value>>>>
        + From<item::WatchErr<Command<Arc<Value>>>>
        + Send
        + Debug
        + 'static,
    Updates: Stream<Item = Message<Value>> + Send + 'static,
{
    let err_trans = || transform_err::<Command<Arc<Value>>, Err>;
    let cmd_sink = item::map_err(cmd_sender, err_trans);
    super::create_downlink(Arc::new(init), update_stream, cmd_sink, buffer_size)
}

impl StateMachine<Value> for Arc<Value> {
    type Ev = Arc<Value>;
    type Cmd = Arc<Value>;

    fn handle_operation(
        model: &mut Model<Self>,
        op: Operation<Value>,
    ) -> (Option<Event<Self::Ev>>, Option<Command<Self::Cmd>>) {
        let Model { data_state, state } = model;
        match op {
            Operation::Start => (None, Some(Command::Sync)),
            Operation::Message(message) => match message {
                Message::Linked => {
                    *state = DownlinkState::Linked;
                    (None, None)
                }
                Message::Synced => {
                    *state = DownlinkState::Synced;
                    (Some(Event(data_state.clone(), false)), None)
                }
                Message::Action(upd_value) => {
                    *data_state = Arc::new(upd_value);
                    if *state == DownlinkState::Synced {
                        (Some(Event(data_state.clone(), false)), None)
                    } else {
                        (None, None)
                    }
                }
                Message::Unlinked => {
                    *state = DownlinkState::Unlinked;
                    (None, None)
                }
            },
            Operation::Action(set_value) => {
                *data_state = Arc::new(set_value);
                (
                    Some(Event(data_state.clone(), true)),
                    Some(Command::Action(data_state.clone())),
                )
            }
            Operation::Close => (None, Some(Command::Unlink)),
        }
    }
}
