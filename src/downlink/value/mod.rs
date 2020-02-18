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
use std::pin::Pin;
use std::sync::Arc;

use futures::executor::block_on;
use futures::future::FusedFuture;
use futures::{future, stream, FutureExt, Sink, SinkExt, Stream, StreamExt};
use pin_utils::pin_mut;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::model::Value;
use crate::sink::item::ItemSink;
use crate::sink::{MpscSink, SinkSendError};

/// An open value downlink. This maintains an internal state consisting of a [`Value`] which can be
/// altered by local set operations or updates from a remote lane. Whenever a local set is applied,
/// a command will be output back to the remote lane. This type implements both [`Stream`] (for
/// observing its state) and [`Sink`] for issuing sets.
///
pub struct ValueDownlink<Err: Debug, S, R> {
    pub set_sink: S,
    pub event_stream: R,
    task: Option<DownlinkTask<Err>>,
}

impl<Err: Debug, S, R> ValueDownlink<Err, S, R>
where
    S: Sink<Value>,
    R: Stream<Item = LaneEvent>,
{
    fn new(
        set_sink: S,
        event_stream: R,
        task: Option<DownlinkTask<Err>>,
    ) -> ValueDownlink<Err, S, R> {
        ValueDownlink {
            set_sink,
            event_stream,
            task,
        }
    }

    /// Stop the downlink from running.
    pub async fn stop(mut self) -> Result<(), Err> {
        match (&mut self).task.take() {
            Some(t) => t.stop().await,
            _ => Ok(()),
        }
    }
}

impl<Err: Debug, S, R> Drop for ValueDownlink<Err, S, R> {
    fn drop(&mut self) {
        match self.task.take() {
            Some(t) => {
                block_on(t.stop()).unwrap();
            }
            _ => {}
        }
    }
}

/// Asynchronously create a new downlink from a stream of input events, writing to a sink of
/// commands.
pub async fn create_downlink<Err, Upd, Cmd>(
    init: Value,
    update_stream: Upd,
    cmd_sink: Cmd,
    buffer_size: usize,
) -> ValueDownlink<Err, MpscSink<Value>, mpsc::Receiver<LaneEvent>>
where
    Err: From<SinkSendError<LaneEvent>> + Send + Debug + 'static,
    Upd: Stream<Item = LaneMessage> + Send + 'static,
    Cmd: Sink<LaneCommand, Error = Err> + Send + 'static,
{
    let (set_tx, set_rx) = mpsc::channel::<Value>(buffer_size);
    let (event_tx, event_rx) = mpsc::channel::<LaneEvent>(buffer_size);
    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    let event_sink = MpscSink::wrap(event_tx).sink_err_into::<Err>();

    // The task that maintains the internal state of the lane.
    let lane_task = make_lane_task(
        init,
        combine_inputs(update_stream, set_rx, stop_rx),
        cmd_sink,
        event_sink,
    );

    let join_handle = tokio::task::spawn(lane_task);

    let dl_task = DownlinkTask {
        join_handle,
        stop_trigger: stop_tx,
    };

    ValueDownlink::new(MpscSink::wrap(set_tx), event_rx, Some(dl_task))
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum ValueLaneOperation {
    Start,
    Message(LaneMessage),
    Set(Value),
    Close,
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum OutputTrigger {
    FromUpdate(Arc<Value>),
    FromSet(Arc<Value>),
    SyncRequired,
}

impl ValueLaneOperation {
    fn into_update(
        self,
        state: &mut DownlinkState,
        value_state: &mut Arc<Value>,
    ) -> Option<OutputTrigger> {
        match self {
            ValueLaneOperation::Start => Some(OutputTrigger::SyncRequired),
            ValueLaneOperation::Message(LaneMessage::Linked) => {
                *state = DownlinkState::Linked;
                None
            }
            ValueLaneOperation::Message(LaneMessage::Synced) => match *state {
                DownlinkState::Synced => None,
                _ => {
                    *state = DownlinkState::Synced;
                    Some(OutputTrigger::FromUpdate(value_state.clone()))
                }
            },
            ValueLaneOperation::Message(LaneMessage::ValueUpdated(new_value)) => match *state {
                DownlinkState::Unlinked => None,
                DownlinkState::Linked => {
                    *value_state = Arc::new(new_value);
                    None
                }
                DownlinkState::Synced => {
                    *value_state = Arc::new(new_value);
                    Some(OutputTrigger::FromUpdate(value_state.clone()))
                }
            },
            ValueLaneOperation::Message(LaneMessage::Unlinked) => {
                *state = DownlinkState::Unlinked;
                None
            }
            ValueLaneOperation::Set(new_value) => {
                *value_state = Arc::new(new_value);
                Some(OutputTrigger::FromSet(value_state.clone()))
            }
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LaneMessage {
    Linked,
    Synced,
    ValueUpdated(Value),
    Unlinked,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LaneCommand {
    Sync,
    SetValue(Arc<Value>),
    Unlink,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LaneEvent(pub Arc<Value>, pub bool);

/// Combines together updates received from the Warp connection, local sets and the stop signal
/// into a single stream.
fn combine_inputs<Upd, Set>(
    updates: Upd,
    sets: Set,
    stop: oneshot::Receiver<()>,
) -> impl Stream<Item = ValueLaneOperation> + Send + 'static
where
    Upd: Stream<Item = LaneMessage> + Send + 'static,
    Set: Stream<Item = Value> + Send + 'static,
{
    let upd_operations = updates.map(|v| ValueLaneOperation::Message(v));
    let set_operations = sets.map(|v| ValueLaneOperation::Set(v));
    let close_operations = stream::once(stop).map(|_| ValueLaneOperation::Close);

    let updates_and_sets = stream::select(upd_operations, set_operations);

    let init = stream::once(future::ready(ValueLaneOperation::Start));

    init.chain(stream::select(close_operations, updates_and_sets))
}

/// A task that continuously reads incoming events and set operations and applies them to a state
/// variable. Each time the state is updated it is emitted on an output sink and each time it is
/// locally set it is announced on a command sink. It also has a callback which can be used to
/// stop the process from another task.
fn make_lane_task<E, Ops, Cmd, Event>(
    init: Value,
    operations: Ops,
    cmd_output: Cmd,
    event_output: Event,
) -> impl FusedFuture<Output = Result<(), E>>
where
    Ops: Stream<Item = ValueLaneOperation>,
    Cmd: Sink<LaneCommand, Error = E>,
    Event: Sink<LaneEvent, Error = E>,
{
    let mut state = DownlinkState::Unlinked;
    let mut state_value = Arc::new(init);

    let updates = StreamExt::take_while(operations, |op| {
        future::ready(*op != ValueLaneOperation::Close)
    })
    .filter_map(move |op| {
        future::ready(
            op.into_update(&mut state, &mut state_value)
                .map(|upd| Ok(upd)),
        )
    });

    let events = event_output.with_flat_map(|update: OutputTrigger| {
        stream::once(future::ready(update)).filter_map(|upd| {
            future::ready(match upd {
                OutputTrigger::FromSet(value) => Some(Ok(LaneEvent(value, true))),
                OutputTrigger::FromUpdate(value) => Some(Ok(LaneEvent(value, false))),
                _ => None,
            })
        })
    });

    let commands = cmd_output.with_flat_map(|update: OutputTrigger| {
        stream::once(future::ready(update)).filter_map(|upd| {
            future::ready(match upd {
                OutputTrigger::FromSet(value) => Some(Ok(LaneCommand::SetValue(value))),
                OutputTrigger::SyncRequired => Some(Ok(LaneCommand::Sync)),
                _ => None,
            })
        })
    });

    let combined = events.fanout(commands);

    updates.forward(combined)
}

/// The state of a value lane and the receives of events that can be generted by it.
struct ValueLaneModel<Cmd, Ev> {
    state: DownlinkState,
    value_state: Arc<Value>,
    command_out: Cmd,
    event_out: Ev,
}

impl<Cmd, Ev> ValueLaneModel<Cmd, Ev> {
    fn new(init: Value, command_out: Cmd, event_out: Ev) -> ValueLaneModel<Cmd, Ev> {
        ValueLaneModel {
            state: DownlinkState::Unlinked,
            value_state: Arc::new(init),
            command_out,
            event_out,
        }
    }
}

impl<E, Cmd, Ev> ValueLaneModel<Cmd, Ev>
where
    Cmd: for<'b> ItemSink<'b, LaneCommand, Error = E>,
    Ev: for<'b> ItemSink<'b, LaneEvent, Error = E>,
{
    /// Updates the state of the value lane, generates any side effects and sends out response
    /// messages.
    async fn handle_operation(&mut self, op: ValueLaneOperation) -> Option<Result<(), E>> {
        let ValueLaneModel {
            state,
            value_state,
            command_out,
            event_out,
        } = self;
        match op {
            ValueLaneOperation::Start => Some(command_out.send_item(LaneCommand::Sync).await),
            ValueLaneOperation::Message(LaneMessage::Linked) => {
                *state = DownlinkState::Linked;
                Some(Ok(()))
            }
            ValueLaneOperation::Message(LaneMessage::Synced) => match *state {
                DownlinkState::Synced => Some(Ok(())),
                _ => {
                    *state = DownlinkState::Synced;
                    Some(
                        event_out
                            .send_item(LaneEvent(value_state.clone(), true))
                            .await,
                    )
                }
            },
            ValueLaneOperation::Message(LaneMessage::ValueUpdated(v)) => match *state {
                DownlinkState::Unlinked => Some(Ok(())),
                DownlinkState::Linked => {
                    *value_state = Arc::new(v);
                    Some(Ok(()))
                }
                DownlinkState::Synced => {
                    *value_state = Arc::new(v);
                    Some(
                        event_out
                            .send_item(LaneEvent(value_state.clone(), false))
                            .await,
                    )
                }
            },
            ValueLaneOperation::Message(LaneMessage::Unlinked) => {
                *state = DownlinkState::Unlinked;
                None
            }
            ValueLaneOperation::Set(v) => {
                *value_state = Arc::new(v);
                match command_out
                    .send_item(LaneCommand::SetValue(value_state.clone()))
                    .await
                {
                    Ok(_) => Some(
                        event_out
                            .send_item(LaneEvent(value_state.clone(), true))
                            .await,
                    ),
                    e @ Err(_) => Some(e),
                }
            }
            ValueLaneOperation::Close => {
                *state = DownlinkState::Unlinked;
                let send_unlink: Result<(), E> = command_out.send_item(LaneCommand::Unlink).await;
                match send_unlink {
                    e @ Err(_) => Some(e),
                    _ => None,
                }
            }
        }
    }
}

/// A task that continuously reads incoming events and set operations and applies them to a state
/// variable. Each time the state is updated it is emitted on an output sink and each time it is
/// locally set it is announced on a command sink. It also has a callback which can be used to
/// stop the process from another task.
///
/// Aleternative imperative implementation.
#[allow(dead_code)]
fn make_lane_task_imp<E, Ops, Cmd, Ev>(
    init: Value,
    ops: Ops,
    cmd_output: Cmd,
    event_output: Ev,
) -> impl FusedFuture<Output = Result<(), E>>
where
    E: Send + 'static,
    Ops: Stream<Item = ValueLaneOperation> + Send + 'static,
    Cmd: for<'a> ItemSink<'a, LaneCommand, Error = E> + Send + 'static,
    Ev: for<'a> ItemSink<'a, LaneEvent, Error = E> + Send + 'static,
{
    async move {
        pin_mut!(ops);
        let mut ops_str: Pin<&mut Ops> = ops;
        let mut model = ValueLaneModel::new(init, cmd_output, event_output);

        loop {
            if let Some(op) = ops_str.next().await {
                match model.handle_operation(op).await {
                    Some(e @ Err(_)) => break e,
                    None => break Ok(()),
                    _ => {}
                }
            } else {
                break Ok(());
            }
        }
    }
    .fuse()
}
