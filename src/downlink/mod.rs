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

use std::pin::Pin;

use futures::executor::block_on;
use futures::{future, stream, Stream, StreamExt};
use futures_util::select_biased;
use pin_utils::pin_mut;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::sink::item;
use crate::sink::item::ItemSink;
use futures::stream::FusedStream;
use std::fmt::Debug;

pub mod map;
pub mod value;

pub struct Sender<Err: Debug, S> {
    /// A sink for local actions (sets, insertions, etc.)
    pub set_sink: S,
    /// The task running the downlink.
    task: Option<DownlinkTask<Err>>,
}

impl<Err: Debug, S> Drop for Sender<Err, S> {
    fn drop(&mut self) {
        match self.task.take() {
            Some(t) => {
                block_on(t.stop()).unwrap();
            }
            _ => {}
        }
    }
}

impl<Err: Debug, S> Sender<Err, S> {
    /// Stop the downlink from running.
    pub async fn stop(mut self) -> Result<(), Err> {
        match (&mut self).task.take() {
            Some(t) => t.stop().await,
            _ => Ok(()),
        }
    }
}

pub struct Receiver<R> {
    /// A stream of events generated by the downlink.
    pub event_stream: R,
}

/// Type containing the components of a running downlink.
pub struct Downlink<Err: Debug, S, R> {
    pub receiver: Receiver<R>,
    pub sender: Sender<Err, S>,
}

impl<Err: Debug, S, R> Downlink<Err, S, R> {
    //Private as downlinks should only be created by methods in this module and its children.
    fn new(set_sink: S, event_stream: R, task: Option<DownlinkTask<Err>>) -> Downlink<Err, S, R> {
        Downlink {
            receiver: Receiver { event_stream },
            sender: Sender { set_sink, task },
        }
    }

    /// Stop the downlink from running.
    pub async fn stop(self) -> Result<(), Err> {
        self.sender.stop().await
    }

    pub fn split(self) -> (Sender<Err, S>, Receiver<R>) {
        let Downlink { sender, receiver } = self;
        (sender, receiver)
    }
}

/// Asynchronously create a new downlink from a stream of input events, writing to a sink of
/// commands.
fn create_downlink<Err, M, A, State, Updates, Commands>(
    init: State,
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> Downlink<Err, mpsc::Sender<A>, mpsc::Receiver<Event<State::Ev>>>
where
    M: Send + 'static,
    A: Send + 'static,
    State: StateMachine<M, A> + Send + 'static,
    State::Ev: Send + 'static,
    State::Cmd: Send + 'static,
    Err: From<item::MpscErr<Event<State::Ev>>> + Send + Debug + 'static,
    Updates: Stream<Item = Message<M>> + Send + 'static,
    Commands: for<'b> ItemSink<'b, Command<State::Cmd>, Error = Err> + Send + 'static,
{
    let model = Model::new(init);
    let (act_tx, act_rx) = mpsc::channel::<A>(buffer_size);
    let (event_tx, event_rx) = mpsc::channel::<Event<State::Ev>>(buffer_size);
    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    let event_sink = item::for_mpsc_sender::<_, Err>(event_tx);

    // The task that maintains the internal state of the lane.
    let lane_task = model.make_downlink_task(
        combine_inputs(update_stream, stop_rx),
        act_rx.fuse(),
        cmd_sink,
        event_sink,
    );

    let join_handle = tokio::task::spawn(lane_task);

    let dl_task = DownlinkTask {
        join_handle,
        stop_trigger: stop_tx,
    };

    Downlink::new(act_tx, event_rx, Some(dl_task))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DownlinkState {
    Unlinked,
    Linked,
    Synced,
}

struct DownlinkTask<E> {
    join_handle: JoinHandle<Result<(), E>>,
    stop_trigger: oneshot::Sender<()>,
}

impl<E> DownlinkTask<E> {
    async fn stop(self) -> Result<(), E> {
        match self.stop_trigger.send(()) {
            Ok(_) => match self.join_handle.await {
                Ok(r) => r,
                Err(_) => Ok(()), //TODO Ignoring the case where the downlink task panicked. Can maybe do better?
            },
            Err(_) => Ok(()),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message<M> {
    Linked,
    Synced,
    Action(M),
    Unlinked,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Command<A> {
    Sync,
    Action(A),
    Unlink,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Event<A>(pub A, pub bool);

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Operation<M, A> {
    Start,
    Message(Message<M>),
    Action(A),
    Close,
}

/// The state of a downlink and the receivers of events that can be generated by it.
struct Model<State> {
    state: DownlinkState,
    data_state: State,
}

impl<State> Model<State> {
    fn new(init: State) -> Model<State> {
        Model {
            state: DownlinkState::Unlinked,
            data_state: init,
        }
    }

    /// A task that consumes the operations applied to the downlink, updates the state and
    /// forwards events and commands to a pair of output sinks.
    async fn make_downlink_task<E, M, A, Ops, Acts, Commands, Events>(
        mut self,
        ops: Ops,
        acts: Acts,
        mut cmd_sink: Commands,
        mut ev_sink: Events,
    ) -> Result<(), E>
    where
        State: StateMachine<M, A>,
        Ops: FusedStream<Item = Operation<M, A>> + Send + 'static,
        Acts: FusedStream<Item = A> + Send + 'static,
        Commands: for<'b> ItemSink<'b, Command<State::Cmd>, Error = E>,
        Events: for<'b> ItemSink<'b, Event<State::Ev>, Error = E>,
    {
        pin_mut!(ops);
        pin_mut!(acts);
        let mut ops_str: Pin<&mut Ops> = ops;
        let mut act_str: Pin<&mut Acts> = acts;

        let mut read_act = false;

        loop {
            let next_op = if self.state == DownlinkState::Synced {
                if read_act {
                    read_act = false;
                    select_biased! {
                        act_op = act_str.next() => act_op.map(Operation::Action),
                        upd_op = ops_str.next() => upd_op,
                    }
                } else {
                    read_act = true;
                    select_biased! {
                        upd_op = ops_str.next() => upd_op,
                        act_op = act_str.next() => act_op.map(Operation::Action),
                    }
                }
            } else {
                ops_str.next().await
            };

            if let Some(op) = next_op {
                let Response {
                    event,
                    command,
                    error,
                    terminate,
                } = StateMachine::handle_operation(&mut self, op);
                let result = match (event, command) {
                    (Some(ev), Some(cmd)) => match ev_sink.send_item(ev).await {
                        Ok(()) => cmd_sink.send_item(cmd).await,
                        e @ Err(_) => e,
                    },
                    (Some(event), _) => ev_sink.send_item(event).await,
                    (_, Some(command)) => cmd_sink.send_item(command).await,
                    _ => Ok(()),
                };

                if error.is_some() {
                    break Ok(()); //TODO Handle this properly.
                } else if terminate || result.is_err() {
                    break result;
                }
            } else {
                break Ok(());
            }
        }
    }
}

struct Response<Ev, Cmd> {
    event: Option<Event<Ev>>,
    command: Option<Command<Cmd>>,
    error: Option<TransitionError>,
    terminate: bool,
}

impl<Ev, Cmd> Response<Ev, Cmd> {
    fn none() -> Response<Ev, Cmd> {
        Response {
            event: None,
            command: None,
            error: None,
            terminate: false,
        }
    }

    fn for_event(event: Event<Ev>) -> Response<Ev, Cmd> {
        Response {
            event: Some(event),
            command: None,
            error: None,
            terminate: false,
        }
    }

    fn for_command(command: Command<Cmd>) -> Response<Ev, Cmd> {
        Response {
            event: None,
            command: Some(command),
            error: None,
            terminate: false,
        }
    }

    fn of(event: Event<Ev>, command: Command<Cmd>) -> Response<Ev, Cmd> {
        Response {
            event: Some(event),
            command: Some(command),
            error: None,
            terminate: false,
        }
    }

    fn then_terminate(mut self) -> Self {
        self.terminate = true;
        self
    }

    fn with_error(mut self, err: TransitionError) -> Self {
        self.error = Some(err);
        self
    }
}

pub enum TransitionError {
    ReceiverDropped,
    SideEffectFailed,
}

/// This trait defines the interface that must be implemented for the state type of a downlink.
trait StateMachine<M, A>: Sized {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd;

    /// For an operation on the downlink, generate output messages.
    fn handle_operation(
        model: &mut Model<Self>,
        op: Operation<M, A>,
    ) -> Response<Self::Ev, Self::Cmd>;
}

/// Combines together updates received from the Warp connection  and the stop signal
/// into a single stream.
fn combine_inputs<M, A, Upd>(
    updates: Upd,
    stop: oneshot::Receiver<()>,
) -> impl FusedStream<Item = Operation<M, A>> + Send + 'static
where
    M: Send + 'static,
    A: Send + 'static,
    Upd: Stream<Item = Message<M>> + Send + 'static,
{
    let upd_operations = updates.map(Operation::Message);
    let close_operations = stream::once(stop).map(|_| Operation::Close);

    let init = stream::once(future::ready(Operation::Start));

    init.chain(stream::select(close_operations, upd_operations))
}
