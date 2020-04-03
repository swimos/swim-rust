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

use futures::{future, stream, Stream};
use futures_util::select_biased;
use pin_utils::pin_mut;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use common::sink::item;
use common::sink::item::{BoxItemSink, ItemSender, ItemSink, MpscSend};
use futures::stream::{BoxStream, FusedStream};
use std::fmt::{Debug, Display, Formatter};
use tokio::sync::broadcast;
use tokio::sync::watch;

pub mod any;
pub mod buffered;
pub mod dropping;
pub mod model;
pub mod queue;
pub mod raw;
pub mod subscription;
pub mod watch_adapter;

pub(self) use self::raw::create_downlink;
use crate::router::RoutingError;
use common::topic::{BoxTopic, Topic, TopicError};
use futures::future::BoxFuture;

/// Shared trait for all Warp downlinks. `Act` is the type of actions that can be performed on the
/// downlink locally and `Upd` is the type of updates that an be observed on the client side.
pub trait Downlink<Act, Upd: Clone>: Topic<Upd> + ItemSender<Act, DownlinkError> {
    /// Type of the topic which can be used to subscribe to the downlink.
    type DlTopic: Topic<Upd>;

    /// Type of the sink that can be used to apply actions to the downlink.
    type DlSink: for<'a> ItemSink<'a, Act, Error = DownlinkError>;

    /// Split the downlink into a topic and sink.
    fn split(self) -> (Self::DlTopic, Self::DlSink);

    /// Box the downlink so that it can be used dynamically.
    fn boxed(self) -> BoxedDownlink<Act, Upd>
    where
        Self: Sized,
        Self::DlTopic: Sized + 'static,
        Self::DlSink: Sized + 'static,
        Upd: Send + 'static,
        Act: 'static,
    {
        let (topic, sink) = self.split();
        let boxed_topic = topic.boxed_topic();
        let boxed_sink = item::boxed(sink);
        BoxedDownlink {
            topic: boxed_topic,
            sink: boxed_sink,
        }
    }
}

pub struct BoxedDownlink<Act, Upd: Clone> {
    topic: BoxTopic<Upd>,
    sink: BoxItemSink<Act, DownlinkError>,
}

impl<Act, Upd: Clone + 'static> Topic<Upd> for BoxedDownlink<Act, Upd> {
    type Receiver = BoxStream<'static, Upd>;
    type Fut = BoxFuture<'static, Result<BoxStream<'static, Upd>, TopicError>>;

    fn subscribe(&mut self) -> Self::Fut {
        self.topic.subscribe()
    }
}

impl<'a, Act, Upd: Clone> ItemSink<'a, Act> for BoxedDownlink<Act, Upd> {
    type Error = DownlinkError;
    type SendFuture = BoxFuture<'a, Result<(), DownlinkError>>;

    fn send_item(&'a mut self, value: Act) -> Self::SendFuture {
        self.sink.send_item(value)
    }
}

impl<Act, Upd: Clone + 'static> Downlink<Act, Upd> for BoxedDownlink<Act, Upd> {
    type DlTopic = BoxTopic<Upd>;
    type DlSink = BoxItemSink<Act, DownlinkError>;

    fn split(self) -> (Self::DlTopic, Self::DlSink) {
        let BoxedDownlink { topic, sink } = self;
        (topic, sink)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DownlinkError {
    DroppedChannel,
    TaskPanic,
    TransitionError,
}

impl From<RoutingError> for DownlinkError {
    fn from(e: RoutingError) -> Self {
        match e {
            RoutingError::RouterDropped => DownlinkError::DroppedChannel,
        }
    }
}

impl Display for DownlinkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DownlinkError::DroppedChannel => write!(
                f,
                "An internal channel was dropped and the downlink is now closed."
            ),
            DownlinkError::TaskPanic => write!(f, "The downlink task panicked."),
            DownlinkError::TransitionError => {
                write!(f, "The downlink state machine produced and error.")
            }
        }
    }
}

impl std::error::Error for DownlinkError {}

impl<T> From<mpsc::error::SendError<T>> for DownlinkError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for DownlinkError {
    fn from(_: mpsc::error::TrySendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<watch::error::SendError<T>> for DownlinkError {
    fn from(_: watch::error::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl From<item::SendError> for DownlinkError {
    fn from(_: item::SendError) -> Self {
        DownlinkError::DroppedChannel
    }
}

impl<T> From<broadcast::SendError<T>> for DownlinkError {
    fn from(_: broadcast::SendError<T>) -> Self {
        DownlinkError::DroppedChannel
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DownlinkState {
    Unlinked,
    Linked,
    Synced,
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
}

/// The state of a downlink and the receivers of events that can be generated by it.
#[derive(Clone, Debug)]
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
}

#[derive(Clone, PartialEq, Eq, Debug)]
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

    fn then_terminate(mut self) -> Self {
        self.terminate = true;
        self
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TransitionError {
    ReceiverDropped,
    SideEffectFailed,
    IllegalTransition(String),
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

#[derive(Clone, PartialEq, Eq, Debug)]
struct BasicResponse<Ev, Cmd> {
    event: Option<Ev>,
    command: Option<Cmd>,
    error: Option<TransitionError>,
}

impl<Ev, Cmd> BasicResponse<Ev, Cmd> {
    fn none() -> Self {
        BasicResponse {
            event: None,
            command: None,
            error: None,
        }
    }

    fn of(event: Ev, command: Cmd) -> Self {
        BasicResponse {
            event: Some(event),
            command: Some(command),
            error: None,
        }
    }

    fn with_error(mut self, err: TransitionError) -> Self {
        self.error = Some(err);
        self
    }
}

impl<Ev, Cmd> From<BasicResponse<Ev, Cmd>> for Response<Ev, Cmd> {
    fn from(basic: BasicResponse<Ev, Cmd>) -> Self {
        let BasicResponse {
            event,
            command,
            error,
        } = basic;
        Response {
            event: event.map(|e| Event(e, true)),
            command: command.map(Command::Action),
            error,
            terminate: false,
        }
    }
}

/// This trait is for simple, stateful downlinks that follow the standard synchronization model.
trait BasicStateMachine<M, A> {
    /// Type of events that will be issued to the owner of the downlink.
    type Ev;
    /// Type of commands that will be sent out to the Warp connection.
    type Cmd;

    /// Generate the initial event when the downlink enters the [`Synced`] state.
    fn on_sync(&self) -> Self::Ev;

    /// Update the state with a message, received between [`Linked`] and [`Synced`'].
    fn handle_message_unsynced(&mut self, message: M);

    /// Update the state with a message when in the [`Synced`] state, potentially generating an
    /// event.
    fn handle_message(&mut self, message: M) -> Option<Self::Ev>;

    /// Handle a local action potentially generating an event and/or a command and/or an error.
    fn handle_action(&mut self, action: A) -> BasicResponse<Self::Ev, Self::Cmd>;
}

//Adapter to make a BasicStateMachine into a StateMachine.
impl<State, M, A> StateMachine<M, A> for State
where
    State: BasicStateMachine<M, A>,
{
    type Ev = <State as BasicStateMachine<M, A>>::Ev;
    type Cmd = <State as BasicStateMachine<M, A>>::Cmd;

    fn handle_operation(
        model: &mut Model<Self>,
        op: Operation<M, A>,
    ) -> Response<Self::Ev, Self::Cmd> {
        let Model { data_state, state } = model;
        match op {
            Operation::Start => {
                if *state == DownlinkState::Synced {
                    Response::none()
                } else {
                    Response::for_command(Command::Sync)
                }
            }
            Operation::Message(message) => match message {
                Message::Linked => {
                    *state = DownlinkState::Linked;
                    Response::none()
                }
                Message::Synced => {
                    let old_state = *state;
                    *state = DownlinkState::Synced;
                    if old_state == DownlinkState::Synced {
                        Response::none()
                    } else {
                        Response::for_event(Event(data_state.on_sync(), false))
                    }
                }
                Message::Action(msg) => match *state {
                    DownlinkState::Unlinked => Response::none(),
                    DownlinkState::Linked => {
                        data_state.handle_message_unsynced(msg);
                        Response::none()
                    }
                    DownlinkState::Synced => match data_state.handle_message(msg) {
                        Some(ev) => Response::for_event(Event(ev, false)),
                        _ => Response::none(),
                    },
                },
                Message::Unlinked => {
                    *state = DownlinkState::Unlinked;
                    Response::none().then_terminate()
                }
            },
            Operation::Action(action) => data_state.handle_action(action).into(),
        }
    }
}
