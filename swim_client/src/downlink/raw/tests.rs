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

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::*;
use crate::downlink::TransitionError;
use std::time::Instant;
use swim_common::routing::RoutingError;
use swim_common::sink::item::*;
use tokio::stream::StreamExt;

struct State(i32);

#[derive(Debug)]
struct Msg(i32, Option<oneshot::Sender<Instant>>);

impl Msg {
    fn of(n: i32) -> Self {
        Msg(n, None)
    }

    fn when_processed(mut self, cb: oneshot::Sender<Instant>) -> Self {
        self.1 = Some(cb);
        self
    }
}

#[derive(Debug)]
struct AddTo(i32, Option<oneshot::Sender<Instant>>);

impl AddTo {
    fn of(n: i32) -> Self {
        AddTo(n, None)
    }

    fn when_processed(mut self, cb: oneshot::Sender<Instant>) -> Self {
        self.1 = Some(cb);
        self
    }
}

fn with_error<Ev, Cmd>(mut response: Response<Ev, Cmd>, err: TransitionError) -> Response<Ev, Cmd> {
    response.error = Some(err);
    response
}

struct TestStateMachine {
    dl_start_state: DownlinkState,
    start_response: Response<i32, i32>,
}

impl TestStateMachine {
    fn new(dl_start_state: DownlinkState, start_response: Response<i32, i32>) -> Self {
        TestStateMachine {
            dl_start_state,
            start_response,
        }
    }
}

impl StateMachine<State, Msg, AddTo> for TestStateMachine {
    type Ev = i32;
    type Cmd = i32;

    fn init_state(&self) -> State {
        State(0)
    }

    fn dl_start_state(&self) -> DownlinkState {
        self.dl_start_state
    }

    fn handle_operation(
        &self,
        dl_state: &mut DownlinkState,
        model: &mut State,
        op: Operation<Msg, AddTo>,
    ) -> Result<Response<Self::Ev, Self::Cmd>, DownlinkError> {
        match op {
            Operation::Start => Ok(self.start_response.clone()),
            Operation::Message(Message::Linked) => {
                *dl_state = DownlinkState::Linked;
                Ok(Response::none())
            }
            Operation::Message(Message::Synced) => {
                let prev = *dl_state;
                *dl_state = DownlinkState::Synced;
                Ok(if prev != DownlinkState::Synced {
                    Response::for_event(Event::Remote(model.0))
                } else {
                    Response::none()
                })
            }
            Operation::Message(Message::Unlinked) => {
                *dl_state = DownlinkState::Unlinked;
                Ok(Response::none())
            }
            Operation::Message(Message::Action(Msg(n, maybe_cb))) => {
                let result = if n < 0 {
                    Err(DownlinkError::MalformedMessage)
                } else {
                    if *dl_state != DownlinkState::Unlinked {
                        model.0 = n;
                    }
                    Ok(if *dl_state == self.dl_start_state() {
                        Response::for_event(Event::Remote(model.0))
                    } else {
                        Response::none()
                    })
                };

                match maybe_cb {
                    Some(cb) => match cb.send(Instant::now()) {
                        Ok(_) => result,
                        Err(_) => {
                            result.map(|resp| with_error(resp, TransitionError::ReceiverDropped))
                        }
                    },
                    _ => result,
                }
            }
            Operation::Message(Message::BadEnvelope(_)) => Err(DownlinkError::MalformedMessage),
            Operation::Action(AddTo(n, maybe_cb)) => {
                let next = model.0 + n;
                let resp = if next < 0 {
                    with_error(
                        Response::none(),
                        TransitionError::IllegalTransition("State cannot be negative.".to_owned()),
                    )
                } else {
                    model.0 = next;
                    response_of(Event::Local(next), Command::Action(next))
                };
                Ok(match maybe_cb {
                    Some(cb) => match cb.send(Instant::now()) {
                        Ok(_) => resp,
                        Err(_) => with_error(resp, TransitionError::ReceiverDropped),
                    },
                    _ => resp,
                })
            }
            Operation::Error(e) => {
                if e.is_fatal() {
                    return Err(DownlinkError::DroppedChannel);
                } else {
                    *dl_state = DownlinkState::Unlinked;
                    Ok(Response::for_command(Command::Sync))
                }
            }
        }
    }
}

fn response_of<Ev, Cmd>(event: Event<Ev>, command: Command<Cmd>) -> Response<Ev, Cmd> {
    Response {
        event: Some(event),
        command: Some(command),
        error: None,
        terminate: false,
    }
}

type Str<T> = mpsc::Receiver<T>;
type Snk<T> = mpsc::Sender<T>;

async fn make_test_dl_custom_on_invalid(
    on_invalid: OnInvalidMessage,
    dl_start_state: DownlinkState,
    start_response: Response<i32, i32>,
) -> (
    RawDownlink<Snk<AddTo>, Str<Event<i32>>>,
    Snk<Result<Message<Msg>, RoutingError>>,
    Str<Command<i32>>,
) {
    let (tx_in, rx_in) = mpsc::channel(10);
    let (tx_out, rx_out) = mpsc::channel::<Command<i32>>(10);
    let downlink = create_downlink(
        TestStateMachine::new(dl_start_state, start_response),
        rx_in,
        for_mpsc_sender::<Command<i32>, RoutingError>(tx_out),
        NonZeroUsize::new(10).unwrap(),
        NonZeroUsize::new(256).unwrap(),
        on_invalid,
    );
    (downlink, tx_in, rx_out)
}

async fn make_test_sync_dl() -> (
    RawDownlink<Snk<AddTo>, Str<Event<i32>>>,
    Snk<Result<Message<Msg>, RoutingError>>,
    Str<Command<i32>>,
) {
    make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Synced,
        Response::for_command(Command::Sync),
    )
    .await
}

#[tokio::test]
async fn sync_on_startup() {
    let (_dl, _messages, mut commands) = make_test_sync_dl().await;

    let first_cmd = commands.next().await;
    assert_eq!(first_cmd, Some(Command::Sync));
}

#[tokio::test]
async fn event_on_sync() {
    let (dl, mut messages, _commands) = make_test_sync_dl().await;
    let (_dl_tx, mut dl_rx) = dl.split();

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn ignore_update_before_link() {
    let (dl, mut messages, _commands) = make_test_sync_dl().await;
    let (_dl_tx, mut dl_rx) = dl.split();

    assert!(messages
        .send(Ok(Message::Action(Msg::of(12))))
        .await
        .is_ok());
    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn apply_updates_between_link_and_sync() {
    let (dl, mut messages, _commands) = make_test_sync_dl().await;
    let (_dl_tx, mut dl_rx) = dl.split();

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages
        .send(Ok(Message::Action(Msg::of(12))))
        .await
        .is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_eq!(first_ev, Some(Event::Remote(12)));
}

/// Pre-synchronizes a downlink for tests that require the ['DownlinkState::Synced'] state.
async fn sync_dl(
    init: Msg,
    messages: &mut Snk<Result<Message<Msg>, RoutingError>>,
    events: &mut Str<Event<i32>>,
    commands: &mut Str<Command<i32>>,
) {
    let n = init.0;
    let first_cmd = commands.recv().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Action(init))).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = events.recv().await;
    assert_eq!(first_ev, Some(Event::Remote(n)));
}

#[tokio::test]
async fn updates_processed_when_synced() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (_dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    assert!(messages
        .send(Ok(Message::Action(Msg::of(10))))
        .await
        .is_ok());
    assert!(messages
        .send(Ok(Message::Action(Msg::of(20))))
        .await
        .is_ok());
    assert!(messages
        .send(Ok(Message::Action(Msg::of(30))))
        .await
        .is_ok());

    assert_eq!(events.recv().await, Some(Event::Remote(10)));
    assert_eq!(events.recv().await, Some(Event::Remote(20)));
    assert_eq!(events.recv().await, Some(Event::Remote(30)));
}

#[tokio::test]
async fn actions_processed_when_synced() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    assert!(dl_tx.send(AddTo::of(4)).await.is_ok());

    assert_eq!(events.recv().await, Some(Event::Local(5)));
    assert_eq!(commands.recv().await, Some(Command::Action(5)));
}

#[tokio::test]
async fn actions_paused_when_not_synced() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    //Send an action.
    assert!(dl_tx.send(action).await.is_ok());
    //Then sync the downlink afterwards.
    sync_dl(msg, &mut messages, &mut events, &mut commands).await;

    let action_at = act_rx.await;
    let msg_at = msg_rx.await;
    assert!(action_at.is_ok());
    assert!(msg_at.is_ok());
    assert!(msg_at.unwrap() <= action_at.unwrap());

    assert_eq!(events.recv().await, Some(Event::Local(17)));
    assert_eq!(commands.recv().await, Some(Command::Action(17)));
}

#[tokio::test]
async fn actions_paused_when_unlinked() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Unlink the downlink.
    assert!(messages.send(Ok(Message::Unlinked)).await.is_ok());
    //Then send an action.
    assert!(dl_tx.send(action).await.is_ok());
    //Link but don't yet sync.
    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    //Send an update that we expect to be handled before the action.
    assert!(messages.send(Ok(Message::Action(msg))).await.is_ok());
    //Re-sync which we expect to unblock the action.
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    //Check that the events happened in the correct order.
    let action_at = act_rx.await;
    let msg_at = msg_rx.await;
    assert!(action_at.is_ok());
    assert!(msg_at.is_ok());
    assert!(msg_at.unwrap() <= action_at.unwrap());

    //Event generated when we re-sync.
    assert_eq!(events.recv().await, Some(Event::Remote(5)));
    //Event and command generated after the action is applied.
    assert_eq!(events.recv().await, Some(Event::Local(17)));
    assert_eq!(commands.recv().await, Some(Command::Action(17)));
}

#[tokio::test]
async fn errors_propagate() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();

    let action = AddTo::of(-100).when_processed(act_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Send an action that will trigger an error.
    assert!(dl_tx.send(action).await.is_ok());

    //Wait for the action the be executed.
    assert!(act_rx.await.is_ok());

    let stop_res = dl_tx.task.task_handle().await_stopped().await;

    assert!(stop_res.is_err());
    assert_eq!(stop_res.err().unwrap(), DownlinkError::TransitionError);
}

#[tokio::test]
async fn terminates_on_invalid() {
    let (dl, mut messages, _commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Synced,
        Response::for_command(Command::Sync),
    )
    .await;
    let (dl_tx, dl_rx) = dl.split();

    let _events = dl_rx.event_stream;

    let (msg_tx, msg_rx) = oneshot::channel();

    let msg = Msg(-1, Some(msg_tx));

    assert!(messages.send(Ok(Message::Action(msg))).await.is_ok());
    //Wait for the message to be processed.
    assert!(msg_rx.await.is_ok());

    let stop_res = dl_tx.task.task_handle().await_stopped().await;

    assert!(stop_res.is_err());
    assert_eq!(stop_res.err().unwrap(), DownlinkError::MalformedMessage);
}

#[tokio::test]
async fn continues_on_invalid() {
    let (dl, mut messages, mut commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Ignore,
        DownlinkState::Synced,
        Response::for_command(Command::Sync),
    )
    .await;
    let (_dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (msg_tx, msg_rx) = oneshot::channel();

    let msg = Msg(-1, Some(msg_tx));

    assert!(messages.send(Ok(Message::Action(msg))).await.is_ok());
    //Wait for the message to be processed.
    assert!(msg_rx.await.is_ok());

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;
}

#[tokio::test]
async fn unlinks_on_unreachable_host() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (_dl_tx, mut dl_rx) = dl.split();

    let first_cmd = commands.next().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::HostUnreachable))
        .await
        .is_ok());

    let first_cmd = commands.recv().await;

    assert_eq!(first_cmd, Some(Command::Sync));
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn queues_on_unreachable_host() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();
    let mut events = dl_rx.event_stream;

    let first_cmd = commands.next().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::HostUnreachable))
        .await
        .is_ok());

    assert_eq!(commands.recv().await, Some(Command::Sync));

    assert!(dl_tx.send(AddTo::of(1)).await.is_ok());
    assert!(dl_tx.send(AddTo::of(2)).await.is_ok());
    assert!(dl_tx.send(AddTo::of(3)).await.is_ok());
    assert!(dl_tx.send(AddTo::of(4)).await.is_ok());

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    assert_eq!(commands.recv().await, Some(Command::Action(1)));
    assert_eq!(commands.recv().await, Some(Command::Action(3)));
    assert_eq!(commands.recv().await, Some(Command::Action(6)));
    assert_eq!(commands.recv().await, Some(Command::Action(10)));

    assert_eq!(events.recv().await, Some(Event::Remote(0)));
    assert_eq!(events.recv().await, Some(Event::Local(1)));
    assert_eq!(events.recv().await, Some(Event::Local(3)));
    assert_eq!(events.recv().await, Some(Event::Local(6)));
    assert_eq!(events.recv().await, Some(Event::Local(10)));
}

#[tokio::test]
async fn terminates_when_router_dropped() {
    let (dl, mut messages, mut commands) = make_test_sync_dl().await;
    let (dl_tx, _dl_rx) = dl.split();
    let first_cmd = commands.next().await;

    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::RouterDropped))
        .await
        .is_ok());

    let stop_res = dl_tx.task.task_handle().await_stopped().await;
    assert_eq!(stop_res.err().unwrap(), DownlinkError::DroppedChannel);
}

#[tokio::test]
async fn action_received_before_synced() {
    let (dl, mut messages, mut commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Linked,
        Response::for_command(Command::Link),
    )
    .await;

    let (mut dl_tx, _dl_rx) = dl.split();
    assert!(messages.send(Ok(Message::Linked)).await.is_ok());

    dl_tx.send(AddTo::of(4)).await.unwrap();

    let first_cmd = commands.next().await;
    let second_cmd = commands.next().await;
    assert_eq!(first_cmd, Some(Command::Link));
    assert_eq!(second_cmd, Some(Command::Action(4)));
}

#[tokio::test]
async fn action_received_before_linked() {
    let (dl, _messages, mut commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Unlinked,
        Response::none(),
    )
    .await;

    let (mut dl_tx, _dl_rx) = dl.split();

    dl_tx.send(AddTo::of(4)).await.unwrap();

    let first_cmd = commands.next().await;
    assert_eq!(first_cmd, Some(Command::Action(4)));
}
