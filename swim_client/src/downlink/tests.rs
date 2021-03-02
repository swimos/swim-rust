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

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::*;
use crate::downlink::state_machine::ResponseResult;
use crate::downlink::{Downlink, DownlinkState};
use futures::StreamExt;
use std::num::NonZeroUsize;
use std::time::Instant;
use swim_common::routing::RoutingError;
use swim_common::sink::item;

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

struct TestStateMachine {
    dl_start_state: DownlinkState,
    start_response: Option<Command<i32>>,
}

impl TestStateMachine {
    fn new(dl_start_state: DownlinkState, start_response: Option<Command<i32>>) -> Self {
        TestStateMachine {
            dl_start_state,
            start_response,
        }
    }
}

impl DownlinkStateMachine<Msg, AddTo> for TestStateMachine {
    type State = (DownlinkState, State);
    type Update = i32;
    type Report = i32;

    fn initialize(&self) -> (Self::State, Option<Command<Self::Update>>) {
        (
            (DownlinkState::Unlinked, State(0)),
            self.start_response.clone(),
        )
    }

    fn handle_actions(&self, state: &Self::State) -> bool {
        let (dl_state, State(_data_state)) = state;
        match self.dl_start_state {
            DownlinkState::Unlinked => true,
            DownlinkState::Linked => {
                *dl_state == DownlinkState::Linked || *dl_state == DownlinkState::Synced
            }
            DownlinkState::Synced => *dl_state == DownlinkState::Synced,
        }
    }

    fn handle_event(
        &self,
        state: &mut Self::State,
        event: Message<Msg>,
    ) -> EventResult<Self::Report> {
        let do_action = self.handle_actions(state);
        let (dl_state, State(data_state)) = state;
        match event {
            Message::Linked => {
                *dl_state = DownlinkState::Linked;
                EventResult::default()
            }
            Message::Synced => {
                let prev = *dl_state;
                *dl_state = DownlinkState::Synced;
                if prev != DownlinkState::Synced {
                    EventResult::of(*data_state)
                } else {
                    EventResult::default()
                }
            }
            Message::Action(Msg(n, maybe_cb)) => {
                let result = if n < 0 {
                    EventResult::fail(DownlinkError::MalformedMessage)
                } else {
                    if *dl_state != DownlinkState::Unlinked {
                        *data_state = n;
                    }
                    if do_action {
                        EventResult::of(*data_state)
                    } else {
                        EventResult::default()
                    }
                };

                match maybe_cb {
                    Some(cb) => match cb.send(Instant::now()) {
                        Ok(_) => result,
                        Err(_) => EventResult {
                            result: Err(DownlinkError::TransitionError),
                            terminate: false,
                        },
                    },
                    _ => result,
                }
            }
            Message::Unlinked => {
                *dl_state = DownlinkState::Unlinked;
                EventResult::default()
            }
            Message::BadEnvelope(_) => EventResult::fail(DownlinkError::MalformedMessage),
        }
    }

    fn handle_request(
        &self,
        state: &mut Self::State,
        request: AddTo,
    ) -> ResponseResult<Self::Report, Self::Update> {
        let (_dl_state, State(data_state)) = state;
        let AddTo(n, maybe_cb) = request;
        let next = *data_state + n;
        let resp = if next < 0 {
            Err(DownlinkError::InvalidAction)
        } else {
            *data_state = next;
            Ok((next, next).into())
        };
        if let Some(cb) = maybe_cb {
            let _ = cb.send(Instant::now());
        }
        resp
    }

    fn finalize(&self, _state: &Self::State) -> Option<Command<Self::Update>> {
        Some(Command::Unlink)
    }
}

type Str<T> = mpsc::Receiver<T>;
type Snk<T> = mpsc::Sender<T>;

fn make_test_dl_custom_on_invalid(
    on_invalid: OnInvalidMessage,
    dl_start_state: DownlinkState,
    start_response: Option<Command<i32>>,
) -> (
    RawDownlink<AddTo, i32>,
    topic::Receiver<Event<i32>>,
    Snk<Result<Message<Msg>, RoutingError>>,
    Str<Command<i32>>,
) {
    let (tx_in, rx_in) = mpsc::channel(10);
    let (tx_out, rx_out) = mpsc::channel::<Command<i32>>(10);

    let config = DownlinkConfig {
        buffer_size: NonZeroUsize::new(10).unwrap(),
        yield_after: NonZeroUsize::new(256).unwrap(),
        on_invalid,
    };

    let (downlink, dl_rx) = create_downlink(
        TestStateMachine::new(dl_start_state, start_response),
        ReceiverStream::new(rx_in),
        item::for_mpsc_sender(tx_out).map_err_into(),
        config,
    );

    (downlink, dl_rx, tx_in, rx_out)
}

fn make_test_sync_dl() -> (
    RawDownlink<AddTo, i32>,
    topic::Receiver<Event<i32>>,
    Snk<Result<Message<Msg>, RoutingError>>,
    Str<Command<i32>>,
) {
    make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Synced,
        Some(Command::Sync),
    )
}

#[tokio::test]
async fn initial_command_on_startup() {
    let (_dl, _rx, _messages, mut commands) = make_test_sync_dl();

    let first_cmd = commands.recv().await;
    assert_eq!(first_cmd, Some(Command::Sync));
}

#[tokio::test]
async fn event_on_state_change() {
    let (_dl, mut dl_rx, messages, _commands) = make_test_sync_dl();

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.recv().await.map(|g| g.clone());
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn state_based_action_filter() {
    let (_dl, mut dl_rx, messages, _commands) = make_test_sync_dl();

    assert!(messages
        .send(Ok(Message::Action(Msg::of(12))))
        .await
        .is_ok());
    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.recv().await.map(|g| g.clone());
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn state_updates_with_no_report() {
    let (_dl, mut dl_rx, messages, _commands) = make_test_sync_dl();

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages
        .send(Ok(Message::Action(Msg::of(12))))
        .await
        .is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.recv().await.map(|g| g.clone());
    assert_eq!(first_ev, Some(Event::Remote(12)));
}

/// Pre-synchronizes a downlink for tests that require the ['DownlinkState::Synced'] state.
async fn sync_dl(
    init: Msg,
    messages: &mut Snk<Result<Message<Msg>, RoutingError>>,
    events: &mut topic::ReceiverStream<Event<i32>>,
    commands: &mut Str<Command<i32>>,
) {
    let n = init.0;
    let first_cmd = commands.recv().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Action(init))).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = events.next().await;
    assert_eq!(first_ev, Some(Event::Remote(n)));
}

#[tokio::test]
async fn process_messages() {
    let (_dl, rx, mut messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

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

    assert_eq!(events.next().await, Some(Event::Remote(10)));
    assert_eq!(events.next().await, Some(Event::Remote(20)));
    assert_eq!(events.next().await, Some(Event::Remote(30)));
}

#[tokio::test]
async fn actions_processed_when_not_filtered() {
    let (dl, rx, mut messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    assert!(dl.send(AddTo::of(4)).await.is_ok());

    assert_eq!(events.next().await, Some(Event::Local(5)));
    assert_eq!(commands.recv().await, Some(Command::Action(5)));
}

#[tokio::test]
async fn actions_paused_when_filtered() {
    let (dl, rx, mut messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    //Send an action.
    assert!(dl.send(action).await.is_ok());
    //Then sync the downlink afterwards.
    sync_dl(msg, &mut messages, &mut events, &mut commands).await;

    let action_at = act_rx.await;
    let msg_at = msg_rx.await;
    assert!(action_at.is_ok());
    assert!(msg_at.is_ok());
    assert!(msg_at.unwrap() <= action_at.unwrap());

    assert_eq!(events.next().await, Some(Event::Local(17)));
    assert_eq!(commands.recv().await, Some(Command::Action(17)));
}

#[tokio::test]
async fn actions_paused_when_unlinked() {
    let (dl, rx, mut messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Unlink the downlink.
    assert!(messages.send(Ok(Message::Unlinked)).await.is_ok());
    //Then send an action.
    assert!(dl.send(action).await.is_ok());
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
    assert_eq!(events.next().await, Some(Event::Remote(5)));
    //Event and command generated after the action is applied.
    assert_eq!(events.next().await, Some(Event::Local(17)));
    assert_eq!(commands.recv().await, Some(Command::Action(17)));
}

#[tokio::test]
async fn continue_after_failed_action() {
    let (dl, rx, mut messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

    let (act_tx, act_rx) = oneshot::channel();

    let action = AddTo::of(-100).when_processed(act_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Send an action that will trigger an error.
    assert!(dl.send(action).await.is_ok());

    //Wait for the action the be executed.
    let _ = act_rx.await;

    assert!(messages
        .send(Ok(Message::Action(Msg(12, None))))
        .await
        .is_ok());
    assert_eq!(events.next().await, Some(Event::Remote(12)));
}

#[tokio::test]
async fn terminates_on_invalid() {
    let (dl, _events, messages, _commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Terminate,
        DownlinkState::Synced,
        Some(Command::Sync),
    );

    let (msg_tx, msg_rx) = oneshot::channel();

    let msg = Msg(-1, Some(msg_tx));

    assert!(messages.send(Ok(Message::Action(msg))).await.is_ok());
    //Wait for the message to be processed.
    assert!(msg_rx.await.is_ok());

    let stop_res = dl.await_stopped().await;

    assert!(stop_res.is_ok());
    let r = stop_res.unwrap();
    assert!(r.is_err());
    assert!(matches!(&*r, Err(DownlinkError::MalformedMessage)));
}

#[tokio::test]
async fn continues_on_invalid() {
    let (_dl, rx, mut messages, mut commands) = make_test_dl_custom_on_invalid(
        OnInvalidMessage::Ignore,
        DownlinkState::Synced,
        Some(Command::Sync),
    );

    let mut events = rx.into_stream();

    let (msg_tx, msg_rx) = oneshot::channel();

    let msg = Msg(-1, Some(msg_tx));

    assert!(messages.send(Ok(Message::Action(msg))).await.is_ok());
    //Wait for the message to be processed.
    assert!(msg_rx.await.is_ok());

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;
}

#[tokio::test]
async fn unlinks_on_unreachable_host() {
    let (_dl, mut dl_rx, messages, mut commands) = make_test_sync_dl();

    let first_cmd = commands.recv().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::HostUnreachable))
        .await
        .is_ok());

    let first_cmd = commands.recv().await;

    assert_eq!(first_cmd, Some(Command::Sync));
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    let first_ev = dl_rx.recv().await.map(|g| (*g).clone());
    assert_eq!(first_ev, Some(Event::Remote(0)));
}

#[tokio::test]
async fn queues_on_unreachable_host() {
    let (dl, rx, messages, mut commands) = make_test_sync_dl();

    let mut events = rx.into_stream();

    let first_cmd = commands.recv().await;
    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::HostUnreachable))
        .await
        .is_ok());

    assert_eq!(commands.recv().await, Some(Command::Sync));

    assert!(dl.send(AddTo::of(1)).await.is_ok());
    assert!(dl.send(AddTo::of(2)).await.is_ok());
    assert!(dl.send(AddTo::of(3)).await.is_ok());
    assert!(dl.send(AddTo::of(4)).await.is_ok());

    assert!(messages.send(Ok(Message::Linked)).await.is_ok());
    assert!(messages.send(Ok(Message::Synced)).await.is_ok());

    assert_eq!(commands.recv().await, Some(Command::Action(1)));
    assert_eq!(commands.recv().await, Some(Command::Action(3)));
    assert_eq!(commands.recv().await, Some(Command::Action(6)));
    assert_eq!(commands.recv().await, Some(Command::Action(10)));

    assert_eq!(events.next().await, Some(Event::Remote(0)));
    assert_eq!(events.next().await, Some(Event::Local(1)));
    assert_eq!(events.next().await, Some(Event::Local(3)));
    assert_eq!(events.next().await, Some(Event::Local(6)));
    assert_eq!(events.next().await, Some(Event::Local(10)));
}

#[tokio::test]
async fn terminates_when_router_dropped() {
    let (dl, _events, messages, mut commands) = make_test_sync_dl();

    let first_cmd = commands.recv().await;

    assert_eq!(first_cmd, Some(Command::Sync));

    assert!(messages
        .send(Err(RoutingError::RouterDropped))
        .await
        .is_ok());

    let stop_res = dl.await_stopped().await;
    assert!(stop_res.is_ok());
    let inner_result = (*stop_res.unwrap()).clone();
    assert_eq!(inner_result, Err(DownlinkError::DroppedChannel));
}
