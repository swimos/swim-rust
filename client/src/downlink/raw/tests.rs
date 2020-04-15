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

use std::time::Instant;

use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use common::sink::item::*;

use super::*;

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

impl StateMachine<Msg, AddTo> for State {
    type Ev = i32;
    type Cmd = i32;

    fn handle_operation(
        model: &mut Model<Self>,
        op: Operation<Msg, AddTo>,
    ) -> Response<Self::Ev, Self::Cmd> {
        match op {
            Operation::Start => Response::for_command(Command::Sync),
            Operation::Message(Message::Linked) => {
                model.state = DownlinkState::Linked;
                Response::none()
            }
            Operation::Message(Message::Synced) => {
                let prev = model.state;
                model.state = DownlinkState::Synced;
                if prev != DownlinkState::Synced {
                    Response::for_event(Event(model.data_state.0, false))
                } else {
                    Response::none()
                }
            }
            Operation::Message(Message::Unlinked) => {
                model.state = DownlinkState::Unlinked;
                Response::none()
            }
            Operation::Message(Message::Action(Msg(n, maybe_cb))) => {
                if model.state != DownlinkState::Unlinked {
                    model.data_state.0 = n;
                }
                let resp = if model.state == DownlinkState::Synced {
                    Response::for_event(Event(model.data_state.0, false))
                } else {
                    Response::none()
                };
                match maybe_cb {
                    Some(cb) => match cb.send(Instant::now()) {
                        Ok(_) => resp,
                        Err(_) => resp.with_error(TransitionError::ReceiverDropped),
                    },
                    _ => resp,
                }
            }
            Operation::Action(AddTo(n, maybe_cb)) => {
                let next = model.data_state.0 + n;
                let resp = if next < 0 {
                    Response::none().with_error(TransitionError::IllegalTransition(
                        "State cannot be negative.".to_owned(),
                    ))
                } else {
                    model.data_state.0 = next;
                    Response::of(Event(next, true), Command::Action(next))
                };
                match maybe_cb {
                    Some(cb) => match cb.send(Instant::now()) {
                        Ok(_) => resp,
                        Err(_) => resp.with_error(TransitionError::ReceiverDropped),
                    },
                    _ => resp,
                }
            }
            Operation::Close => Response::none().then_terminate(),
        }
    }
}

type Str<T> = mpsc::Receiver<T>;
type Snk<T> = mpsc::Sender<T>;

async fn make_test_dl() -> (
    RawDownlink<Snk<AddTo>, Str<Event<i32>>>,
    Snk<Message<Msg>>,
    Str<Command<i32>>,
) {
    let (tx_in, rx_in) = mpsc::channel(10);
    let (tx_out, rx_out) = mpsc::channel::<Command<i32>>(10);
    let downlink = create_downlink(
        State(0),
        rx_in,
        for_mpsc_sender::<Command<i32>, DownlinkError>(tx_out),
        10,
    );
    (downlink, tx_in, rx_out)
}

#[tokio::test]
async fn sync_on_startup() {
    let (dl, _messages, mut commands) = make_test_dl().await;

    let first_cmd = commands.next().await;
    assert_that!(first_cmd, eq(Some(Command::Sync)));
    let stop_res = dl.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn event_on_sync() {
    let (dl, mut messages, _commands) = make_test_dl().await;
    let (dl_tx, mut dl_rx) = dl.split();

    assert_that!(messages.send(Message::Linked).await, ok());
    assert_that!(messages.send(Message::Synced).await, ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_that!(first_ev, eq(Some(Event(0, false))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn ignore_update_before_link() {
    let (dl, mut messages, _commands) = make_test_dl().await;
    let (dl_tx, mut dl_rx) = dl.split();

    assert_that!(messages.send(Message::Action(Msg::of(12))).await, ok());
    assert_that!(messages.send(Message::Linked).await, ok());
    assert_that!(messages.send(Message::Synced).await, ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_that!(first_ev, eq(Some(Event(0, false))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn apply_updates_between_link_and_sync() {
    let (dl, mut messages, _commands) = make_test_dl().await;
    let (dl_tx, mut dl_rx) = dl.split();

    assert_that!(messages.send(Message::Linked).await, ok());
    assert_that!(messages.send(Message::Action(Msg::of(12))).await, ok());
    assert_that!(messages.send(Message::Synced).await, ok());

    let first_ev = dl_rx.event_stream.recv().await;
    assert_that!(first_ev, eq(Some(Event(12, false))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

/// Pre-synchronizes a downlink for tests that require the ['DownlinkState::Synced'] state.
async fn sync_dl(
    init: Msg,
    messages: &mut Snk<Message<Msg>>,
    events: &mut Str<Event<i32>>,
    commands: &mut Str<Command<i32>>,
) {
    let n = init.0;
    let first_cmd = commands.recv().await;
    assert_that!(first_cmd, eq(Some(Command::Sync)));

    assert_that!(messages.send(Message::Linked).await, ok());
    assert_that!(messages.send(Message::Action(init)).await, ok());
    assert_that!(messages.send(Message::Synced).await, ok());

    let first_ev = events.recv().await;
    assert_that!(first_ev, eq(Some(Event(n, false))));
}

#[tokio::test]
async fn updates_processed_when_synced() {
    let (dl, mut messages, mut commands) = make_test_dl().await;
    let (dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    assert_that!(messages.send(Message::Action(Msg::of(10))).await, ok());
    assert_that!(messages.send(Message::Action(Msg::of(20))).await, ok());
    assert_that!(messages.send(Message::Action(Msg::of(30))).await, ok());

    assert_that!(events.recv().await, eq(Some(Event(10, false))));
    assert_that!(events.recv().await, eq(Some(Event(20, false))));
    assert_that!(events.recv().await, eq(Some(Event(30, false))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn actions_processed_when_synced() {
    let (dl, mut messages, mut commands) = make_test_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    assert_that!(dl_tx.send(AddTo::of(4)).await, ok());

    assert_that!(events.recv().await, eq(Some(Event(5, true))));
    assert_that!(commands.recv().await, eq(Some(Command::Action(5))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn actions_paused_when_not_synced() {
    let (dl, mut messages, mut commands) = make_test_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    //Send an action.
    assert_that!(dl_tx.send(action).await, ok());
    //Then sync the downlink afterwards.
    sync_dl(msg, &mut messages, &mut events, &mut commands).await;

    let action_at = act_rx.await;
    let msg_at = msg_rx.await;
    assert_that!(&action_at, ok());
    assert_that!(&msg_at, ok());
    assert_that!(msg_at.unwrap(), less_than_or_equal_to(action_at.unwrap()));

    assert_that!(events.recv().await, eq(Some(Event(17, true))));
    assert_that!(commands.recv().await, eq(Some(Command::Action(17))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn actions_paused_when_unlinked() {
    let (dl, mut messages, mut commands) = make_test_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();
    let (msg_tx, msg_rx) = oneshot::channel();

    let action = AddTo::of(12).when_processed(act_tx);
    let msg = Msg::of(5).when_processed(msg_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Unlink the downlink.
    assert_that!(messages.send(Message::Unlinked).await, ok());
    //Then send an action.
    assert_that!(dl_tx.send(action).await, ok());
    //Link but don't yet sync.
    assert_that!(messages.send(Message::Linked).await, ok());
    //Send an update that we expect to be handled before the action.
    assert_that!(messages.send(Message::Action(msg)).await, ok());
    //Re-sync which we expect to unblock the action.
    assert_that!(messages.send(Message::Synced).await, ok());

    //Check that the events happened in the correct order.
    let action_at = act_rx.await;
    let msg_at = msg_rx.await;
    assert_that!(&action_at, ok());
    assert_that!(&msg_at, ok());
    assert_that!(msg_at.unwrap(), less_than_or_equal_to(action_at.unwrap()));

    //Event generated when we re-sync.
    assert_that!(events.recv().await, eq(Some(Event(5, false))));
    //Event and command generated after the action is applied.
    assert_that!(events.recv().await, eq(Some(Event(17, true))));
    assert_that!(commands.recv().await, eq(Some(Command::Action(17))));

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, ok());
}

#[tokio::test]
async fn errors_propagate() {
    let (dl, mut messages, mut commands) = make_test_dl().await;
    let (mut dl_tx, dl_rx) = dl.split();

    let mut events = dl_rx.event_stream;

    let (act_tx, act_rx) = oneshot::channel();

    let action = AddTo::of(-100).when_processed(act_tx);

    sync_dl(Msg::of(1), &mut messages, &mut events, &mut commands).await;

    //Send an action that will trigger an error.
    assert_that!(dl_tx.send(action).await, ok());

    //Wait for the action the be executed.
    assert_that!(act_rx.await, ok());

    let stop_res = dl_tx.stop().await;
    assert_that!(stop_res, err());
    assert_that!(stop_res.err().unwrap(), eq(DownlinkError::TransitionError));
}
