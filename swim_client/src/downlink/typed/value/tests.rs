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

use super::ValueActions;
use crate::configuration::downlink::OnInvalidMessage;
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::typed::value::{TypedValueDownlink, ValueDownlinkReceiver, ValueViewError};
use crate::downlink::typed::ViewMode;
use crate::downlink::{Command, Message};
use crate::downlink::{DownlinkConfig, DownlinkError};
use futures::future::join;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::ValidatedForm;
use swim_common::model::schema::StandardSchema;
use swim_common::model::Value;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

async fn responder(mut state: SharedValue, mut rx: mpsc::Receiver<Action>) {
    while let Some(value) = rx.recv().await {
        match value {
            Action::Set(v, maybe_cb) => {
                if matches!(v, Value::Int32Value(_)) {
                    state = SharedValue::new(v);
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send_ok(());
                    }
                } else {
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
            Action::Get(cb) => {
                let _ = cb.send_ok(state.clone());
            }
            Action::Update(f, maybe_cb) => {
                let old = state.clone();
                let new = f(state.as_ref());
                if matches!(new, Value::Int32Value(_)) {
                    state = SharedValue::new(new);
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send_ok(old);
                    }
                } else {
                    if let Some(cb) = maybe_cb {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
            Action::TryUpdate(f, maybe_cb) => {
                let old = state.clone();
                let maybe_new = f(state.as_ref());
                match maybe_new {
                    Ok(new @ Value::Int32Value(_)) => {
                        state = SharedValue::new(new);
                        if let Some(cb) = maybe_cb {
                            let _ = cb.send_ok(Ok(old));
                        }
                    }
                    Ok(_) => {
                        if let Some(cb) = maybe_cb {
                            let _ = cb.send_err(DownlinkError::InvalidAction);
                        }
                    }
                    Err(err) => {
                        if let Some(cb) = maybe_cb {
                            let _ = cb.send_ok(Err(err));
                        }
                    }
                }
            }
        };
    }
}

struct Actions(mpsc::Sender<Action>);
impl AsMut<mpsc::Sender<Action>> for Actions {
    fn as_mut(&mut self) -> &mut mpsc::Sender<Action> {
        &mut self.0
    }
}

#[tokio::test]
async fn value_get() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions = ValueActions::new(&tx);
        let n = actions.get().await;
        assert_eq!(n, Ok(2));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn value_set() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions = ValueActions::new(&tx);
        let result = actions.set(7).await;
        assert_eq!(result, Ok(()));

        let n = actions.get().await;
        assert_eq!(n, Ok(7));
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn invalid_value_set() {
    let (tx, rx) = mpsc::channel(8);

    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions: ValueActions<String> = ValueActions::new(&tx);
        let result = actions.set("hello".to_string()).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn value_set_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions = ValueActions::new(&tx);
        let result = actions.set_and_forget(7).await;
        assert_eq!(result, Ok(()));

        let n = actions.get().await;
        assert_eq!(n, Ok(7));
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn value_update() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions = ValueActions::new(&tx);
        let result = actions.update(|n| n + 2).await;
        assert_eq!(result, Ok(2));

        let n = actions.get().await;
        assert_eq!(n, Ok(4));
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn invalid_value_update() {
    let (tx, rx) = mpsc::channel(8);

    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions: ValueActions<Value> = ValueActions::new(&tx);
        let result = actions.update(|_| Value::Extant).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn value_update_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(Arc::new(2.into()), rx);

    let assertions = async move {
        let actions = ValueActions::new(&tx);
        let result = actions.update_and_forget(|n| n + 2).await;
        assert_eq!(result, Ok(()));

        let n = actions.get().await;
        assert_eq!(n, Ok(4));
    };
    join(assertions, responder).await;
}

struct Components<T> {
    downlink: TypedValueDownlink<T>,
    receiver: ValueDownlinkReceiver<T>,
    update_tx: mpsc::Sender<Result<Message<Value>, RoutingError>>,
    command_rx: mpsc::Receiver<Command<SharedValue>>,
}

fn make_value_downlink<T: ValidatedForm>(init: T) -> Components<T> {
    let init_value = init.into_value();

    let (update_tx, update_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let sender = swim_common::sink::item::for_mpsc_sender(command_tx).map_err_into();

    let (dl, rx) = crate::downlink::model::value::create_downlink(
        init_value,
        Some(T::schema()),
        ReceiverStream::new(update_rx),
        sender,
        DownlinkConfig {
            buffer_size: NonZeroUsize::new(8).unwrap(),
            yield_after: NonZeroUsize::new(2048).unwrap(),
            on_invalid: OnInvalidMessage::Terminate,
        },
    );
    let downlink = TypedValueDownlink::new(Arc::new(dl));
    let receiver = ValueDownlinkReceiver::new(rx);

    Components {
        downlink,
        receiver,
        update_tx,
        command_rx,
    }
}

#[tokio::test]
async fn subscriber_covariant_cast() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_value_downlink(0);

    let sub = downlink.subscriber();

    assert!(sub.clone().covariant_cast::<i32>().is_ok());
    assert!(sub.clone().covariant_cast::<Value>().is_ok());
    assert!(sub.clone().covariant_cast::<String>().is_err());
}

#[tokio::test]
async fn sender_contravariant_view() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_value_downlink(0i64);

    let sender = downlink.sender();

    assert!(sender.contravariant_view::<i64>().is_ok());
    assert!(sender.contravariant_view::<i32>().is_ok());
    assert!(sender.contravariant_view::<String>().is_err());
}

#[tokio::test]
async fn sender_covariant_view() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_value_downlink(0);

    let sender = downlink.sender();

    assert!(sender.covariant_view::<i32>().is_ok());
    assert!(sender.covariant_view::<Value>().is_ok());
    assert!(sender.covariant_view::<String>().is_err());
}

#[test]
fn value_view_error_display() {
    let err = ValueViewError {
        existing: StandardSchema::Nothing,
        requested: StandardSchema::Anything,
        mode: ViewMode::ReadOnly,
    };
    let str = err.to_string();

    assert_eq!(str, format!("A Read Only view of a value downlink with schema {} was requested but the original value downlink is running with schema {}.", StandardSchema::Anything, StandardSchema::Nothing));
}
