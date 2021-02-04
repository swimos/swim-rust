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
use crate::downlink::model::value::{Action, SharedValue};
use crate::downlink::DownlinkError;
use futures::future::join;
use std::sync::Arc;
use swim_common::model::Value;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

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
    fn as_mut(&mut self) -> &mut Sender<Action> {
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
