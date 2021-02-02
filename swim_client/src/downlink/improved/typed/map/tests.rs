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

use crate::downlink::model::map::{MapAction, ValMap};
use crate::downlink::DownlinkError;
use futures::future::join;
use im::OrdMap;
use std::future::Future;
use std::sync::Arc;
use swim_common::model::Value;
use tokio::sync::mpsc;
use super::MapActions;

async fn responder(init: OrdMap<i32, i32>, mut rx: mpsc::Receiver<MapAction>) {
    let mut state: ValMap = init
        .iter()
        .map(|(k, v)| (Value::Int32Value(*k), Value::Int32Value(*v)))
        .collect();

    while let Some(value) = rx.recv().await {
        match value {
            MapAction::Update { key, value, old } => {
                if matches!((&key, &value), (Value::Int32Value(_), Value::Int32Value(_))) {
                    let old_val = state.get(&key).map(Clone::clone);
                    state.insert(key, Arc::new(value));
                    if let Some(cb) = old {
                        let _ = cb.send_ok(old_val);
                    }
                } else {
                    if let Some(cb) = old {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
            MapAction::Remove { key, old } => {
                if matches!(&key, Value::Int32Value(_)) {
                    let old_val = state.get(&key).map(Clone::clone);
                    state.remove(&key);
                    if let Some(cb) = old {
                        let _ = cb.send_ok(old_val);
                    }
                } else {
                    if let Some(cb) = old {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
            MapAction::Take { n, before, after } => {
                let map_before = state.clone();
                state = state.take(n);
                if let Some(cb) = before {
                    let _ = cb.send_ok(map_before);
                }
                if let Some(cb) = after {
                    let _ = cb.send_ok(state.clone());
                }
            }
            MapAction::Skip { n, before, after } => {
                let map_before = state.clone();
                state = state.skip(n);
                if let Some(cb) = before {
                    let _ = cb.send_ok(map_before);
                }
                if let Some(cb) = after {
                    let _ = cb.send_ok(state.clone());
                }
            }
            MapAction::Clear { before } => {
                let map_before = state.clone();
                state.clear();
                if let Some(cb) = before {
                    let _ = cb.send_ok(map_before);
                }
            }
            MapAction::Get { request } => {
                let _ = request.send_ok(state.clone());
            }
            MapAction::GetByKey { key, request } => {
                if matches!(&key, Value::Int32Value(_)) {
                    let _ = request.send_ok(state.get(&key).map(Clone::clone));
                } else {
                    let _ = request.send_err(DownlinkError::InvalidAction);
                }
            }
            MapAction::Modify {
                key,
                f,
                before,
                after,
            } => {
                if matches!(&key, Value::Int32Value(_)) {
                    let old_val = state.get(&key).map(Clone::clone);
                    let replacement = match &old_val {
                        None => f(&None),
                        Some(v) => f(&Some(v.as_ref())),
                    }
                        .map(Arc::new);
                    match &replacement {
                        Some(v) => state.insert(key, v.clone()),
                        _ => state.remove(&key),
                    };
                    if let Some(cb) = before {
                        let _ = cb.send_ok(old_val);
                    }
                    if let Some(cb) = after {
                        let _ = cb.send_ok(replacement);
                    }
                } else {
                    if let Some(cb) = before {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                    if let Some(cb) = after {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
            MapAction::TryModify {
                key,
                f,
                before,
                after,
            } => {
                if matches!(&key, Value::Int32Value(_)) {
                    let old_val = state.get(&key).map(Clone::clone);
                    let replacement = match &old_val {
                        None => f(&None),
                        Some(v) => f(&Some(v.as_ref())),
                    };
                    let after_val = match replacement {
                        Ok(Some(v)) => {
                            let new_val = Arc::new(v);
                            state.insert(key, new_val.clone());
                            Ok(Some(new_val))
                        }
                        Ok(None) => {
                            state.remove(&key);
                            Ok(None)
                        }
                        Err(e) => Err(e),
                    };
                    if let Some(cb) = before {
                        let _ = cb.send_ok(Ok(old_val));
                    }
                    if let Some(cb) = after {
                        let _ = cb.send_ok(after_val);
                    }
                } else {
                    if let Some(cb) = before {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                    if let Some(cb) = after {
                        let _ = cb.send_err(DownlinkError::InvalidAction);
                    }
                }
            }
        }
    }
}

struct Actions(mpsc::Sender<MapAction>);
impl AsMut<mpsc::Sender<MapAction>> for Actions {
    fn as_mut(&mut self) -> &mut mpsc::Sender<MapAction> {
        &mut self.0
    }
}

fn make_actions(
    tx: &mpsc::Sender<MapAction>,
    rx: mpsc::Receiver<MapAction>,
    init: OrdMap<i32, i32>,
) -> (MapActions<i32, i32>, impl Future<Output = ()>) {
    let actions = MapActions::new(&tx);
    let task = responder(init, rx);
    (actions, task)
}

fn make_map() -> OrdMap<i32, i32> {
    let mut map = OrdMap::new();
    map.insert(1, 2);
    map.insert(2, 4);
    map.insert(3, 6);
    map
}

#[tokio::test]
async fn map_view() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.view().await;
        assert!(result.is_ok());

        let map = result.unwrap().as_ord_map();

        assert_eq!(map, make_map());
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn map_get() {
    let (tx, rx) = mpsc::channel(8);
    let (actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.get(2).await;
        assert!(result.is_ok());

        let map = result.unwrap();

        assert_eq!(map, Some(4));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_insert() {
    let (tx, rx) = mpsc::channel(8);
    let (actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.update(4, 8).await;
        assert!(result.is_ok());

        let map = result.unwrap();

        assert_eq!(map, None);

        let result = actions.get(4).await;
        assert_eq!(result, Ok(Some(8)));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_invalid_key_update() {
    let (tx, rx) = mpsc::channel(8);
    let actions: MapActions<String, i32> = MapActions::new(&tx);
    let task = responder(make_map(), rx);

    let assertions = async move {
        let result = actions.update("bad".to_string(), 8).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, task).await;
}

#[tokio::test]
async fn map_invalid_value_update() {
    let (tx, rx) = mpsc::channel(8);
    let actions: MapActions<i32, String> = MapActions::new(&tx);
    let task = responder(make_map(), rx);

    let assertions = async move {
        let result = actions.update(4, "bad".to_string()).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, task).await;
}

#[tokio::test]
async fn map_insert_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let (actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.update_and_forget(4, 8).await;
        assert_eq!(result, Ok(()));

        let result = actions.get(4).await;
        assert_eq!(result, Ok(Some(8)));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_remove() {
    let (tx, rx) = mpsc::channel(8);
    let (actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.remove(2).await;
        assert!(result.is_ok());

        let map = result.unwrap();

        assert_eq!(map, Some(4));

        let result = actions.get(2).await;
        assert_eq!(result, Ok(None));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_invalid_remove() {
    let (tx, rx) = mpsc::channel(8);
    let actions: MapActions<String, i32> = MapActions::new(&tx);
    let task = responder(make_map(), rx);

    let assertions = async move {
        let result = actions.remove("bad".to_string()).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, task).await;
}

#[tokio::test]
async fn map_remove_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let (actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.remove_and_forget(2).await;
        assert_eq!(result, Ok(()));

        let result = actions.get(2).await;
        assert_eq!(result, Ok(None));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_clear() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.clear().await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        let map = result.unwrap();

        assert!(map.is_empty());
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_clear_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.clear_and_forget().await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        let map = result.unwrap();

        assert!(map.is_empty());
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_remove_all() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.remove_all().await;
        assert!(result.is_ok());

        let map = result.unwrap().as_ord_map();

        assert_eq!(map, make_map());

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap();

        assert!(map.is_empty());
    };
    join(assertions, responder).await;
}

#[tokio::test]
async fn map_take() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.take(1).await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(1, 2);
        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_take_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.take_and_forget(1).await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(1, 2);
        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_take_and_get() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.take_and_get(1).await;

        let mut expected = OrdMap::new();
        expected.insert(1, 2);

        assert!(result.is_ok());

        let (before, after) = result.unwrap();

        assert_eq!(before.as_ord_map(), make_map());
        assert_eq!(after.as_ord_map(), expected.clone());

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_skip() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.skip(2).await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(3, 6);
        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_skip_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.skip_and_forget(2).await;
        assert_eq!(result, Ok(()));

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        let mut expected = OrdMap::new();
        expected.insert(3, 6);
        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_skip_and_get() {
    let (tx, rx) = mpsc::channel(8);
    let (mut actions, responder) = make_actions(&tx, rx, make_map());

    let assertions = async move {
        let result = actions.skip_and_get(2).await;

        let mut expected = OrdMap::new();
        expected.insert(3, 6);

        assert!(result.is_ok());

        let (before, after) = result.unwrap();

        assert_eq!(before.as_ord_map(), make_map());
        assert_eq!(after.as_ord_map(), expected.clone());

        let result = actions.view().await;
        assert!(result.is_ok());
        let map = result.unwrap().as_ord_map();

        assert_eq!(map, expected);
    };

    join(assertions, responder).await;
}