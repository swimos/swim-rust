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

use super::MapActions;
use crate::configuration::downlink::OnInvalidMessage;
use crate::downlink::model::map::{MapAction, UntypedMapModification, ValMap};
use crate::downlink::typed::map::{
    Incompatibility, MapDownlinkReceiver, MapViewError, TypedMapDownlink,
};
use crate::downlink::typed::ViewMode;
use crate::downlink::{Command, Message};
use crate::downlink::{DownlinkConfig, DownlinkError};
use futures::future::join;
use im::OrdMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_schema::ValueSchema;
use swim_schema::schema::StandardSchema;
use swim_model::Value;
use swim_common::routing::RoutingError;
use swim_utilities::future::item_sink::ItemSender;
use swim_warp::model::map::MapUpdate;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

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
        }
    }
}

struct Actions(mpsc::Sender<MapAction>);
impl AsMut<mpsc::Sender<MapAction>> for Actions {
    fn as_mut(&mut self) -> &mut mpsc::Sender<MapAction> {
        &mut self.0
    }
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
    let responder = responder(make_map(), rx);
    let assertions = async move {
        let actions = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions = MapActions::new(&tx);
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
async fn map_insert_discard() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions = MapActions::new(&tx);
        let result = actions.update_discard(4, 8).await;
        assert!(result.is_ok());

        let result = actions.get(4).await;
        assert_eq!(result, Ok(Some(8)));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_invalid_key_update() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<String, i32> = MapActions::new(&tx);
        let result = actions.update("bad".to_string(), 8).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_invalid_value_update() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, String> = MapActions::new(&tx);
        let result = actions.update(4, "bad".to_string()).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_insert_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions = MapActions::new(&tx);
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
async fn map_remove_discard() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
        let result = actions.remove_discard(2).await;
        assert!(result.is_ok());

        let result = actions.get(2).await;
        assert_eq!(result, Ok(None));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_invalid_remove() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<String, i32> = MapActions::new(&tx);
        let result = actions.remove("bad".to_string()).await;
        assert_eq!(result, Err(DownlinkError::InvalidAction));
    };

    join(assertions, responder).await;
}

#[tokio::test]
async fn map_remove_and_forget() {
    let (tx, rx) = mpsc::channel(8);
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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
    let responder = responder(make_map(), rx);

    let assertions = async move {
        let actions: MapActions<i32, i32> = MapActions::new(&tx);
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

struct Components<K, V> {
    downlink: TypedMapDownlink<K, V>,
    receiver: MapDownlinkReceiver<K, V>,
    update_tx: mpsc::Sender<Result<Message<MapUpdate<Value, Value>>, RoutingError>>,
    command_rx: mpsc::Receiver<Command<UntypedMapModification<Value>>>,
}

fn make_map_downlink<K: ValueSchema, V: ValueSchema>() -> Components<K, V> {
    let (update_tx, update_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let sender = swim_utilities::future::item_sink::for_mpsc_sender(command_tx).map_err_into();

    let (dl, rx) = crate::downlink::map_downlink(
        Some(K::schema()),
        Some(V::schema()),
        ReceiverStream::new(update_rx),
        sender,
        DownlinkConfig {
            buffer_size: NonZeroUsize::new(8).unwrap(),
            yield_after: NonZeroUsize::new(2048).unwrap(),
            on_invalid: OnInvalidMessage::Terminate,
        },
    );
    let downlink = TypedMapDownlink::new(Arc::new(dl));
    let receiver = MapDownlinkReceiver::new(rx);

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
    } = make_map_downlink::<i32, i32>();

    let sub = downlink.subscriber();

    assert!(sub.clone().covariant_cast::<i32, i32>().is_ok());
    assert!(sub.clone().covariant_cast::<Value, Value>().is_ok());
    assert!(sub.clone().covariant_cast::<i32, Value>().is_ok());
    assert!(sub.clone().covariant_cast::<Value, i32>().is_ok());
    assert!(sub.clone().covariant_cast::<String, i32>().is_err());
    assert!(sub.clone().covariant_cast::<i32, String>().is_err());
    assert!(sub.clone().covariant_cast::<String, String>().is_err());
}

#[tokio::test]
async fn sender_contravariant_view() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_map_downlink::<i64, i64>();

    let sender = downlink.sender();

    assert!(sender.contravariant_view::<i64, i64>().is_ok());
    assert!(sender.contravariant_view::<i64, i32>().is_ok());
    assert!(sender.contravariant_view::<i32, i64>().is_ok());
    assert!(sender.contravariant_view::<i32, i32>().is_ok());
    assert!(sender.contravariant_view::<String, i64>().is_err());
    assert!(sender.contravariant_view::<i64, String>().is_err());
    assert!(sender.contravariant_view::<String, String>().is_err());
}

#[tokio::test]
async fn sender_covariant_view() {
    let Components {
        downlink,
        receiver: _receiver,
        update_tx: _update_tx,
        command_rx: _command_rx,
    } = make_map_downlink::<i32, i32>();

    let sender = downlink.sender();

    assert!(sender.covariant_view::<i32, i32>().is_ok());
    assert!(sender.covariant_view::<i32, Value>().is_ok());
    assert!(sender.covariant_view::<Value, i32>().is_ok());
    assert!(sender.covariant_view::<Value, Value>().is_ok());
    assert!(sender.covariant_view::<String, i32>().is_err());
    assert!(sender.covariant_view::<i32, String>().is_err());
    assert!(sender.covariant_view::<String, String>().is_err());
}

#[test]
fn map_view_error_display() {
    let err = MapViewError {
        mode: ViewMode::ReadOnly,
        existing_key: StandardSchema::Anything,
        existing_value: StandardSchema::Nothing,
        requested_key: StandardSchema::NonNan,
        requested_value: StandardSchema::Finite,
        incompatibility: Incompatibility::Key,
    };

    let string = err.to_string();

    assert_eq!(string, format!("A Read Only view of a map downlink (key schema {} and value schema {})) was requested with key schema {} and value schema {}. The key schemas are incompatible.", 
                               StandardSchema::Anything, StandardSchema::Nothing, StandardSchema::NonNan, StandardSchema::Finite));
}
