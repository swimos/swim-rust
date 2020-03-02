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

use std::sync::Arc;

use im::ordmap::OrdMap;
use tokio::sync::mpsc;

use super::*;
use model::Value;
use request::Request;
use sink::item::ItemSink;
use std::fmt::Formatter;

#[cfg(test)]
mod tests;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapModification<V> {
    Insert(Value, V),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

pub enum MapAction {
    Insert {
        key: Value,
        value: Value,
        old: Option<Request<Option<Arc<Value>>>>,
    },
    Remove {
        key: Value,
        old: Option<Request<Option<Arc<Value>>>>,
    },
    Take {
        n: usize,
        before: Option<Request<ValMap>>,
        after: Option<Request<ValMap>>,
    },
    Skip {
        n: usize,
        before: Option<Request<ValMap>>,
        after: Option<Request<ValMap>>,
    },
    Clear {
        before: Option<Request<ValMap>>,
    },
    Get {
        request: Request<ValMap>,
    },
    GetByKey {
        key: Value,
        request: Request<Option<Arc<Value>>>,
    },
    Update {
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        before: Option<Request<Option<Arc<Value>>>>,
        after: Option<Request<Option<Arc<Value>>>>,
    },
}

impl MapAction {
    pub fn insert(key: Value, value: Value) -> MapAction {
        MapAction::Insert {
            key,
            value,
            old: None,
        }
    }

    pub fn insert_and_await(
        key: Value,
        value: Value,
        request: Request<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Insert {
            key,
            value,
            old: Some(request),
        }
    }

    pub fn remove(key: Value) -> MapAction {
        MapAction::Remove { key, old: None }
    }

    pub fn remove_and_await(key: Value, request: Request<Option<Arc<Value>>>) -> MapAction {
        MapAction::Remove {
            key,
            old: Some(request),
        }
    }

    pub fn take(n: usize) -> MapAction {
        MapAction::Take {
            n,
            before: None,
            after: None,
        }
    }

    pub fn take_and_await(
        n: usize,
        map_before: Request<ValMap>,
        map_after: Request<ValMap>,
    ) -> MapAction {
        MapAction::Take {
            n,
            before: Some(map_before),
            after: Some(map_after),
        }
    }

    pub fn skip(n: usize) -> MapAction {
        MapAction::Skip {
            n,
            before: None,
            after: None,
        }
    }

    pub fn skip_and_await(
        n: usize,
        map_before: Request<ValMap>,
        map_after: Request<ValMap>,
    ) -> MapAction {
        MapAction::Skip {
            n,
            before: Some(map_before),
            after: Some(map_after),
        }
    }

    pub fn clear() -> MapAction {
        MapAction::Clear { before: None }
    }

    pub fn clear_and_await(map_before: Request<ValMap>) -> MapAction {
        MapAction::Clear {
            before: Some(map_before),
        }
    }

    pub fn get_map(request: Request<ValMap>) -> MapAction {
        MapAction::Get { request }
    }

    pub fn get(key: Value, request: Request<Option<Arc<Value>>>) -> MapAction {
        MapAction::GetByKey { key, request }
    }

    pub fn update<F>(key: Value, f: F) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn update_box(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
    ) -> MapAction {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: None,
            after: None,
        }
    }

    pub fn update_and_await<F>(
        key: Value,
        f: F,
        val_before: Request<Option<Arc<Value>>>,
        val_after: Request<Option<Arc<Value>>>,
    ) -> MapAction
    where
        F: FnOnce(&Option<&Value>) -> Option<Value> + Send + 'static,
    {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }

    pub fn update_box_and_await(
        key: Value,
        f: Box<dyn FnOnce(&Option<&Value>) -> Option<Value> + Send>,
        val_before: Request<Option<Arc<Value>>>,
        val_after: Request<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Update {
            key,
            f: Box::new(f),
            before: Some(val_before),
            after: Some(val_after),
        }
    }
}

impl Debug for MapAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapAction::Insert { key, value, old } => {
                write!(f, "Insert({:?} => {:?}, {:?})", key, value, old)
            }
            MapAction::Remove { key, old } => write!(f, "Remove({:?}, {:?})", key, old),
            MapAction::Take { n, before, after } => {
                write!(f, "Take({:?}, {:?}, {:?})", n, before, after)
            }
            MapAction::Skip { n, before, after } => {
                write!(f, "Skip({:?}, {:?}, {:?})", n, before, after)
            }
            MapAction::Clear { before } => write!(f, "Clear({:?})", before),
            MapAction::Get { request } => write!(f, "Get({:?})", request),
            MapAction::GetByKey { key, request } => write!(f, "GetByKey({:?}, {:?})", key, request),
            MapAction::Update {
                key, before, after, ..
            } => write!(f, "Update({:?}, <closure>, {:?}, {:?})", key, before, after),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapEvent {
    Initial,
    Insert(Value),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

pub type ValMap = OrdMap<Value, Arc<Value>>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ViewWithEvent {
    pub view: ValMap,
    pub event: MapEvent,
}

impl ViewWithEvent {
    fn initial(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Initial,
        }
    }

    fn insert(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Insert(key),
        }
    }

    fn remove(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Remove(key),
        }
    }

    fn take(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Take(n),
        }
    }

    fn skip(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Skip(n),
        }
    }

    fn clear(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Clear,
        }
    }
}

type MapLaneOperation = Operation<MapModification<Value>, MapAction>;

/// Create a map downlink.
pub fn create_downlink<Err, Updates, Commands>(
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> Downlink<mpsc::Sender<MapAction>, mpsc::Receiver<Event<ViewWithEvent>>>
where
    Err: Into<DownlinkError> + Send + 'static,
    Updates: Stream<Item = Message<MapModification<Value>>> + Send + 'static,
    Commands:
        for<'b> ItemSink<'b, Command<MapModification<Arc<Value>>>, Error = Err> + Send + 'static,
{
    let init: ValMap = OrdMap::new();
    super::create_downlink(init, update_stream, cmd_sink, buffer_size)
}

impl StateMachine<MapModification<Value>, MapAction> for ValMap {
    type Ev = ViewWithEvent;
    type Cmd = MapModification<Arc<Value>>;

    fn handle_operation(
        model: &mut Model<ValMap>,
        op: MapLaneOperation,
    ) -> Response<Self::Ev, Self::Cmd> {
        let Model { data_state, state } = model;
        match op {
            Operation::Start => {
                if *state != DownlinkState::Synced {
                    Response::for_command(Command::Sync)
                } else {
                    Response::none()
                }
            }
            Operation::Message(Message::Linked) => {
                *state = DownlinkState::Linked;
                Response::none()
            }
            Operation::Message(Message::Synced) => {
                let state_before = *state;
                *state = DownlinkState::Synced;
                if state_before == DownlinkState::Synced {
                    Response::none()
                } else {
                    Response::for_event(Event(ViewWithEvent::initial(data_state), false))
                }
            }
            Operation::Message(Message::Action(a)) => {
                if *state != DownlinkState::Unlinked {
                    let event = match a {
                        MapModification::Insert(k, v) => {
                            data_state.insert(k.clone(), Arc::new(v));
                            ViewWithEvent::insert(data_state, k)
                        }
                        MapModification::Remove(k) => {
                            data_state.remove(&k);
                            ViewWithEvent::remove(data_state, k)
                        }
                        MapModification::Take(n) => {
                            *data_state = data_state.take(n);
                            ViewWithEvent::take(data_state, n)
                        }
                        MapModification::Skip(n) => {
                            *data_state = data_state.skip(n);
                            ViewWithEvent::skip(data_state, n)
                        }
                        MapModification::Clear => {
                            data_state.clear();
                            ViewWithEvent::clear(data_state)
                        }
                    };
                    if *state == DownlinkState::Synced {
                        Response::for_event(Event(event, false))
                    } else {
                        Response::none()
                    }
                } else {
                    Response::none()
                }
            }
            Operation::Message(Message::Unlinked) => {
                *state = DownlinkState::Unlinked;
                Response::none().then_terminate()
            }
            Operation::Action(a) => handle_action(data_state, a),
            Operation::Close => Response::for_command(Command::Unlink).then_terminate(),
        }
    }
}

fn update_and_notify<Upd>(
    data_state: &mut ValMap,
    update: Upd,
    request: Option<Request<ValMap>>,
) -> Option<()>
where
    Upd: FnOnce(&mut ValMap) -> (),
{
    match request {
        Some(req) => {
            let prev = data_state.clone();
            update(data_state);
            req.send(prev)
        }
        _ => {
            update(data_state);
            None
        }
    }
}

fn update_and_notify_prev<Upd>(
    data_state: &mut ValMap,
    key: &Value,
    update: Upd,
    request: Option<Request<Option<Arc<Value>>>>,
) -> Option<()>
where
    Upd: FnOnce(&mut ValMap) -> (),
{
    match request {
        Some(req) => {
            let prev = data_state.get(key).cloned();
            update(data_state);
            req.send(prev)
        }
        _ => {
            update(data_state);
            None
        }
    }
}

fn handle_action(
    data_state: &mut ValMap,
    action: MapAction,
) -> Response<ViewWithEvent, MapModification<Arc<Value>>> {
    let (resp, err) = match action {
        MapAction::Insert { key, value, old } => {
            let v_arc = Arc::new(value);
            let err = update_and_notify_prev(
                data_state,
                &key,
                |map| {
                    map.insert(key.clone(), v_arc.clone());
                },
                old,
            );
            (
                Response::of(
                    Event(ViewWithEvent::insert(data_state, key.clone()), true),
                    Command::Action(MapModification::Insert(key, v_arc)),
                ),
                err,
            )
        }
        MapAction::Remove { key, old } => {
            let (err, did_rem) = match old {
                Some(req) => {
                    let prev = data_state.remove(&key);
                    let did_remove = prev.is_some();
                    (req.send(prev), did_remove)
                }
                _ => {
                    let old = data_state.remove(&key);
                    (None, old.is_some())
                }
            };
            if did_rem {
                (
                    Response::of(
                        Event(ViewWithEvent::remove(data_state, key.clone()), true),
                        Command::Action(MapModification::Remove(key)),
                    ),
                    err,
                )
            } else {
                (Response::none(), err)
            }
        }
        MapAction::Take { n, before, after } => {
            let err1 = update_and_notify(
                data_state,
                |map| {
                    *map = map.take(n);
                },
                before,
            );
            let err2 = after.and_then(|req| req.send(data_state.clone()));
            (
                Response::of(
                    Event(ViewWithEvent::take(data_state, n), true),
                    Command::Action(MapModification::Take(n)),
                ),
                err1.or(err2),
            )
        }
        MapAction::Skip { n, before, after } => {
            let err1 = update_and_notify(
                data_state,
                |map| {
                    *map = map.skip(n);
                },
                before,
            );
            let err2 = after.and_then(|req| req.send(data_state.clone()));
            (
                Response::of(
                    Event(ViewWithEvent::skip(data_state, n), true),
                    Command::Action(MapModification::Skip(n)),
                ),
                err1.or(err2),
            )
        }
        MapAction::Clear { before } => {
            let err = match before {
                Some(req) => {
                    let prev = std::mem::take(data_state);
                    req.send(prev)
                }
                _ => {
                    data_state.clear();
                    None
                }
            };
            (
                Response::of(
                    Event(ViewWithEvent::clear(data_state), true),
                    Command::Action(MapModification::Clear),
                ),
                err,
            )
        }
        MapAction::Get { request } => {
            let err = request.send(data_state.clone());
            (Response::none(), err)
        }
        MapAction::GetByKey { key, request } => {
            let err = request.send(data_state.get(&key).cloned());
            (Response::none(), err)
        }
        MapAction::Update {
            key,
            f,
            before: old,
            after: replacement,
        } => {
            let prev = data_state.get(&key).map(|arc| arc.as_ref());
            let maybe_new_val = f(&prev);
            match maybe_new_val {
                Some(new_val) => {
                    let v_arc = Arc::new(new_val);
                    let replaced = data_state.insert(key.clone(), v_arc.clone());
                    let err1 = old.and_then(|req| req.send(replaced));
                    let err2 = replacement.and_then(|req| req.send(Some(v_arc.clone())));
                    (
                        Response::of(
                            Event(ViewWithEvent::insert(data_state, key.clone()), true),
                            Command::Action(MapModification::Insert(key, v_arc)),
                        ),
                        err1.or(err2),
                    )
                }
                _ if prev.is_some() => {
                    let removed = data_state.remove(&key);
                    let err1 = old.and_then(|req| req.send(removed));
                    let err2 = replacement.and_then(|req| req.send(None));
                    (
                        Response::of(
                            Event(ViewWithEvent::remove(data_state, key.clone()), true),
                            Command::Action(MapModification::Remove(key)),
                        ),
                        err1.or(err2),
                    )
                }
                _ => (Response::none(), None),
            }
        }
    };
    match err {
        Some(_) => resp.with_error(TransitionError::ReceiverDropped),
        _ => resp,
    }
}
