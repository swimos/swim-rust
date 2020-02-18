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

use crate::model::Value;

use super::*;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapAction<V> {
    Insert(Value, V),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

pub enum MapEvent {
    Initial,
    Insert(Value),
    Remove(Value),
    Take(usize),
    Skip(usize),
    Clear,
}

pub type ValMap = OrdMap<Value, Arc<Value>>;

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

type MapLaneOperation = Operation<MapAction<Value>>;

/// Asynchronously create a new downlink from a stream of input events, writing to a sink of
/// commands.
pub fn create_downlink<Err, Updates, Commands>(
    update_stream: Updates,
    cmd_sink: Commands,
    buffer_size: usize,
) -> Downlink<Err, mpsc::Sender<MapAction<Value>>, mpsc::Receiver<Event<ViewWithEvent>>>
where
    Err: From<item::MpscErr<Event<ViewWithEvent>>> + Send + Debug + 'static,
    Updates: Stream<Item = Message<MapAction<Value>>> + Send + 'static,
    Commands: for<'b> ItemSink<'b, Command<MapAction<Arc<Value>>>, Error = Err> + Send + 'static,
{
    let init: ValMap = OrdMap::new();
    super::create_downlink(init, update_stream, cmd_sink, buffer_size)
}

impl StateMachine<MapAction<Value>> for ValMap {
    type Ev = ViewWithEvent;
    type Cmd = MapAction<Arc<Value>>;

    fn handle_operation(
        model: &mut Model<ValMap>,
        op: MapLaneOperation,
    ) -> (Option<Event<Self::Ev>>, Option<Command<Self::Cmd>>) {
        let Model { data_state, state } = model;
        match op {
            Operation::Start => (None, Some(Command::Sync)),
            Operation::Message(Message::Linked) => {
                *state = DownlinkState::Linked;
                (None, None)
            }
            Operation::Message(Message::Synced) => {
                *state = DownlinkState::Linked;
                (Some(Event(ViewWithEvent::initial(data_state), false)), None)
            }
            Operation::Message(Message::Action(a)) => {
                if *state != DownlinkState::Unlinked {
                    let event = match a {
                        MapAction::Insert(k, v) => {
                            data_state.insert(k.clone(), Arc::new(v));
                            ViewWithEvent::insert(data_state, k)
                        }
                        MapAction::Remove(k) => {
                            data_state.remove(&k);
                            ViewWithEvent::remove(data_state, k)
                        }
                        MapAction::Take(n) => {
                            *data_state = data_state.take(n);
                            ViewWithEvent::take(data_state, n)
                        }
                        MapAction::Skip(n) => {
                            *data_state = data_state.skip(n);
                            ViewWithEvent::skip(data_state, n)
                        }
                        MapAction::Clear => {
                            data_state.clear();
                            ViewWithEvent::clear(data_state)
                        }
                    };
                    if *state == DownlinkState::Synced {
                        (Some(Event(event, false)), None)
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            }
            Operation::Message(Message::Unlinked) => {
                *state = DownlinkState::Linked;
                (None, None)
            }
            Operation::Action(a) => {
                let (event, cmd) = match a {
                    MapAction::Insert(k, v) => {
                        let v_arc = Arc::new(v);
                        data_state.insert(k.clone(), v_arc.clone());
                        (
                            ViewWithEvent::insert(data_state, k.clone()),
                            MapAction::Insert(k, v_arc),
                        )
                    }
                    MapAction::Remove(k) => {
                        data_state.remove(&k);
                        (
                            ViewWithEvent::remove(data_state, k.clone()),
                            MapAction::Remove(k),
                        )
                    }
                    MapAction::Take(n) => {
                        *data_state = data_state.take(n);
                        (ViewWithEvent::take(data_state, n), MapAction::Take(n))
                    }
                    MapAction::Skip(n) => {
                        *data_state = data_state.skip(n);
                        (ViewWithEvent::skip(data_state, n), MapAction::Skip(n))
                    }
                    MapAction::Clear => {
                        data_state.clear();
                        (ViewWithEvent::clear(data_state), MapAction::Clear)
                    }
                };
                (Some(Event(event, true)), Some(Command::Action(cmd)))
            }
            Operation::Close => (None, Some(Command::Unlink)),
        }
    }
}
