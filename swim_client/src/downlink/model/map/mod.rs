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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use im::ordmap::OrdMap;

use swim_model::Value;

use crate::downlink::DownlinkRequest;
use swim_warp::model::map::MapUpdate;

#[cfg(test)]
mod tests;

pub type MapModification<K, V> = MapUpdate<K, V>;

pub type UntypedMapModification<V> = MapModification<Value, V>;

pub enum MapAction {
    Update {
        key: Value,
        value: Value,
        old: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    Remove {
        key: Value,
        old: Option<DownlinkRequest<Option<Arc<Value>>>>,
    },
    Take {
        n: usize,
        before: Option<DownlinkRequest<ValMap>>,
        after: Option<DownlinkRequest<ValMap>>,
    },
    Skip {
        n: usize,
        before: Option<DownlinkRequest<ValMap>>,
        after: Option<DownlinkRequest<ValMap>>,
    },
    Clear {
        before: Option<DownlinkRequest<ValMap>>,
    },
    Get {
        request: DownlinkRequest<ValMap>,
    },
    GetByKey {
        key: Value,
        request: DownlinkRequest<Option<Arc<Value>>>,
    },
}

impl MapAction {
    pub fn update(key: Value, value: Value) -> MapAction {
        MapAction::Update {
            key,
            value,
            old: None,
        }
    }

    pub fn update_and_await(
        key: Value,
        value: Value,
        request: DownlinkRequest<Option<Arc<Value>>>,
    ) -> MapAction {
        MapAction::Update {
            key,
            value,
            old: Some(request),
        }
    }

    pub fn remove(key: Value) -> MapAction {
        MapAction::Remove { key, old: None }
    }

    pub fn remove_and_await(key: Value, request: DownlinkRequest<Option<Arc<Value>>>) -> MapAction {
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
        map_before: DownlinkRequest<ValMap>,
        map_after: DownlinkRequest<ValMap>,
    ) -> MapAction {
        MapAction::Take {
            n,
            before: Some(map_before),
            after: Some(map_after),
        }
    }

    pub fn drop(n: usize) -> MapAction {
        MapAction::Skip {
            n,
            before: None,
            after: None,
        }
    }

    pub fn drop_and_await(
        n: usize,
        map_before: DownlinkRequest<ValMap>,
        map_after: DownlinkRequest<ValMap>,
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

    pub fn clear_and_await(map_before: DownlinkRequest<ValMap>) -> MapAction {
        MapAction::Clear {
            before: Some(map_before),
        }
    }

    pub fn get_map(request: DownlinkRequest<ValMap>) -> MapAction {
        MapAction::Get { request }
    }

    pub fn get(key: Value, request: DownlinkRequest<Option<Arc<Value>>>) -> MapAction {
        MapAction::GetByKey { key, request }
    }
}

impl Debug for MapAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MapAction::Update { key, value, old } => {
                write!(f, "Update({:?} => {:?}, {:?})", key, value, old)
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
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapEvent<K> {
    Initial,
    Update(K),
    Remove(K),
    Take(usize),
    Drop(usize),
    Clear,
}

pub type ValMap = OrdMap<Value, Arc<Value>>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ViewWithEvent {
    pub view: ValMap,
    pub event: MapEvent<Value>,
}

impl ViewWithEvent {
    pub fn initial(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Initial,
        }
    }

    pub fn update(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Update(key),
        }
    }

    pub fn remove(map: &ValMap, key: Value) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Remove(key),
        }
    }

    pub fn take(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Take(n),
        }
    }

    pub fn skip(map: &ValMap, n: usize) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Drop(n),
        }
    }

    pub fn clear(map: &ValMap) -> ViewWithEvent {
        ViewWithEvent {
            view: map.clone(),
            event: MapEvent::Clear,
        }
    }
}
