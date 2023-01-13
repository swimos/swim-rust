// Copyright 2015-2021 Swim Inc.
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

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use swim_api::store::{NodePersistence, PlanePersistence};
use swim_store::{KeyValue, RangeConsumer, StoreError};

#[derive(Clone, Default)]
pub struct InMemoryPlanePersistence(Arc<Mutex<PlaneState>>);

#[derive(Default)]
struct PlaneState {
    nodes: HashMap<String, NodeEntry>,
}

enum NodeEntry {
    Idle(NodeState),
    InUse,
}

pub struct InMemoryNodePersistence {
    uri: String,
    plane: Arc<Mutex<PlaneState>>,
    state: NodeState,
}

impl InMemoryNodePersistence {
    fn new(uri: String, plane: Arc<Mutex<PlaneState>>, state: NodeState) -> Self {
        InMemoryNodePersistence { uri, plane, state }
    }
}

impl PlanePersistence for InMemoryPlanePersistence {
    type Node = InMemoryNodePersistence;

    fn node_store(&self, node_uri: &str) -> Result<Self::Node, StoreError> {
        let InMemoryPlanePersistence(inner) = self;
        let mut guard = inner.lock();
        let map = &mut guard.nodes;

        match map.remove_entry(node_uri) {
            Some((key, NodeEntry::Idle(node_state))) => {
                map.insert(key, NodeEntry::InUse);
                Ok(InMemoryNodePersistence::new(
                    node_uri.to_string(),
                    inner.clone(),
                    node_state,
                ))
            }
            Some((_, NodeEntry::InUse)) => todo!(),
            _ => {
                let node_state = NodeState::default();
                map.insert(node_uri.to_string(), NodeEntry::InUse);
                Ok(InMemoryNodePersistence::new(
                    node_uri.to_string(),
                    inner.clone(),
                    node_state,
                ))
            }
        }
    }
}

type MapIt<'a> = std::collections::hash_map::Iter<'a, Vec<u8>, Vec<u8>>;

pub struct InMemRangeConsumer<'a>(Option<MapIt<'a>>);

impl<'a> RangeConsumer for InMemRangeConsumer<'a> {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        let InMemRangeConsumer(inner) = self;
        Ok(inner
            .as_mut()
            .and_then(|it| it.next().map(|(k, v)| (k.as_ref(), v.as_ref()))))
    }
}

#[derive(Default)]
struct Ids {
    id_map: HashMap<String, u64>,
    counter: u64,
}

impl Ids {
    fn id_for(&mut self, name: &str) -> u64 {
        let Ids { id_map, counter } = self;
        if let Some(id) = id_map.get(name) {
            *id
        } else {
            let id = *counter;
            *counter += 1;
            id_map.insert(name.to_string(), id);
            id
        }
    }
}

#[derive(Default)]
struct NodeState {
    ids: Mutex<Ids>,
    values: HashMap<u64, Vec<u8>>,
    maps: HashMap<u64, HashMap<Vec<u8>, Vec<u8>>>,
}

impl NodePersistence for InMemoryNodePersistence {
    type MapCon<'a> = InMemRangeConsumer<'a>;

    type LaneId = u64;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError> {
        let InMemoryNodePersistence { state, .. } = self;
        Ok(state.ids.lock().id_for(name))
    }

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut BytesMut,
    ) -> Result<Option<usize>, StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { values, .. },
            ..
        } = self;
        Ok(values.get(&id).map(|bytes| {
            let len = bytes.len();
            buffer.put(bytes.as_ref());
            len
        }))
    }

    fn put_value(&mut self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { values, .. },
            ..
        } = self;
        match values.entry(id) {
            Entry::Occupied(mut entry) => {
                let bytes = entry.get_mut();
                bytes.clear();
                bytes.extend_from_slice(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value.to_vec());
            }
        }
        Ok(())
    }

    fn delete_value(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { values, .. },
            ..
        } = self;
        values.remove(&id);
        Ok(())
    }

    fn update_map(&mut self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { maps, .. },
            ..
        } = self;
        match maps.entry(id) {
            Entry::Occupied(mut entry) => {
                let map = entry.get_mut();
                if let Some(existing) = map.get_mut(key) {
                    existing.clear();
                    existing.extend_from_slice(value);
                } else {
                    map.insert(key.to_vec(), value.to_vec());
                }
            }
            Entry::Vacant(entry) => {
                entry.insert([(key.to_vec(), value.to_vec())].into_iter().collect());
            }
        }
        Ok(())
    }

    fn remove_map(&mut self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { maps, .. },
            ..
        } = self;
        if let Some(map) = maps.get_mut(&id) {
            map.remove(key);
        }
        Ok(())
    }

    fn clear_map(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { maps, .. },
            ..
        } = self;
        maps.remove(&id);
        Ok(())
    }

    fn read_map(&self, id: Self::LaneId) -> Result<Self::MapCon<'_>, StoreError> {
        let InMemoryNodePersistence {
            state: NodeState { maps, .. },
            ..
        } = self;
        if let Some(map) = maps.get(&id) {
            Ok(InMemRangeConsumer(Some(map.iter())))
        } else {
            Ok(InMemRangeConsumer(None))
        }
    }
}

impl Drop for InMemoryNodePersistence {
    fn drop(&mut self) {
        let InMemoryNodePersistence { uri, plane, state } = self;
        let state = std::mem::take(state);
        let mut guard = plane.lock();
        guard
            .nodes
            .insert(std::mem::take(uri), NodeEntry::Idle(state));
    }
}
