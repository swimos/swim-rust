// Copyright 2015-2023 Swim Inc.
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
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use bytes::BufMut;
use parking_lot::Mutex;
use swimos_api::{
    error::StoreError,
    store::{KeyValue, NodePersistence, RangeConsumer},
};
use swimos_form::structural::write::StructuralWritable;
use swimos_model::Text;
use swimos_recon::print_recon_compact;
use swimos_utilities::trigger;

#[derive(Debug, Default)]
struct FakeMapLaneStore {
    data: HashMap<Vec<u8>, Vec<u8>>,
    staged_error: Option<StoreError>,
}

#[derive(Debug, Default)]
struct FakeStoreInner {
    values: HashMap<u64, Vec<u8>>,
    maps: HashMap<u64, FakeMapLaneStore>,
    next_id: u64,
    ids_forward: HashMap<Text, u64>,
    ids_back: HashMap<u64, Text>,
    valid: HashSet<Text>,
    waiters: Vec<trigger::Sender>,
}

#[derive(Debug, Default, Clone)]
pub struct FakeStore {
    inner: Arc<Mutex<FakeStoreInner>>,
}

type ByteMap = HashMap<Vec<u8>, Vec<u8>>;

impl FakeStore {
    pub fn new<'a, I: IntoIterator<Item = &'a str>>(valid: I) -> Self {
        let inner = FakeStoreInner {
            valid: valid.into_iter().map(Text::new).collect(),
            ..Default::default()
        };
        FakeStore {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn put_map<K: StructuralWritable, V: StructuralWritable>(
        &self,
        id: u64,
        map: HashMap<K, V>,
        error: Option<StoreError>,
    ) {
        let mut guard = self.inner.lock();
        let FakeStoreInner { maps, ids_back, .. } = &mut *guard;
        assert!(ids_back.contains_key(&id));
        let target = maps.entry(id).or_default();
        target.staged_error = error;
        for (k, v) in map {
            let key = format!("{}", print_recon_compact(&k)).into_bytes();
            let value = format!("{}", print_recon_compact(&v)).into_bytes();
            target.data.insert(key, value);
        }
    }

    pub fn get_map(&self, id: u64) -> Result<Option<ByteMap>, StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner { maps, ids_back, .. } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            Ok(maps.get(&id).map(|map_store| map_store.data.clone()))
        }
    }

    pub fn subscribe_to_changes(&self, on_change: trigger::Sender) {
        let mut guard = self.inner.lock();
        guard.waiters.push(on_change);
    }
}

#[derive(Debug, Default)]
pub struct FakeConsumer {
    current: Option<(Vec<u8>, Vec<u8>)>,
    values: VecDeque<(Vec<u8>, Vec<u8>)>,
    error: Option<StoreError>,
}

impl FakeConsumer {
    fn new(map: &HashMap<Vec<u8>, Vec<u8>>, error: Option<StoreError>) -> Self {
        let values = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        FakeConsumer {
            current: None,
            values,
            error,
        }
    }
}

impl RangeConsumer for FakeConsumer {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        let FakeConsumer {
            values,
            error,
            current,
        } = self;
        if let Some(kv) = values.pop_front() {
            let (k, v) = current.insert(kv);
            Ok(Some((&*k, &*v)))
        } else if let Some(err) = error.take() {
            Err(err)
        } else {
            Ok(None)
        }
    }
}

impl NodePersistence for FakeStore {
    type LaneId = u64;

    fn id_for(&self, name: &str) -> Result<Self::LaneId, StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            valid,
            next_id,
            ids_forward,
            ids_back,
            ..
        } = &mut *guard;
        if let Some(id) = ids_forward.get(name) {
            Ok(*id)
        } else if valid.contains(name) {
            let id = *next_id;
            *next_id += 1;
            ids_forward.insert(Text::new(name), id);
            ids_back.insert(id, Text::new(name));
            Ok(id)
        } else {
            Err(StoreError::KeyNotFound)
        }
    }

    fn get_value(
        &self,
        id: Self::LaneId,
        buffer: &mut bytes::BytesMut,
    ) -> Result<Option<usize>, StoreError> {
        let guard = self.inner.lock();
        let FakeStoreInner {
            values, ids_back, ..
        } = &*guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else if let Some(v) = values.get(&id) {
            let n = v.len();
            buffer.reserve(n);
            buffer.put(v.as_ref());
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn put_value(&mut self, id: Self::LaneId, value: &[u8]) -> Result<(), StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            values,
            ids_back,
            waiters,
            ..
        } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            values.insert(id, value.to_owned());
            waiters.drain(..).for_each(|t| {
                t.trigger();
            });
            Ok(())
        }
    }

    fn update_map(&mut self, id: Self::LaneId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            maps,
            ids_back,
            waiters,
            ..
        } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            let FakeMapLaneStore { data, .. } = maps.entry(id).or_default();
            data.insert(key.to_vec(), value.to_vec());
            waiters.drain(..).for_each(|t| {
                t.trigger();
            });
            Ok(())
        }
    }

    fn remove_map(&mut self, id: Self::LaneId, key: &[u8]) -> Result<(), StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            maps,
            ids_back,
            waiters,
            ..
        } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            let FakeMapLaneStore { data, .. } = maps.entry(id).or_default();
            data.remove(key);
            waiters.drain(..).for_each(|t| {
                t.trigger();
            });
            Ok(())
        }
    }

    fn clear_map(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            maps,
            ids_back,
            waiters,
            ..
        } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            let FakeMapLaneStore { data, .. } = maps.entry(id).or_default();
            data.clear();
            waiters.drain(..).for_each(|t| {
                t.trigger();
            });
            Ok(())
        }
    }

    fn delete_value(&mut self, id: Self::LaneId) -> Result<(), StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner {
            values,
            ids_back,
            waiters,
            ..
        } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else {
            values.remove(&id);
            waiters.drain(..).for_each(|t| {
                t.trigger();
            });
            Ok(())
        }
    }

    type MapCon<'a> = FakeConsumer
    where
        Self: 'a;

    fn read_map(&self, id: Self::LaneId) -> Result<Self::MapCon<'_>, StoreError> {
        let mut guard = self.inner.lock();
        let FakeStoreInner { maps, ids_back, .. } = &mut *guard;
        if !ids_back.contains_key(&id) {
            Err(StoreError::KeyNotFound)
        } else if let Some(FakeMapLaneStore { data, staged_error }) = maps.get_mut(&id) {
            Ok(FakeConsumer::new(data, staged_error.take()))
        } else {
            Err(StoreError::KeyNotFound)
        }
    }
}
