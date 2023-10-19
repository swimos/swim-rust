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
    borrow::Borrow,
    cell::RefCell,
    collections::{HashMap, HashSet},
    hash::Hash,
};

use bytes::BytesMut;
use swim_form::structural::write::StructuralWritable;

use crate::{
    agent_model::WriteResult,
    item::AgentItem,
    lanes::{LaneItem, MapLane},
};

use super::DownlinkStatus;

mod downlink;
pub mod lifecycle;

#[derive(Debug)]
pub struct JoinMapLane<L, K, V> {
    inner: MapLane<K, V>,
    link_tracker: RefCell<Links<L, K>>,
}

#[derive(Debug)]
struct Link<K> {
    status: DownlinkStatus,
    keys: HashSet<K>,
}

#[derive(Debug)]
struct Links<L, K> {
    links: HashMap<L, Link<K>>,
    ownership: HashMap<K, L>,
}

impl<L, K> Default for Links<L, K> {
    fn default() -> Self {
        Self {
            links: Default::default(),
            ownership: Default::default(),
        }
    }
}

impl<L, K, V> JoinMapLane<L, K, V> {
    pub fn new(id: u64) -> Self {
        JoinMapLane {
            inner: MapLane::new(id, HashMap::new()),
            link_tracker: Default::default(),
        }
    }

    pub(crate) fn map_lane(&self) -> &MapLane<K, V> {
        &self.inner
    }
}

impl<L, K, V> JoinMapLane<L, K, V>
where
    K: Clone + Eq + Hash,
{
    /// Read a value from the map, if it exists.
    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.get(key, f)
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        self.inner.get_map(f)
    }
}

impl<L, K, V> AgentItem for JoinMapLane<L, K, V> {
    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<L, K, V> LaneItem for JoinMapLane<L, K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        self.inner.write_to_buffer(buffer)
    }
}
