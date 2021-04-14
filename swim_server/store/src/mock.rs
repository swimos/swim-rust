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

use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::node::NodeStore;
use crate::stores::plane::{PlaneStore, SwimPlaneStore};
use crate::stores::StoreKey;
use crate::{
    ByteEngine, FromOpts, KeyedSnapshot, RangedSnapshot, Store, StoreEngine, StoreError, StoreOpts,
    SwimStore,
};
use serde::Serialize;
use std::path::Path;

#[derive(Debug)]
pub struct MockServerStore;

impl Store for MockServerStore {}

pub struct MockOpts;

impl StoreOpts for MockOpts {}

impl Default for MockOpts {
    fn default() -> Self {
        MockOpts
    }
}

impl FromOpts for MockServerStore {
    type Opts = MockOpts;

    fn from_opts<I: AsRef<Path>>(_path: I, _opts: &Self::Opts) -> Result<Self, StoreError> {
        Ok(MockServerStore)
    }
}

impl ByteEngine for MockServerStore {
    fn put(&self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Vec<u8>) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshot for MockServerStore {
    type Prefix = Vec<u8>;

    fn ranged_snapshot<F, K, V>(
        &self,
        _prefix: Self::Prefix,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }
}

impl SwimStore for MockServerStore {
    type PlaneStore = SwimPlaneStore<MockServerStore>;

    fn plane_store<I>(&mut self, _path: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString,
    {
        Ok(SwimPlaneStore::new("target".into(), MockServerStore))
    }
}

impl<'a> StoreEngine<'a> for MockServerStore {
    type Key = &'a [u8];

    fn put(&self, _key: Self::Key, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EmptyDelegateStore;
impl<'a> StoreEngine<'a> for EmptyDelegateStore {
    type Key = &'a [u8];

    fn put(&self, _key: Self::Key, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshot for EmptyDelegateStore {
    type Prefix = StoreKey;

    fn ranged_snapshot<F, K, V>(
        &self,
        _prefix: Self::Prefix,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct MockNodeStore;
impl NodeStore for MockNodeStore {
    type Delegate = MockPlaneStore;

    fn map_lane_store<I, K, V>(
        &self,
        _lane: I,
        _transient: bool,
    ) -> MapDataModel<Self::Delegate, K, V>
    where
        I: ToString,
        K: Serialize,
        V: Serialize,
        Self: Sized,
    {
        unimplemented!()
    }

    fn value_lane_store<I, V>(&self, _lane: I, _transient: bool) -> ValueDataModel<Self::Delegate>
    where
        I: ToString,
        V: Serialize,
        Self: Sized,
    {
        unimplemented!()
    }
}

impl RangedSnapshot for MockPlaneStore {
    type Prefix = StoreKey;

    fn ranged_snapshot<F, K, V>(
        &self,
        _prefix: Self::Prefix,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        todo!()
    }
}

impl<'a> StoreEngine<'a> for MockNodeStore {
    type Key = &'a [u8];

    fn put(&self, _key: Self::Key, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MockPlaneStore;
impl PlaneStore for MockPlaneStore {
    type NodeStore = MockNodeStore;

    fn node_store<I>(&self, _node: I) -> Self::NodeStore
    where
        I: ToString,
    {
        MockNodeStore
    }
}

impl<'a> StoreEngine<'a> for MockPlaneStore {
    type Key = StoreKey;

    fn put(&self, _key: Self::Key, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<(), StoreError> {
        Ok(())
    }
}
