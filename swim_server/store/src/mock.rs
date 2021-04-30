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

use crate::engines::{KeyedSnapshot, KeyspaceByteEngine, NoStore, StoreOpts};
use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::node::NodeStore;
use crate::stores::plane::{PlaneStore, SwimPlaneStore};
use crate::stores::{LaneKey, StoreKey};
use crate::{
    ByteEngine, FromOpts, RangedSnapshotLoad, Store, StoreError, StoreInfo, SwimNodeStore,
    SwimStore,
};
use serde::Serialize;
use std::path::Path;
use swim_common::model::text::Text;
use tempdir::TempDir;

#[derive(Debug)]
pub struct MockServerStore {
    pub dir: TempDir,
}

impl Store for MockServerStore {
    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: "mock".to_string(),
            kind: "mock".to_string(),
        }
    }
}

impl KeyspaceByteEngine for MockServerStore {
    fn put_keyspace(&self, _keyspace: &str, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_keyspace(&self, _keyspace: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete_keyspace(&self, _keyspace: &str, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn merge_keyspace(
        &self,
        _keyspace: &str,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshotLoad for MockServerStore {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }
}

pub struct MockOpts;

impl StoreOpts for MockOpts {}

impl Default for MockOpts {
    fn default() -> Self {
        MockOpts
    }
}

impl FromOpts for MockServerStore {
    type Opts = MockOpts;

    fn from_opts<I: AsRef<Path>>(path: I, _opts: &Self::Opts) -> Result<Self, StoreError> {
        Ok(MockServerStore {
            dir: TempDir::new(path.as_ref().to_string_lossy().as_ref())
                .expect("Failed to build temporary directory"),
        })
    }
}

impl ByteEngine for MockServerStore {
    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }
}

impl SwimStore for MockServerStore {
    type PlaneStore = SwimPlaneStore<NoStore>;

    fn plane_store<I>(&mut self, path: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString,
    {
        Ok(SwimPlaneStore::new(
            path.to_string(),
            NoStore {
                path: path.to_string().into(),
            },
        ))
    }
}

#[derive(Clone, Debug)]
pub struct MockNodeStore;

impl MockNodeStore {
    fn mock() -> SwimNodeStore<MockPlaneStore> {
        let plane_store = MockPlaneStore;
        SwimNodeStore::new(plane_store, "test_node")
    }
}

impl NodeStore for MockNodeStore {
    type Delegate = MockPlaneStore;

    fn map_lane_store<I, K, V>(&self, lane: I) -> MapDataModel<Self::Delegate, K, V>
    where
        I: Into<Text>,
        K: Serialize,
        V: Serialize,
        Self: Sized,
    {
        MapDataModel::new(MockNodeStore::mock(), lane)
    }

    fn value_lane_store<I, V>(&self, lane: I) -> ValueDataModel<Self::Delegate>
    where
        I: Into<Text>,
        V: Serialize,
        Self: Sized,
    {
        ValueDataModel::new(MockNodeStore::mock(), lane)
    }

    fn put(&self, _key: LaneKey, _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: LaneKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: LaneKey) -> Result<(), StoreError> {
        Ok(())
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: "Mock".to_string(),
            kind: "Mock".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MockPlaneStore;
impl PlaneStore for MockPlaneStore {
    type NodeStore = MockNodeStore;

    fn node_store<I>(&self, _node: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        MockNodeStore
    }

    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: StoreKey<'_, '_>,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }

    fn put(&self, _key: StoreKey, _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        Ok(())
    }

    fn store_info(&self) -> StoreInfo {
        todo!()
    }
}
