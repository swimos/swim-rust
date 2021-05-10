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

use crate::engines::keyspaces::{
    failing_keystore, KeyType, KeyspaceByteEngine, KeyspaceName, KeyspaceOptions, KeyspaceResolver,
    Keyspaces,
};
use crate::engines::{KeyedSnapshot, NoStore};
use crate::iterator::{
    EngineIterOpts, EngineIterator, EnginePrefixIterator, EngineRefIterator, IteratorKey,
};
use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::node::NodeStore;
use crate::stores::plane::{PlaneStore, SwimPlaneStore};
use crate::stores::StoreKey;
use crate::{
    ByteEngine, FromKeyspaces, RangedSnapshotLoad, Store, StoreError, StoreInfo, SwimNodeStore,
    SwimStore,
};
use futures::future::ready;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::Serialize;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use swim_common::model::text::Text;
use tempdir::TempDir;

impl<D> Deref for TransientDatabase<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

pub struct TransientDatabase<D> {
    _dir: TempDir,
    delegate: D,
}

impl<D> TransientDatabase<D>
where
    D: Store,
{
    #[allow(dead_code)]
    pub(crate) fn new(keyspace_opts: KeyspaceOptions<D::KeyspaceOpts>) -> TransientDatabase<D> {
        let dir = TempDir::new("test").expect("Failed to create temporary directory");
        let delegate = D::from_keyspaces(
            dir.path(),
            &Default::default(),
            &Keyspaces::from(keyspace_opts),
        )
        .expect("Failed to build delegate store");

        TransientDatabase {
            _dir: dir,
            delegate,
        }
    }
}

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

impl KeyspaceResolver for MockServerStore {
    type ResolvedKeyspace = KeyspaceName;

    fn resolve_keyspace(&self, _space: &KeyspaceName) -> Option<&Self::ResolvedKeyspace> {
        // Some(space)
        unimplemented!()
    }
}

pub struct MockEngineIterator;
impl EngineIterator for MockEngineIterator {
    fn seek_to(&mut self, _key: IteratorKey) -> Result<bool, StoreError> {
        Ok(true)
    }

    fn seek_next(&mut self) {}

    fn key(&self) -> Option<&[u8]> {
        None
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn valid(&self) -> Result<bool, StoreError> {
        Ok(true)
    }
}

pub struct MockEnginePrefixIterator;
impl EnginePrefixIterator for MockEnginePrefixIterator {
    fn next(&mut self) -> Option<(Box<[u8]>, Box<[u8]>)> {
        None
    }

    fn valid(&self) -> Result<bool, StoreError> {
        Ok(false)
    }
}

impl<'a: 'b, 'b> EngineRefIterator<'a, 'b> for MockServerStore {
    type EngineIterator = MockEngineIterator;
    type EnginePrefixIterator = MockEnginePrefixIterator;

    fn iterator_opt(
        &'a self,
        _space: &'b Self::ResolvedKeyspace,
        _pts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError> {
        Ok(MockEngineIterator)
    }

    fn prefix_iterator_opt(
        &'a self,
        _space: &'b Self::ResolvedKeyspace,
        _opts: EngineIterOpts,
        _prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        Ok(MockEnginePrefixIterator)
    }
}

impl KeyspaceByteEngine for MockServerStore {
    fn put_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete_keyspace(&self, _keyspace: KeyspaceName, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn merge_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
        _value: KeyType,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshotLoad for MockServerStore {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        _keyspace: KeyspaceName,
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

impl Default for MockOpts {
    fn default() -> Self {
        MockOpts
    }
}

impl FromKeyspaces for MockServerStore {
    type EnvironmentOpts = MockOpts;
    type KeyspaceOpts = ();

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        _db_opts: &Self::EnvironmentOpts,
        _keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
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
            Arc::new(NoStore {
                path: path.to_string().into(),
            }),
            failing_keystore(),
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

    fn map_lane_store<I, K, V>(
        &self,
        _lane: I,
    ) -> BoxFuture<'static, MapDataModel<Self::Delegate, K, V>>
    where
        I: ToString,
        K: Serialize + Send + 'static,
        V: Serialize + Send + 'static,
        Self: Sized,
    {
        ready(MapDataModel::new(MockNodeStore::mock(), 0)).boxed()
    }

    fn value_lane_store<I, V>(&self, _lane: I) -> BoxFuture<'static, ValueDataModel<Self::Delegate>>
    where
        I: ToString,
        V: Serialize + Send + 'static,
        Self: Sized,
    {
        ready(ValueDataModel::new(MockNodeStore::mock(), 0)).boxed()
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
        _prefix: StoreKey,
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

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<'static, KeyType>
    where
        I: Into<String>,
    {
        async { 0 }.boxed()
    }
}
