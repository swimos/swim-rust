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

use crate::engines::NoStore;
use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::node::NodeStore;
use crate::stores::plane::{PlaneStore, SwimPlaneStore};
use crate::stores::{LaneKey, StoreKey};
use crate::{
    ByteEngine, FromOpts, KeyedSnapshot, RangedSnapshotLoad, Store, StoreError, StoreOpts,
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
}

impl RangedSnapshotLoad for MockServerStore {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: Vec<u8>,
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

// impl RangedSnapshotLoad for MockServerStore {
//     type Prefix = Vec<u8>;
//
//     fn load_ranged_snapshot<F, K, V>(
//         &self,
//         _prefix: Self::Prefix,
//         _map_fn: F,
//     ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
//     where
//         F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
//     {
//         Ok(None)
//     }
// }

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
impl NodeStore for MockNodeStore {
    type Delegate = MockPlaneStore;

    fn map_lane_store<I, K, V>(&self, _lane: I) -> MapDataModel<Self::Delegate, K, V>
    where
        I: Into<Text>,
        K: Serialize,
        V: Serialize,
        Self: Sized,
    {
        unimplemented!()
    }

    fn value_lane_store<I, V>(&self, _lane: I) -> ValueDataModel<Self::Delegate>
    where
        I: Into<Text>,
        V: Serialize,
        Self: Sized,
    {
        unimplemented!()
    }

    fn put(&self, _key: LaneKey, _value: Vec<u8>) -> Result<(), StoreError> {
        todo!()
    }

    fn get(&self, _key: LaneKey) -> Result<Option<Vec<u8>>, StoreError> {
        todo!()
    }

    fn delete(&self, _key: LaneKey) -> Result<(), StoreError> {
        todo!()
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

    fn put(&self, _key: StoreKey, _value: Vec<u8>) -> Result<(), StoreError> {
        todo!()
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        todo!()
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        todo!()
    }
}
