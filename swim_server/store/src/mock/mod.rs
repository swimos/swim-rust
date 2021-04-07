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

use crate::engines::db::rocks::RocksDatabase;
use crate::engines::db::StoreDelegate;
use crate::stores::lane::map::MapDataModel;
use crate::stores::lane::observer::StoreObserver;
use crate::stores::lane::value::mem::ValueDataMemStore;
use crate::stores::lane::value::ValueDataModel;
use crate::stores::lane::LaneKey;
use crate::stores::node::NodeStore;
use crate::stores::plane::PlaneStore;
use crate::{StoreEngine, StoreError};
use rocksdb::{Options, DB};
use serde::Serialize;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tempdir::TempDir;

#[derive(Clone, Debug)]
pub struct MockPlaneStore {
    temp_dir: Arc<TempDir>,
    delegate: StoreDelegate,
}

impl MockPlaneStore {
    pub fn new(name: String) -> MockPlaneStore {
        let temp_dir = TempDir::new(name.as_str()).expect("Failed to build mock plane store");
        let path = temp_dir.path().to_path_buf();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = RocksDatabase::new(DB::open(&opts, path).expect("Failed to open Rocks database"));
        let delegate = StoreDelegate::Rocksdb(db);

        MockPlaneStore {
            temp_dir: Arc::new(temp_dir),
            delegate,
        }
    }
}

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
    type Key = &'a [u8];
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct MockNodeStore;
impl NodeStore for MockNodeStore {
    fn map_lane_store<I, K, V>(&self, _lane: I, transient: bool) -> MapDataModel<K, V>
    where
        I: ToString,
        K: Serialize,
        V: Serialize,
        Self: Sized,
    {
        if !transient {
            panic!("Persistence is not supported by mock stores")
        }

        todo!()
    }

    fn value_lane_store<I, V>(
        &self,
        _lane: I,
        transient: bool,
        default_value: V,
    ) -> ValueDataModel<V>
    where
        I: ToString,
        V: Serialize + Send + Sync + 'static,
        Self: Sized,
    {
        if !transient {
            panic!("Persistence is not supported by mock stores")
        }

        ValueDataModel::Mem(ValueDataMemStore::new(default_value))
    }

    fn observable_value_lane_store<I, V>(
        &self,
        _lane: I,
        transient: bool,
        buffer_size: NonZeroUsize,
        default_value: V,
    ) -> (ValueDataModel<V>, StoreObserver<V>)
    where
        I: ToString,
        V: Serialize + Send + Sync + 'static,
        Self: Sized,
    {
        if !transient {
            panic!("Persistence is not supported by mock stores")
        }

        let (model, rx) = ValueDataMemStore::observable(default_value, buffer_size);
        (ValueDataModel::Mem(model), rx)
    }
}

impl<'a> StoreEngine<'a> for MockNodeStore {
    type Key = LaneKey;
    type Value = Vec<u8>;
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        // Only memory stores are ever returned so this _shouldn't_ be called
        panic!("Unsupported operation")
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        // Only memory stores are ever returned so this _shouldn't_ be called
        panic!("Unsupported operation")
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        // Only memory stores are ever returned so this _shouldn't_ be called
        panic!("Unsupported operation")
    }
}
