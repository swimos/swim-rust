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

use crate::stores::key::{MapStorageKey, StoreKey, ValueStorageKey};
use crate::stores::{DatabaseStore, StoreSnapshot};
use crate::{
    Destroy, FromOpts, Snapshot, Store, StoreEngine, StoreEngineOpts, StoreError,
    StoreInitialisationError,
};
use std::sync::Arc;

pub struct PlaneStoreInner<'l> {
    map_store: DatabaseStore<MapStorageKey<'l>>,
    value_store: DatabaseStore<ValueStorageKey<'l>>,
}

impl<'l> PlaneStoreInner<'l> {
    pub(crate) fn open(_addr: &String) -> Result<PlaneStore<'_>, StoreError> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct PlaneStore<'l> {
    inner: Arc<PlaneStoreInner<'l>>,
}

impl<'l> PlaneStore<'l> {
    pub fn new(
        map_store: DatabaseStore<MapStorageKey<'l>>,
        value_store: DatabaseStore<ValueStorageKey<'l>>,
    ) -> Self {
        PlaneStore {
            inner: Arc::new(PlaneStoreInner {
                map_store,
                value_store,
            }),
        }
    }

    pub fn from_inner(inner: Arc<PlaneStoreInner<'l>>) -> PlaneStore {
        PlaneStore { inner }
    }
}

impl<'i> StoreEngine<'i> for PlaneStore<'i> {
    type Key = &'i StoreKey<'i>;
    type Value = &'i [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
        } = &self.inner;
        match key {
            StoreKey::Map(key) => map_store.put(key, value),
            StoreKey::Value(key) => value_store.put(key, value),
        }
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
        } = &self.inner;
        match key {
            StoreKey::Map(key) => map_store.get(key),
            StoreKey::Value(key) => value_store.get(key),
        }
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let PlaneStoreInner {
            map_store,
            value_store,
        } = &self.inner;
        match key {
            StoreKey::Map(key) => map_store.delete(key),
            StoreKey::Value(key) => value_store.delete(key),
        }
    }
}

impl<'i> Store for PlaneStore<'i> {
    fn address(&self) -> String {
        unimplemented!()
    }
}

impl<'i> FromOpts for PlaneStore<'i> {
    fn from_opts(_opts: StoreEngineOpts) -> Result<Self, StoreInitialisationError> {
        unimplemented!()
    }
}

impl<'i> Destroy for PlaneStore<'i> {
    fn destroy(self) {
        unimplemented!()
    }
}

impl<'i> Snapshot<'i> for PlaneStore<'i> {
    type Snapshot = StoreSnapshot;

    fn snapshot(&'i self) -> Self::Snapshot {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::rocks::RocksDatabase;
    use crate::stores::key::{StoreKey, ValueStorageKey};
    use crate::stores::plane::PlaneStore;
    use crate::stores::DatabaseStore;
    use crate::StoreEngine;
    use rocksdb::{Options, DB};

    #[test]
    fn t() {
        let map_path = "__test_rocks_db_map";
        let map_db = DB::open_default(map_path).unwrap();

        let value_path = "__test_rocks_db_value";
        let value_db = DB::open_default(value_path).unwrap();

        {
            let map_delegate = RocksDatabase::new(map_db);
            let map_store = DatabaseStore::new(map_delegate);

            let value_delegate = RocksDatabase::new(value_db);
            let value_store = DatabaseStore::new(value_delegate);

            let plane_store = PlaneStore::new(map_store, value_store);

            let value_key = StoreKey::Value(ValueStorageKey {
                node_uri: "/node".into(),
                lane_uri: "/lane".into(),
            });

            assert!(plane_store.put(&value_key, b"test").is_ok());

            let result = plane_store.get(&value_key);
            let vec = result.unwrap().unwrap();
            println!("{:?}", String::from_utf8(vec));
        }

        assert!(DB::destroy(&Options::default(), map_path).is_ok());
        assert!(DB::destroy(&Options::default(), value_path).is_ok());
    }
}
