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

use crate::engines::StoreDelegate;
use crate::{
    Destroy, FromOpts, Iterable, Snapshot, Store, StoreEngine, StoreEngineOpts, StoreError,
    StoreInitialisationError, SwimStore,
};
use parking_lot::RwLock;
use rocksdb::{DBIterator, Error, Options, Snapshot as DBSnapshot, DB};
use std::path::PathBuf;
use std::sync::Arc;

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

struct RocksSwimStore {
    database: RocksDatabase,
    config: Options,
    base_path: PathBuf,
}

impl RocksSwimStore {
    pub fn new(base_path: PathBuf, database: DB) -> RocksSwimStore {
        RocksSwimStore {
            base_path,
            database: RocksDatabase::new(database),
            config: Options::default(),
        }
    }
}

#[derive(Clone)]
pub struct RocksDatabase {
    // todo: this can be removed if we know all of the possible planes that will exist in the
    //  lifetime of the server
    delegate: Arc<RwLock<DB>>,
}

impl RocksDatabase {
    pub fn new(delegate: DB) -> RocksDatabase {
        RocksDatabase {
            delegate: Arc::new(RwLock::new(delegate)),
        }
    }
}

impl From<RocksDatabase> for StoreDelegate {
    fn from(d: RocksDatabase) -> Self {
        StoreDelegate::Rocksdb(d)
    }
}

impl SwimStore for RocksSwimStore {
    type PlaneStore = RocksDatabase;

    fn plane_store<I: ToString>(&mut self, address: I) -> Result<Self::PlaneStore, StoreError> {
        let address = address.to_string();
        let RocksSwimStore {
            database, config, ..
        } = self;

        let mut db = database.delegate.write();

        if db.cf_handle(address.as_str()).is_none() {
            db.create_cf(address, config)?;
        }

        Ok(database.clone())
    }
}

impl<'a> StoreEngine<'a> for RocksDatabase {
    type Key = &'a [u8];
    type Value = &'a [u8];
    type Error = rocksdb::Error;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        let db = &*delegate.read();

        db.put(key, value)
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        let db = &*delegate.read();

        db.get(key)
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        let db = &*delegate.read();

        db.delete(key).map(|_| true)
    }
}

impl Store for RocksDatabase {
    fn address(&self) -> String {
        self.delegate.read().path().to_string_lossy().to_string()
    }
}

impl Destroy for RocksDatabase {
    fn destroy(self) {
        let _ = DB::destroy(&Options::default(), self.delegate.read().path());
    }
}

impl<'a> Snapshot<'a> for RocksDatabase {
    type Snapshot = DBSnapshot<'a>;

    fn snapshot(&'a self) -> Self::Snapshot {
        unimplemented!()
    }
}

impl<'a> Iterable for DBSnapshot<'a> {
    type Iterator = DBIterator<'a>;
}

impl FromOpts for RocksDatabase {
    fn from_opts(_opts: StoreEngineOpts) -> Result<Self, StoreInitialisationError> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::rocks::RocksDatabase;
    use crate::StoreEngine;
    use rocksdb::{Options, DB};

    #[test]
    fn test() {
        let path = "__test_rocks_db";
        let db = DB::open_default(path).unwrap();

        {
            let rocks = RocksDatabase::new(db);

            assert!(rocks.put(b"a", b"a").is_ok());

            let value = rocks.get(b"a").unwrap().unwrap();
            assert_eq!(String::from_utf8(value).unwrap(), "a".to_string());

            assert!(rocks.put(b"a", b"b").is_ok());

            let value = rocks.get(b"a").unwrap().unwrap();
            assert_eq!(String::from_utf8(value).unwrap(), "b".to_string());

            assert!(rocks.delete(b"a").is_ok());
            assert_eq!(rocks.get(b"a").unwrap(), None);
        }

        assert!(DB::destroy(&Options::default(), path).is_ok());
    }
}
