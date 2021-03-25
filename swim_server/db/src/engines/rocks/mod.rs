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

pub mod snapshot;

use crate::engines::StoreDelegate;
use crate::{Destroy, FromOpts, Store, StoreEngine, StoreError, StoreInitialisationError};
use rocksdb::{Error, Options, DB};
use std::path::Path;
use std::sync::Arc;

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

#[derive(Clone)]
pub struct RocksDatabase {
    delegate: Arc<DB>,
}

impl RocksDatabase {
    pub fn new(delegate: DB) -> RocksDatabase {
        RocksDatabase {
            delegate: Arc::new(delegate),
        }
    }
}

impl From<RocksDatabase> for StoreDelegate {
    fn from(d: RocksDatabase) -> Self {
        StoreDelegate::Rocksdb(d)
    }
}

impl<'i> StoreEngine<'i> for RocksDatabase {
    type Key = &'i [u8];
    type Value = &'i [u8];
    type Error = rocksdb::Error;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        delegate.put(key, value)
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        delegate.get(key)
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        delegate.delete(key).map(|_| true)
    }
}

impl Store for RocksDatabase {
    fn path(&self) -> String {
        self.delegate.path().to_string_lossy().to_string()
    }
}

impl Destroy for RocksDatabase {
    fn destroy(self) {
        let _ = DB::destroy(&Options::default(), self.delegate.path());
    }
}

impl FromOpts for RocksDatabase {
    type Opts = Options;

    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        let db = DB::open(opts, path)?;
        Ok(RocksDatabase::new(db))
    }
}

impl From<rocksdb::Error> for StoreInitialisationError {
    fn from(e: Error) -> Self {
        StoreInitialisationError::Error(e.to_string())
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
