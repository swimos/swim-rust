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

use crate::{
    FromOpts, Iterable, Snapshot, Store, StoreEngine, StoreEngineOpts, StoreError,
    StoreInitialisationError,
};
use rocksdb::{DBIterator, Error, Snapshot as DBSnapshot, DB};

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

pub struct RocksDatabase {
    delegate: DB,
}

impl RocksDatabase {
    pub fn new(delegate: DB) -> Self {
        RocksDatabase { delegate }
    }
}

impl<'a> StoreEngine<'a> for RocksDatabase {
    type Key = &'a [u8];
    type Value = &'a [u8];
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

impl<'a> Store<'a> for RocksDatabase {}

impl<'a> Snapshot<'a> for RocksDatabase {
    type Snapshot = DBSnapshot<'a>;

    fn snapshot(&'a self) -> Self::Snapshot {
        self.delegate.snapshot()
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
    use crate::stores::rocks::RocksDatabase;
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
