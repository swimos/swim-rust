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

#[cfg(test)]
mod tests;

use crate::engines::db::StoreDelegate;
use crate::{
    FromOpts, KeyedSnapshot, RangedSnapshot, Store, StoreEngine, StoreError,
    StoreInitialisationError,
};
use rocksdb::{Error, Options, DB};
use std::path::Path;
use std::sync::Arc;

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

#[derive(Clone, Debug)]
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

    fn delete(&self, key: Self::Key) -> Result<(), Self::Error> {
        let RocksDatabase { delegate, .. } = self;
        delegate.delete(key)
    }
}

impl Store for RocksDatabase {}

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

impl RangedSnapshot for RocksDatabase {
    type Prefix = Vec<u8>;

    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let db = &*self.delegate;
        let mut raw = db.raw_iterator();
        let mut data = Vec::new();

        raw.seek(prefix.clone());

        loop {
            if raw.valid() {
                match (raw.key(), raw.value()) {
                    (Some(key), Some(value)) => {
                        if !key.starts_with(&prefix) {
                            break;
                        } else {
                            let mapped = map_fn(key, value)?;
                            data.push(mapped);
                            raw.next();
                        }
                    }
                    _ => panic!(),
                }
            } else {
                break;
            }
        }

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(KeyedSnapshot::new(data.into_iter())))
        }
    }
}
