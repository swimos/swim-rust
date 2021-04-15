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

use crate::{
    ByteEngine, FromOpts, KeyedSnapshot, RangedSnapshotLoad, Store, StoreError, StoreOpts,
};
use rocksdb::{Error, Options, DB};
use std::path::Path;
use std::sync::Arc;

const INCONSISTENT_DB: &str = "Missing key or value in store";

impl From<rocksdb::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Delegate(Box::new(e))
    }
}

#[derive(Debug)]
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

impl ByteEngine for RocksDatabase {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError> {
        let RocksDatabase { delegate, .. } = self;
        delegate.put(key, value).map_err(Into::into)
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StoreError> {
        let RocksDatabase { delegate, .. } = self;
        delegate.get(key).map_err(Into::into)
    }

    fn delete(&self, key: Vec<u8>) -> Result<(), StoreError> {
        let RocksDatabase { delegate, .. } = self;
        delegate.delete(key).map_err(Into::into)
    }
}

impl Store for RocksDatabase {
    fn path(&self) -> &Path {
        &self.delegate.path()
    }
}

impl FromOpts for RocksDatabase {
    type Opts = RocksOpts;

    fn from_opts<I: AsRef<Path>>(path: I, opts: &Self::Opts) -> Result<Self, StoreError> {
        let db = DB::open(&opts.0, path)?;
        Ok(RocksDatabase::new(db))
    }
}

pub struct RocksOpts(pub Options);

impl StoreOpts for RocksOpts {}

impl Default for RocksOpts {
    fn default() -> Self {
        let mut rock_opts = rocksdb::Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        RocksOpts(rock_opts)
    }
}

impl RangedSnapshotLoad for RocksDatabase {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: Vec<u8>,
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
                    _ => return Err(StoreError::Decoding(INCONSISTENT_DB.to_string())),
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
