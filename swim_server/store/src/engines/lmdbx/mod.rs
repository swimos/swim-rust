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
use heed::types::ByteSlice;
use heed::{Database, Env, EnvOpenOptions, Error};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::path::{Path, PathBuf};

impl From<heed::Error> for StoreError {
    fn from(e: heed::Error) -> Self {
        match e {
            Error::Io(e) => StoreError::Io(e),
            Error::Mdb(e) => StoreError::Delegate(Box::new(e)),
            e @ Error::Encoding => StoreError::Encoding(e.to_string()),
            e @ Error::Decoding => StoreError::Encoding(e.to_string()),
            e @ Error::InvalidDatabaseTyping => StoreError::Delegate(Box::new(e)),
            Error::DatabaseClosing => StoreError::Closing,
        }
    }
}

impl PartialEq for LmdbxDatabase {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

pub struct LmdbxDatabase {
    path: PathBuf,
    delegate: Database<ByteSlice, ByteSlice>,
    env: Env,
}

impl Debug for LmdbxDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbxDatabase")
            .field("path", &self.path)
            .finish()
    }
}

impl LmdbxDatabase {
    fn init<P: AsRef<Path>>(path: P, config: &EnvOpenOptions) -> Result<Self, StoreError> {
        let path = path.as_ref().to_owned();

        if !path.exists() {
            std::fs::create_dir_all(&path)
                .map_err(|e| StoreError::InitialisationFailure(e.to_string()))?;
        }

        let env = config.open(path.deref())?;
        let db = env.create_database(None)?;

        Ok(LmdbxDatabase {
            path,
            delegate: db,
            env,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }
}

impl Store for LmdbxDatabase {
    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl FromOpts for LmdbxDatabase {
    type Opts = LmdbOpts;

    fn from_opts<I: AsRef<Path>>(path: I, opts: &Self::Opts) -> Result<Self, StoreError> {
        LmdbxDatabase::init(path, &opts.0)
    }
}

pub struct LmdbOpts(pub EnvOpenOptions);

impl StoreOpts for LmdbOpts {}

impl Default for LmdbOpts {
    fn default() -> Self {
        LmdbOpts(EnvOpenOptions::new())
    }
}

impl RangedSnapshotLoad for LmdbxDatabase {
    type Prefix = Vec<u8>;

    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let LmdbxDatabase { delegate, env, .. } = self;
        let tx = env.read_txn()?;

        let mut it = delegate
            .prefix_iter(&tx, &prefix)
            .map_err(|e| StoreError::Snapshot(e.to_string()))?;

        let data = it.try_fold(Vec::new(), |mut vec, result| match result {
            Ok((key, value)) => {
                let mapped = map_fn(key, value)?;
                vec.push(mapped);

                Ok(vec)
            }
            Err(e) => Err(StoreError::Snapshot(e.to_string())),
        })?;

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(KeyedSnapshot::new(data.into_iter())))
        }
    }
}

impl ByteEngine for LmdbxDatabase {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let mut wtxn = env.write_txn()?;

        delegate.put(&mut wtxn, key.as_slice(), value.as_slice())?;
        wtxn.commit().map_err(Into::into)
    }

    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let rtxn = env.read_txn()?;

        delegate
            .get(&rtxn, key.as_slice())
            .map(|e| e.map(|e| e.to_vec()))
            .map_err(Into::into)
    }

    fn delete(&self, key: Vec<u8>) -> Result<(), StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let mut wtxn = env.write_txn()?;
        let _result = delegate.delete(&mut wtxn, key.as_slice())?;

        wtxn.commit()?;
        Ok(())
    }
}
