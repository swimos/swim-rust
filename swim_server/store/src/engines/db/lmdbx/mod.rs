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
use crate::{FromOpts, KeyedSnapshot, RangedSnapshot, Store, StoreEngine, StoreError};
use heed::types::ByteSlice;
use heed::{Database, Env, EnvOpenOptions, Error};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

pub struct LmdbxDatabaseInner {
    path: PathBuf,
    delegate: Arc<Database<ByteSlice, ByteSlice>>,
    env: Arc<Env>,
}

impl PartialEq for LmdbxDatabaseInner {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

impl LmdbxDatabaseInner {
    pub fn new(
        path: PathBuf,
        delegate: Arc<Database<ByteSlice, ByteSlice>>,
        env: Arc<Env>,
    ) -> Self {
        LmdbxDatabaseInner {
            path,
            delegate,
            env,
        }
    }
}

#[derive(PartialEq)]
pub struct LmdbxDatabase {
    inner: Arc<LmdbxDatabaseInner>,
}

impl Debug for LmdbxDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbxDatabase")
            .field("path", &self.inner.path)
            .finish()
    }
}

impl From<LmdbxDatabase> for StoreDelegate {
    fn from(d: LmdbxDatabase) -> Self {
        StoreDelegate::Lmdbx(d)
    }
}

impl LmdbxDatabase {
    pub fn new(
        path: PathBuf,
        delegate: Arc<Database<ByteSlice, ByteSlice>>,
        env: Arc<Env>,
    ) -> Self {
        LmdbxDatabase {
            inner: Arc::new(LmdbxDatabaseInner::new(path, delegate, env)),
        }
    }

    fn from_raw(path: PathBuf, delegate: Database<ByteSlice, ByteSlice>, env: Env) -> Self {
        LmdbxDatabase {
            inner: Arc::new(LmdbxDatabaseInner::new(
                path,
                Arc::new(delegate),
                Arc::new(env),
            )),
        }
    }

    fn init<P: AsRef<Path>>(path: P, config: &EnvOpenOptions) -> Result<Self, StoreError> {
        let path = path.as_ref().to_owned();

        if !path.exists() {
            std::fs::create_dir_all(&path)
                .map_err(|e| StoreError::InitialisationFailure(e.to_string()))?;
        }

        let env = config.open(path.deref())?;
        let db = env.create_database(None)?;

        Ok(LmdbxDatabase::from_raw(path, db, env))
    }

    pub fn path(&self) -> &Path {
        &self.inner.path.as_path()
    }
}

impl Store for LmdbxDatabase {}

impl FromOpts for LmdbxDatabase {
    type Opts = EnvOpenOptions;

    fn from_opts<I: AsRef<Path>>(path: I, opts: &Self::Opts) -> Result<Self, StoreError> {
        LmdbxDatabase::init(path, opts)
    }
}

impl RangedSnapshot for LmdbxDatabase {
    type Prefix = Vec<u8>;

    fn ranged_snapshot<F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
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

#[derive(Clone)]
pub struct LmdbxOpts {
    pub open_opts: EnvOpenOptions,
}

impl<'i> StoreEngine<'i> for LmdbxDatabase {
    type Key = &'i [u8];
    type Value = &'i [u8];
    type Error = heed::Error;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
        let mut wtxn = env.write_txn()?;

        delegate.put(&mut wtxn, key, value)?;
        wtxn.commit()
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
        let rtxn = env.read_txn()?;

        delegate.get(&rtxn, key).map(|e| e.map(|e| e.to_vec()))
    }

    fn delete(&self, key: Self::Key) -> Result<(), Self::Error> {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
        let mut wtxn = env.write_txn()?;
        let _result = delegate.delete(&mut wtxn, key)?;

        wtxn.commit()?;
        Ok(())
    }
}
