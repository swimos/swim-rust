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

use crate::engines::{KeyedSnapshot, RangedSnapshotLoad, StoreOpts};
use crate::{ByteEngine, FromOpts, Store, StoreError, StoreInfo};
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
            Error::Encoding(e) => StoreError::Encoding(e.to_string()),
            Error::Decoding(e) => StoreError::Encoding(e.to_string()),
            e @ Error::InvalidDatabaseTyping => StoreError::DelegateMessage(e.to_string()),
            Error::DatabaseClosing => StoreError::Closing,
        }
    }
}

impl PartialEq for LmdbxDatabase {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

/// An Libmdbx database engine.
///
/// See https://github.com/erthink/libmdbx for details about its features and limitations.
pub struct LmdbxDatabase {
    /// The path to the database directory.
    path: PathBuf,
    /// The delegate Libmdbx database.
    delegate: Database<ByteSlice, ByteSlice>,
    /// Libmdbx environment handle.
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
    /// Initialise a Libmdbx database at `path` with the provided environment open options.
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

    /// Returns a reference to the path that the database is opened in.
    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }
}

impl Store for LmdbxDatabase {
    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: self.path.to_string_lossy().to_string(),
            kind: "Libmdbx".to_string(),
        }
    }
}

impl FromOpts for LmdbxDatabase {
    type Opts = LmdbOpts;

    fn from_opts<I: AsRef<Path>>(path: I, opts: &Self::Opts) -> Result<Self, StoreError> {
        LmdbxDatabase::init(path, &opts.0)
    }
}

/// Configuration wrapper for a Libmdbx database used by `FromOpts`.
pub struct LmdbOpts(pub EnvOpenOptions);

impl StoreOpts for LmdbOpts {}

impl Default for LmdbOpts {
    fn default() -> Self {
        LmdbOpts(EnvOpenOptions::new())
    }
}

impl RangedSnapshotLoad for LmdbxDatabase {
    /// Returns a lexicographically ordered iterator of deserialized key-value pairs that exist
    /// in this store that start with `prefix`.
    fn load_ranged_snapshot<F, K, V>(
        &self,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let LmdbxDatabase { delegate, env, .. } = self;
        let tx = env.read_txn()?;

        let mut it = delegate
            .prefix_iter(&tx, prefix)
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
    /// Inserts a key-value pair into this Libmdbx database. If a write transaction already exists,
    /// then this will block the current thread until it finishes.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let mut wtxn = env.write_txn()?;

        delegate.put(&mut wtxn, key, value)?;
        wtxn.commit().map_err(Into::into)
    }

    /// Gets the value associated with `key` if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let rtxn = env.read_txn()?;

        delegate
            .get(&rtxn, key)
            .map(|e| e.map(|e| e.to_vec()))
            .map_err(Into::into)
    }

    /// Delete the key-value pair associated with `key`.
    fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        let LmdbxDatabase { delegate, env, .. } = self;
        let mut wtxn = env.write_txn()?;
        let _result = delegate.delete(&mut wtxn, key)?;

        wtxn.commit()?;
        Ok(())
    }
}
