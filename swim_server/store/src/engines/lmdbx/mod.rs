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

use crate::engines::keyspaces::{KeyspaceByteEngine, KeyspaceName, Keyspaces};
use crate::engines::{KeyedSnapshot, RangedSnapshotLoad, StoreOpts};
use crate::stores::lane::{deserialize, serialize};
use crate::{FromKeyspaces, Store, StoreError, StoreInfo};
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

struct SubDatabases {
    lanes: Database<ByteSlice, ByteSlice>,
    map: Database<ByteSlice, ByteSlice>,
    value: Database<ByteSlice, ByteSlice>,
}

impl SubDatabases {
    fn get(&self, keyspace: KeyspaceName) -> &Database<ByteSlice, ByteSlice> {
        match keyspace {
            KeyspaceName::Lane => &self.lanes,
            KeyspaceName::Map => &self.map,
            KeyspaceName::Value => &self.value,
        }
    }
}

/// An Libmdbx database engine.
///
/// See https://github.com/erthink/libmdbx for details about its features and limitations.
pub struct LmdbxDatabase {
    /// The path to the database directory.
    path: PathBuf,
    /// The delegate Libmdbx databases.
    sub_databases: SubDatabases,
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
    fn init<P: AsRef<Path>>(
        path: P,
        config: &EnvOpenOptions,
        keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_owned();

        if !path.exists() {
            std::fs::create_dir_all(&path)
                .map_err(|e| StoreError::InitialisationFailure(e.to_string()))?;
        }

        let env = config.open(path.deref())?;

        let Keyspaces { lane, value, map } = keyspaces;
        let lane_db = env.create_database(Some(lane.name))?;
        let value_db = env.create_database(Some(value.name))?;
        let map_db = env.create_database(Some(map.name))?;

        let sub_databases = SubDatabases {
            lanes: lane_db,
            map: map_db,
            value: value_db,
        };

        Ok(LmdbxDatabase {
            path,
            sub_databases,
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

impl FromKeyspaces for LmdbxDatabase {
    type EnvironmentOpts = LmdbOpts;
    type KeyspaceOpts = ();

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        db_opts: &Self::EnvironmentOpts,
        keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        LmdbxDatabase::init(path, &db_opts.0, keyspaces)
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
        keyspace: KeyspaceName,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let LmdbxDatabase {
            sub_databases, env, ..
        } = self;
        let delegate = sub_databases.get(keyspace);
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

impl KeyspaceByteEngine for LmdbxDatabase {
    fn put_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        let LmdbxDatabase {
            sub_databases, env, ..
        } = self;
        let mut wtxn = env.write_txn()?;
        let delegate = sub_databases.get(keyspace);

        delegate.put(&mut wtxn, key, value)?;
        wtxn.commit().map_err(Into::into)
    }

    fn get_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let LmdbxDatabase {
            sub_databases, env, ..
        } = self;
        let rtxn = env.read_txn()?;
        let delegate = sub_databases.get(keyspace);

        delegate
            .get(&rtxn, key)
            .map(|e| e.map(|e| e.to_vec()))
            .map_err(Into::into)
    }

    fn delete_keyspace(&self, keyspace: KeyspaceName, key: &[u8]) -> Result<(), StoreError> {
        let LmdbxDatabase {
            sub_databases, env, ..
        } = self;
        let mut wtxn = env.write_txn()?;
        let delegate = sub_databases.get(keyspace);
        let _result = delegate.delete(&mut wtxn, key)?;

        wtxn.commit()?;
        Ok(())
    }

    fn merge_keyspace(
        &self,
        keyspace: KeyspaceName,
        key: &[u8],
        value: u64,
    ) -> Result<(), StoreError> {
        let LmdbxDatabase {
            sub_databases, env, ..
        } = self;
        let delegate = sub_databases.get(keyspace);

        let rtxn = env.read_txn()?;
        let value_opt = delegate.get(&rtxn, key)?;

        let mut new_value = match value_opt {
            Some(value) => deserialize::<u64>(value)?,
            None => 0,
        };

        new_value += value;

        let mut wtxn = env.write_txn()?;
        delegate
            .put(&mut wtxn, key, serialize(&new_value)?.as_slice())
            .map_err(Into::into)
    }
}
