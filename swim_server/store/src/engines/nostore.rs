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

use crate::engines::keyspaces::{KeyspaceByteEngine, KeyspaceName, Keyspaces};
use crate::engines::{KeyedSnapshot, RangedSnapshotLoad, StoreOpts};
use crate::{ByteEngine, FromKeyspaces, Store, StoreError, StoreInfo};
use std::borrow::Borrow;
use std::path::{Path, PathBuf};

/// A delegate store database that does nothing.
#[derive(Debug)]
pub struct NoStore {
    /// A path stub that is the name of the plane that created it.
    pub(crate) path: PathBuf,
}

impl Store for NoStore {
    fn path(&self) -> &Path {
        self.path.borrow()
    }

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: self.path.to_string_lossy().to_string(),
            kind: "NoStore".to_string(),
        }
    }
}

impl KeyspaceByteEngine for NoStore {
    fn put_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete_keyspace(&self, _keyspace: KeyspaceName, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn merge_keyspace(
        &self,
        _keyspace: KeyspaceName,
        _key: &[u8],
        _value: u64,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshotLoad for NoStore {
    fn load_ranged_snapshot<F, K, V>(
        &self,
        _keyspace: KeyspaceName,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }
}

#[derive(Default)]
pub struct NoStoreOpts;

impl StoreOpts for NoStoreOpts {}

impl FromKeyspaces for NoStore {
    type EnvironmentOpts = NoStoreOpts;
    type KeyspaceOpts = ();

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        _db_opts: &Self::EnvironmentOpts,
        _keyspaces: &Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        Ok(NoStore {
            path: path.as_ref().to_path_buf(),
        })
    }
}

impl ByteEngine for NoStore {
    /// Put operation does nothing and that always succeeds.
    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    /// Get operation does nothing and always returns `Ok(None)`.
    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    /// Delete operation does nothing and always returns `Ok(())`.
    fn delete(&self, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }
}
