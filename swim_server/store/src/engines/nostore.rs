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
    ByteEngine, FromOpts, KeyedSnapshot, RangedSnapshotLoad, Store, StoreError, StoreOpts,
};
use std::borrow::Borrow;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct NoStore {
    path: PathBuf,
}

impl Store for NoStore {
    fn path(&self) -> &Path {
        self.path.borrow()
    }
}

#[derive(Default)]
pub struct NoStoreOpts;

impl StoreOpts for NoStoreOpts {}

impl FromOpts for NoStore {
    type Opts = NoStoreOpts;

    fn from_opts<I: AsRef<Path>>(path: I, _opts: &Self::Opts) -> Result<Self, StoreError> {
        Ok(NoStore {
            path: path.as_ref().to_path_buf(),
        })
    }
}

impl ByteEngine for NoStore {
    fn put(&self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }

    fn get(&self, _key: Vec<u8>) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete(&self, _key: Vec<u8>) -> Result<(), StoreError> {
        Ok(())
    }
}

impl RangedSnapshotLoad for NoStore {
    type Prefix = Vec<u8>;

    fn load_ranged_snapshot<F, K, V>(
        &self,
        _prefix: Self::Prefix,
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        Ok(None)
    }
}
