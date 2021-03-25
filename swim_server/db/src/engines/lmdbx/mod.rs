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
use heed::types::ByteSlice;
use heed::{Database, Env, EnvOpenOptions, Error};
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl From<heed::Error> for StoreInitialisationError {
    fn from(e: Error) -> Self {
        StoreInitialisationError::Error(e.to_string())
    }
}

impl From<heed::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
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

#[derive(Clone, PartialEq)]
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

    fn init<P: AsRef<Path>>(
        path: P,
        config: &EnvOpenOptions,
    ) -> Result<Self, StoreInitialisationError> {
        let path = path.as_ref().to_owned();

        if !path.exists() {
            std::fs::create_dir_all(&path)
                .map_err(|e| StoreInitialisationError::Error(e.to_string()))?;
        }

        let env = config.open(path.deref())?;
        let db = env.create_database(None)?;

        Ok(LmdbxDatabase::from_raw(path, db, env))
    }
}

impl Store for LmdbxDatabase {
    fn path(&self) -> String {
        self.inner.path.to_string_lossy().to_string()
    }
}

impl Destroy for LmdbxDatabase {
    fn destroy(self) {
        let _ = std::fs::remove_file(&self.inner.path);
    }
}

impl FromOpts for LmdbxDatabase {
    type Opts = LmdbxOpts;

    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        let LmdbxOpts { open_opts } = opts;

        LmdbxDatabase::init(path, open_opts)
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

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
        let mut wtxn = env.write_txn()?;
        let result = delegate.delete(&mut wtxn, key)?;

        wtxn.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::engines::lmdbx::LmdbxDatabase;
    use crate::StoreEngine;
    use heed::types::UnalignedSlice;
    use heed::{Database, EnvOpenOptions};
    use std::fs;
    use std::ops::Deref;
    use std::path::Path;

    #[test]
    fn test() {
        fs::create_dir_all(Path::new("target").join("bytemuck.mdb")).unwrap();
        let path = Path::new("target").join("bytemuck.mdb");
        let env = EnvOpenOptions::new().open(path.deref()).unwrap();
        let db: Database<UnalignedSlice<u8>, UnalignedSlice<u8>> =
            env.create_database(None).unwrap();

        let lm = LmdbxDatabase::from_raw(path, db, env);

        assert!(lm.put(b"a", b"a").is_ok());

        let value = lm.get(b"a").unwrap().unwrap();
        assert_eq!(String::from_utf8(value).unwrap(), "a".to_string());

        assert!(lm.put(b"a", b"b").is_ok());

        let value = lm.get(b"a").unwrap().unwrap();
        assert_eq!(String::from_utf8(value).unwrap(), "b".to_string());

        assert!(lm.delete(b"b").is_ok());
        assert_eq!(lm.get(b"b").unwrap(), None);
    }
}
