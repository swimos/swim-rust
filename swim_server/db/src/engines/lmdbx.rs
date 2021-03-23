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

use crate::engines::StoreDelegate;
use crate::{
    Destroy, FromOpts, Iterable, Snapshot, Store, StoreEngine, StoreEngineOpts, StoreError,
    StoreInitialisationError, SwimStore,
};
use heed::types::ByteSlice;
use heed::{Database, Env, EnvOpenOptions, Error};
use rocksdb::DBIterator;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

const LMDB_EXT: &str = "mdb";

impl From<heed::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

struct LmdbxSwimStore {
    databases: HashMap<String, Weak<LmdbxDatabaseInner>>,
    config: EnvOpenOptions,
    base_path: PathBuf,
}

pub struct LmdbxDatabaseInner {
    address: PathBuf,
    delegate: Arc<Database<ByteSlice, ByteSlice>>,
    env: Arc<Env>,
}

impl PartialEq for LmdbxDatabaseInner {
    fn eq(&self, other: &Self) -> bool {
        self.address.eq(&other.address)
    }
}

impl LmdbxDatabaseInner {
    pub fn new(
        address: PathBuf,
        delegate: Arc<Database<ByteSlice, ByteSlice>>,
        env: Arc<Env>,
    ) -> Self {
        LmdbxDatabaseInner {
            address,
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
            .field("path", &self.inner.address)
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
        address: PathBuf,
        delegate: Arc<Database<ByteSlice, ByteSlice>>,
        env: Arc<Env>,
    ) -> Self {
        LmdbxDatabase {
            inner: Arc::new(LmdbxDatabaseInner::new(address, delegate, env)),
        }
    }

    fn from_raw(address: PathBuf, delegate: Database<ByteSlice, ByteSlice>, env: Env) -> Self {
        LmdbxDatabase {
            inner: Arc::new(LmdbxDatabaseInner::new(
                address,
                Arc::new(delegate),
                Arc::new(env),
            )),
        }
    }

    fn from_inner(inner: Arc<LmdbxDatabaseInner>) -> LmdbxDatabase {
        LmdbxDatabase { inner }
    }

    fn init<P: AsRef<Path>>(
        base_path: P,
        address: &String,
        config: &EnvOpenOptions,
    ) -> Result<Self, StoreError> {
        let path = Path::new(base_path.as_ref().into()).join(format!("{}.{}", address, LMDB_EXT));
        if !path.exists() {
            std::fs::create_dir_all(path.deref()).map_err(|e| StoreError::Error(e.to_string()))?;
        }

        let env = config.open(path.deref())?;
        let db = env.create_database(None)?;

        Ok(LmdbxDatabase::from_raw(path, db, env))
    }
}

impl SwimStore for LmdbxSwimStore {
    type PlaneStore = LmdbxDatabase;

    fn plane_store<I: ToString>(&mut self, address: I) -> Result<Self::PlaneStore, StoreError> {
        let address = address.to_string();
        let LmdbxSwimStore {
            databases,
            config,
            base_path,
        } = self;

        let result = match databases.get(&address) {
            Some(weak) => match weak.upgrade() {
                Some(db) => Ok(LmdbxDatabase::from_inner(db)),
                None => LmdbxDatabase::init(base_path.deref(), &address, config),
            },
            None => LmdbxDatabase::init(base_path.deref(), &address, config),
        };

        match result {
            Ok(db) => {
                let weak = Arc::downgrade(&db.inner);
                databases.insert(address, weak);

                Ok(db)
            }
            Err(e) => Err(e),
        }
    }
}

impl Store for LmdbxDatabase {
    fn address(&self) -> String {
        self.inner.address.to_string_lossy().to_string()
    }
}

impl Destroy for LmdbxDatabase {
    fn destroy(self) {
        let _ = std::fs::remove_file(&self.inner.address);
    }
}

impl FromOpts for LmdbxDatabase {
    fn from_opts(_opts: StoreEngineOpts) -> Result<Self, StoreInitialisationError> {
        unimplemented!()
    }
}

pub struct LmdbxSnapshot<'a>(PhantomData<&'a ()>);

impl<'a> Snapshot<'a> for LmdbxDatabase {
    type Snapshot = LmdbxSnapshot<'a>;

    fn snapshot(&'a self) -> Self::Snapshot {
        unimplemented!()
    }
}

impl<'a> Iterable for LmdbxSnapshot<'a> {
    type Iterator = DBIterator<'a>;
}

impl<'a> StoreEngine<'a> for LmdbxDatabase {
    type Key = &'a [u8];
    type Value = &'a [u8];
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
    use crate::engines::lmdbx::{LmdbxDatabase, LmdbxSwimStore};
    use crate::{Store, StoreEngine, SwimStore};
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

    #[test]
    fn store() {
        let mut swim_store = LmdbxSwimStore {
            databases: Default::default(),
            config: EnvOpenOptions::new(),
            base_path: "target".into(),
        };

        let plane_store = swim_store.plane_store("unit2").unwrap();
        let _address = plane_store.address();
    }
}
