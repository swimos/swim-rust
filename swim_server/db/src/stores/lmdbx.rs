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

use crate::{StoreEngine, StoreError};
use heed::types::ByteSlice;
use heed::{Database, Env, Error};

impl From<heed::Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Error(e.to_string())
    }
}

pub struct LmdbxDatabase {
    delegate: Database<ByteSlice, ByteSlice>,
    env: Env,
}

impl LmdbxDatabase {
    pub fn new(delegate: Database<ByteSlice, ByteSlice>, env: Env) -> Self {
        LmdbxDatabase { delegate, env }
    }
}

impl<'a> StoreEngine<'a> for LmdbxDatabase {
    type Key = &'a [u8];
    type Value = &'a [u8];
    type Error = heed::Error;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let LmdbxDatabase { delegate, env } = self;
        let mut wtxn = env.write_txn()?;

        delegate.put(&mut wtxn, key, value)?;
        wtxn.commit()
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let LmdbxDatabase { delegate, env } = self;
        let rtxn = env.read_txn()?;

        delegate.get(&rtxn, key).map(|e| e.map(|e| e.to_vec()))
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let LmdbxDatabase { delegate, env } = self;
        let mut wtxn = env.write_txn()?;

        let result = delegate.delete(&mut wtxn, key)?;
        wtxn.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::stores::lmdbx::LmdbxDatabase;
    use crate::StoreEngine;
    use heed::types::UnalignedSlice;
    use heed::{Database, EnvOpenOptions};
    use std::fs;
    use std::path::Path;

    #[test]
    fn test() {
        fs::create_dir_all(Path::new("target").join("bytemuck.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .open(Path::new("target").join("bytemuck.mdb"))
            .unwrap();
        let db: Database<UnalignedSlice<u8>, UnalignedSlice<u8>> =
            env.create_database(None).unwrap();

        let lm = LmdbxDatabase::new(db, env);

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
