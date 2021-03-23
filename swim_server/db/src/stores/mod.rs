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

use std::marker::PhantomData;

use serde::Serialize;

use crate::engines::StoreDelegate;
use crate::{Iterable, Snapshot, StoreEngine, StoreError};

pub mod key;
pub mod plane;

pub struct DatabaseStore<K> {
    delegate: StoreDelegate,
    _key_pd: PhantomData<K>,
}

impl<K> DatabaseStore<K> {
    pub fn new<D>(delegate: D) -> Self
    where
        D: for<'a> StoreEngine<'a> + Into<StoreDelegate>,
    {
        DatabaseStore {
            delegate: delegate.into(),
            _key_pd: Default::default(),
        }
    }

    fn serialize<S: Serialize>(&self, key: &S) -> Result<Vec<u8>, StoreError> {
        bincode::serialize(key).map_err(Into::into)
    }
}

impl<'i, K: 'i> StoreEngine<'i> for DatabaseStore<K>
where
    K: Serialize,
{
    type Key = &'i K;
    type Value = &'i [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let key = self.serialize(key)?;
        self.delegate.put(key.as_slice(), value).map_err(Into::into)
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = self.serialize(key)?;
        self.delegate.get(key.as_slice()).map_err(Into::into)
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        let key = self.serialize(key)?;
        self.delegate.delete(key.as_slice()).map_err(Into::into)
    }
}

pub struct StoreSnapshot;

impl<'a, V: 'a> Snapshot<'a> for DatabaseStore<V>
where
    V: Send + Sync,
{
    type Snapshot = StoreSnapshot;

    fn snapshot(&'a self) -> Self::Snapshot {
        unimplemented!()
    }
}

impl Iterable for StoreSnapshot {
    type Iterator = StoreSnapshotIterator;
}

pub struct StoreSnapshotIterator;
impl Iterator for StoreSnapshotIterator {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
