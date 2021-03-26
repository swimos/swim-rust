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
use crate::stores::plane::SwimPlaneStore;
use crate::stores::{DatabaseStore, StoreKey};
use crate::{
    Destroy, FromOpts, KeyedSnapshot, RangedSnapshot, Store, StoreEngine, StoreError,
    StoreInitialisationError, SwimStore,
};
use std::path::Path;

pub struct MockServerStore;

impl Store for MockServerStore {}

impl FromOpts for MockServerStore {
    type Opts = ();

    fn from_opts<I: AsRef<Path>>(
        _path: I,
        _opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        Ok(MockServerStore)
    }
}

impl Destroy for MockServerStore {
    fn destroy(self) {}
}

impl RangedSnapshot for MockServerStore {
    type Prefix = StoreKey;

    fn ranged_snapshot<F, K, V>(
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

impl SwimStore for MockServerStore {
    type PlaneStore = SwimPlaneStore;

    fn plane_store<I>(&mut self, _path: I) -> Result<Self::PlaneStore, StoreError>
    where
        I: ToString,
    {
        Ok(SwimPlaneStore::new(
            "target".into(),
            DatabaseStore::new(EmptyDelegateStore),
            DatabaseStore::new(EmptyDelegateStore),
        ))
    }
}

impl From<EmptyDelegateStore> for StoreDelegate {
    fn from(mock: EmptyDelegateStore) -> Self {
        StoreDelegate::Mock(mock)
    }
}

impl<'a> StoreEngine<'a> for MockServerStore {
    type Key = &'a [u8];
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct EmptyDelegateStore;
impl<'a> StoreEngine<'a> for EmptyDelegateStore {
    type Key = &'a [u8];
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        Ok(())
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl RangedSnapshot for EmptyDelegateStore {
    type Prefix = Vec<u8>;

    fn ranged_snapshot<F, K, V>(
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
