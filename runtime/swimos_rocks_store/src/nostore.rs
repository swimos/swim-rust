// Copyright 2015-2024 Swim Inc.
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

use std::path::Path;

use swimos_api::error::StoreError;
use swimos_api::persistence::{KeyValue, RangeConsumer};

use crate::keyspaces::{Keyspace, KeyspaceByteEngine, KeyspaceResolver, Keyspaces};
use crate::store::{Store, StoreBuilder};

/// A delegate store database that does nothing.
#[derive(Debug, Clone)]
pub struct NoStore;

impl Store for NoStore {}

impl KeyspaceResolver for NoStore {
    type ResolvedKeyspace = ();

    fn resolve_keyspace<K: Keyspace>(&self, _space: &K) -> Option<&Self::ResolvedKeyspace> {
        None
    }
}

pub struct NoRange;

impl RangeConsumer for NoRange {
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        Ok(None)
    }
}

impl KeyspaceByteEngine for NoStore {
    type RangeCon<'a>
        = NoRange
    where
        Self: 'a;

    fn get_prefix_range_consumer<'a, S>(
        &'a self,
        _keyspace: S,
        _prefix: &[u8],
    ) -> Result<Self::RangeCon<'a>, StoreError>
    where
        S: Keyspace,
    {
        Ok(NoRange)
    }

    fn put_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    fn delete_keyspace<K: Keyspace>(&self, _keyspace: K, _key: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    fn merge_keyspace<K: Keyspace>(
        &self,
        _keyspace: K,
        _key: &[u8],
        _value: u64,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn delete_key_range<S>(
        &self,
        _keyspace: S,
        _start: &[u8],
        _ubound: &[u8],
    ) -> Result<(), StoreError>
    where
        S: Keyspace,
    {
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct NoStoreOpts;
impl StoreBuilder for NoStoreOpts {
    type Store = NoStore;

    fn build<I>(self, _path: I, _keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>,
    {
        Ok(NoStore)
    }
}
