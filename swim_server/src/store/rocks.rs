// Copyright 2015-2021 SWIM.AI inc.
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

use crate::store::keystore::rocks::incrementing_merge_operator;
use crate::store::keystore::COUNTER_KEY;
use crate::store::{LANE_KS, MAP_LANE_KS, VALUE_LANE_KS};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, SliceTransform, DB};
use std::mem::size_of;
use std::path::Path;
use swim_store::{
    EngineInfo, EngineIterOpts, EngineRefIterator, Keyspace, KeyspaceByteEngine, KeyspaceDef,
    KeyspaceResolver, Keyspaces, RocksEngine, RocksIterator, RocksPrefixIterator, Store,
    StoreBuilder, StoreError,
};

const PREFIX_BLOOM_RATIO: f64 = 0.2;

#[derive(Debug, Clone)]
pub struct RocksDatabase {
    db: RocksEngine,
}

impl Store for RocksDatabase {
    fn path(&self) -> &Path {
        self.db.path()
    }

    fn engine_info(&self) -> EngineInfo {
        self.db.engine_info()
    }
}

impl KeyspaceByteEngine for RocksDatabase {
    fn put_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        self.db.put_keyspace(keyspace, key, value)
    }

    fn get_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        self.db.get_keyspace(keyspace, key)
    }

    fn delete_keyspace<K: Keyspace>(&self, keyspace: K, key: &[u8]) -> Result<(), StoreError> {
        self.db.delete_keyspace(keyspace, key)
    }

    fn merge_keyspace<K: Keyspace>(
        &self,
        keyspace: K,
        key: &[u8],
        step: u64,
    ) -> Result<(), StoreError> {
        self.db.merge_keyspace(keyspace, key, step)
    }

    fn get_prefix_range<F, K, V, S>(
        &self,
        keyspace: S,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        self.db.get_prefix_range(keyspace, prefix, map_fn)
    }
}

impl<'a: 'b, 'b> EngineRefIterator<'a, 'b> for RocksDatabase {
    type EngineIterator = RocksIterator<'b>;
    type EnginePrefixIterator = RocksPrefixIterator<'b>;

    fn iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError> {
        self.db.iterator_opt(space, opts)
    }

    fn prefix_iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
        prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        self.db.prefix_iterator_opt(space, opts, prefix)
    }
}

impl KeyspaceResolver for RocksDatabase {
    type ResolvedKeyspace = ColumnFamily;

    fn resolve_keyspace<K: Keyspace>(&self, space: &K) -> Option<&Self::ResolvedKeyspace> {
        self.db.resolve_keyspace(space)
    }
}

#[derive(Clone)]
pub struct RocksOpts(Options);

impl Default for RocksOpts {
    fn default() -> Self {
        let mut rock_opts = rocksdb::Options::default();
        rock_opts.create_if_missing(true);
        rock_opts.create_missing_column_families(true);

        RocksOpts(rock_opts)
    }
}

impl StoreBuilder for RocksOpts {
    type Store = RocksDatabase;

    fn build<I>(self, path: I, keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>,
    {
        let Keyspaces { keyspaces } = keyspaces;
        let descriptors =
            keyspaces
                .iter()
                .fold(Vec::with_capacity(keyspaces.len()), |mut vec, def| {
                    let cf_descriptor =
                        ColumnFamilyDescriptor::new(def.name.to_string(), def.opts.0.clone());
                    vec.push(cf_descriptor);
                    vec
                });

        let db = DB::open_cf_descriptors(&self.0, path, descriptors)
            .map_err(|e| StoreError::Delegate(Box::new(e)))?;
        Ok(RocksDatabase {
            db: RocksEngine::new(db),
        })
    }
}

pub fn default_keyspaces() -> Keyspaces<RocksOpts> {
    let mut lane_counter_opts = Options::default();
    lane_counter_opts.set_merge_operator_associative(COUNTER_KEY, incrementing_merge_operator);

    let lane_def = KeyspaceDef::new(LANE_KS, RocksOpts(lane_counter_opts));
    let value_def = KeyspaceDef::new(VALUE_LANE_KS, RocksOpts(Options::default()));

    let mut map_opts = Options::default();
    map_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<u64>()));
    map_opts.set_memtable_prefix_bloom_ratio(PREFIX_BLOOM_RATIO);

    let map_def = KeyspaceDef::new(MAP_LANE_KS, RocksOpts(map_opts));

    Keyspaces::new(vec![lane_def, value_def, map_def])
}

pub fn default_db_opts() -> RocksOpts {
    let mut rock_opts = Options::default();
    rock_opts.create_if_missing(true);
    rock_opts.create_missing_column_families(true);

    RocksOpts(rock_opts)
}
