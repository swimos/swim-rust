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

use crate::store::keystore::{incrementing_merge_operator, COUNTER_KEY};
use crate::store::keystore::{lane_key_task, KeyRequest, KeystoreTask};
use crate::store::{LANE_KS, MAP_LANE_KS, VALUE_LANE_KS};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::Stream;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use store::engines::{
    FromKeyspaces, KeyedSnapshot, RangedSnapshotLoad, RocksEngine, RocksIterator, RocksOpts,
    RocksPrefixIterator,
};
use store::iterator::{EngineIterOpts, EngineRefIterator};
use store::keyspaces::{
    KeyType, Keyspace, KeyspaceByteEngine, KeyspaceDef, KeyspaceResolver, Keyspaces,
};
use store::{ColumnFamily, Options, SliceTransform, Store, StoreError, StoreInfo};

#[derive(Debug, Clone)]
pub struct RocksDatabase {
    db: RocksEngine,
}

impl FromKeyspaces for RocksDatabase {
    type Opts = RocksOpts;

    fn from_keyspaces<I: AsRef<Path>>(
        path: I,
        db_opts: &Self::Opts,
        keyspaces: Keyspaces<Self>,
    ) -> Result<Self, StoreError> {
        let keyspace_defs = keyspaces.keyspaces.into_iter().map(|ks| ks).collect();
        let keyspaces = Keyspaces::new(keyspace_defs);
        let db = RocksEngine::from_keyspaces(path, db_opts, keyspaces)?;

        Ok(RocksDatabase { db })
    }
}

impl Store for RocksDatabase {
    fn path(&self) -> &Path {
        self.db.path()
    }

    fn store_info(&self) -> StoreInfo {
        self.db.store_info()
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

impl RangedSnapshotLoad for RocksDatabase {
    fn load_ranged_snapshot<F, K, V, S>(
        &self,
        keyspace: S,
        prefix: &[u8],
        map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        self.db.load_ranged_snapshot(keyspace, prefix, map_fn)
    }
}

impl KeystoreTask for RocksDatabase {
    fn run<DB, S>(db: Arc<DB>, events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
    {
        lane_key_task(db, events).boxed()
    }
}

pub fn default_keyspaces() -> Keyspaces<RocksDatabase> {
    let mut lane_counter_opts = Options::default();
    lane_counter_opts.set_merge_operator_associative(COUNTER_KEY, incrementing_merge_operator);

    let lane_def = KeyspaceDef::new(LANE_KS, RocksOpts(lane_counter_opts));
    let value_def = KeyspaceDef::new(VALUE_LANE_KS, RocksOpts(Options::default()));

    let mut map_opts = Options::default();
    map_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<KeyType>()));
    map_opts.set_memtable_prefix_bloom_ratio(0.2);

    let map_def = KeyspaceDef::new(MAP_LANE_KS, RocksOpts(map_opts));

    Keyspaces::new(vec![lane_def, value_def, map_def])
}

pub fn default_db_opts() -> RocksOpts {
    let mut rock_opts = Options::default();
    rock_opts.create_if_missing(true);
    rock_opts.create_missing_column_families(true);

    RocksOpts(rock_opts)
}
