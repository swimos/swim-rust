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

use crate::plane::store::keystore::{incrementing_merge_operator, COUNTER_KEY};
use crate::store::{LANE_KS, MAP_LANE_KS, VALUE_LANE_KS};
use std::mem::size_of;
use store::keyspaces::{KeyType, KeyspaceDef, Keyspaces};
use store::{Options, RocksDatabase, RocksOpts, SliceTransform};

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
