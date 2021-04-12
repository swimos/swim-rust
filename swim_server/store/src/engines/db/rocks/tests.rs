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

use crate::engines::db::rocks::RocksDatabase;
use crate::engines::db::test_suite;
use crate::engines::db::test_suite::TransientDatabase;
use rocksdb::Options;

fn delegate() -> TransientDatabase<RocksDatabase> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    TransientDatabase::new(opts)
}

#[test]
fn crud() {
    test_suite::crud(delegate());
}

#[test]
fn get_missing() {
    test_suite::get_missing(delegate());
}

#[test]
fn delete_missing() {
    test_suite::delete_missing(delegate());
}

#[test]
fn empty_snapshot() {
    test_suite::empty_snapshot(delegate());
}

#[test]
fn ranged_snapshot() {
    test_suite::ranged_snapshot(delegate());
}
