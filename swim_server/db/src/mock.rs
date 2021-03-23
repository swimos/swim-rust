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

use crate::{
    Destroy, FromOpts, Iterable, Snapshot, Store, StoreEngine, StoreError,
    StoreInitialisationError, SwimStore,
};
use std::path::Path;

struct TestStore;
impl SwimStore for TestStore {
    type PlaneStore = TestPlaneStore;

    fn plane_store<I>(&mut self, _path: I) -> Result<Self::PlaneStore, StoreError> {
        unimplemented!()
    }
}

struct TestPlaneStore;
impl Store for TestPlaneStore {
    fn path(&self) -> String {
        unimplemented!()
    }
}

impl Destroy for TestPlaneStore {
    fn destroy(self) {
        panic!()
    }
}

impl FromOpts for TestPlaneStore {
    type Opts = ();

    fn from_opts<I: AsRef<Path>>(
        _path: I,
        _opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        unimplemented!()
    }
}

impl<'a> Snapshot<'a> for TestPlaneStore {
    type Snapshot = TestSnapshot;

    fn snapshot(&'a self) -> Self::Snapshot {
        unimplemented!()
    }
}

struct TestSnapshot;
impl Iterable for TestSnapshot {
    type Iterator = SnapshotIterator;
}

struct SnapshotIterator;
impl Iterator for SnapshotIterator {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

impl<'a> StoreEngine<'a> for TestPlaneStore {
    type Key = ();
    type Value = ();
    type Error = StoreError;

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        unimplemented!()
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        unimplemented!()
    }
}
