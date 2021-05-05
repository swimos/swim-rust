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

use crate::engines::keyspaces::{KeyspaceOptions, Keyspaces};
use crate::Store;
use std::ops::Deref;
use tempdir::TempDir;

fn temp_dir() -> TempDir {
    TempDir::new("test").expect("Failed to create temporary directory")
}

impl<D> Deref for TransientDatabase<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

pub struct TransientDatabase<D> {
    _dir: TempDir,
    delegate: D,
}

impl<D> TransientDatabase<D>
where
    D: Store,
{
    pub fn new(keyspace_opts: KeyspaceOptions<D::KeyspaceOpts>) -> TransientDatabase<D> {
        let dir = temp_dir();
        let delegate = D::from_keyspaces(
            dir.path(),
            &Default::default(),
            &Keyspaces::from(keyspace_opts),
        )
        .expect("Failed to build delegate store");

        TransientDatabase {
            _dir: dir,
            delegate,
        }
    }
}
