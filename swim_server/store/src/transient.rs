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

use crate::engines::{FromKeyspaces, RocksEngine};
use crate::keyspaces::Keyspaces;
use crate::StoreError;
use std::ops::Deref;
use tempdir::TempDir;

impl Deref for TransientDatabase {
    type Target = RocksEngine;

    fn deref(&self) -> &Self::Target {
        &self.delegate
    }
}

/// A wrapper around a `RocksDatabase` that holds a reference to a temporary directory that is
/// cleared when its dropped.
pub struct TransientDatabase {
    _dir: TempDir,
    delegate: RocksEngine,
}

impl TransientDatabase {
    pub fn new(keyspaces: Keyspaces<RocksEngine>) -> Result<TransientDatabase, StoreError> {
        let dir = TempDir::new("test").map_err(StoreError::Io)?;
        let delegate = RocksEngine::from_keyspaces(dir.path(), &Default::default(), keyspaces)?;

        Ok(TransientDatabase {
            _dir: dir,
            delegate,
        })
    }
}
