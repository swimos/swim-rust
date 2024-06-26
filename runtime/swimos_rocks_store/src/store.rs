// Copyright 2015-2023 Swim Inc.
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

use std::fmt::Debug;
use std::path::Path;

/// An enumeration over the keyspaces that exist in a store.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KeyspaceName {
    Lane,
    Value,
    Map,
}

pub use swimos_api::error::StoreError;
/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for a store.
pub trait Store:
    KeyspaceByteEngine + KeyspaceByteEngine + KeyspaceResolver + Send + Sync + Clone + Debug + 'static
{
}

use crate::keyspaces::{KeyspaceByteEngine, KeyspaceResolver, Keyspaces};

/// Information regarding a delegate store engine that is useful for displaying along with debug
/// information or an error report.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EngineInfo {
    /// The path that the store engine is operating from.
    pub path: String,
    /// The type of store engine: RocksDB/NoStore etc.
    pub kind: String,
}

pub trait StoreBuilder: Sized + Clone {
    type Store: Store;

    fn build<I>(self, path: I, keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>;
}
