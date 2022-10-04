// Copyright 2015-2021 Swim Inc.
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

mod iterator;
mod keyspaces;
mod utils;

pub use iterator::*;
pub use keyspaces::*;
pub use utils::*;

use std::fmt::Debug;
use std::path::Path;

pub type KvBytes = (Box<[u8]>, Box<[u8]>);

pub use swim_api::error::StoreError;
/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for a store.
pub trait Store:
    KeyspaceByteEngine
    + KeyspaceByteEngine
    + KeyspaceResolver
    + Send
    + Sync
    + Clone
    + Debug
    + OwnedEngineRefIterator
    + 'static
{
    /// Returns a reference to the path that the delegate byte engine is operating from.
    fn path(&self) -> &Path;

    /// Returns information about the store engine.
    fn engine_info(&self) -> EngineInfo;
}

pub use swim_api::store::RangeConsumer;

/// Information regarding a delegate store engine that is useful for displaying along with debug
/// information or an error report.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EngineInfo {
    /// The path that the store engine is operating from.
    pub path: String,
    /// The type of store engine: RocksDB/NoStore etc.
    pub kind: String,
}

/// A storage engine for server stores that handles byte arrays.
pub trait ByteEngine: 'static {
    /// Put a key-value pair into this store.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Get an entry from this store by its key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;

    /// Delete a value from this store by its key.
    fn delete(&self, key: &[u8]) -> Result<(), StoreError>;
}

pub trait StoreBuilder: Sized + Clone {
    type Store: Store;

    fn build<I>(self, path: I, keyspaces: &Keyspaces<Self>) -> Result<Self::Store, StoreError>
    where
        I: AsRef<Path>;
}
