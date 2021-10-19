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

mod nostore;

#[cfg(feature = "rocks")]
mod rocks;

#[cfg(feature = "rocks")]
pub use rocks::{RocksEngine, RocksIterator, RocksPrefixIterator};

use std::path::Path;

use crate::keyspaces::Keyspaces;
use crate::{Store, StoreError};
pub use nostore::{NoStore, NoStoreOpts};

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
