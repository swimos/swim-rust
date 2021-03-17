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

pub mod key;
pub mod stores;

use std::sync::Arc;

pub enum StoreError {
    Error(String),
}

pub struct StoreEngineOpts;

pub enum StoreInitialisationError {}

pub enum SnapshotError {}

pub trait SwimStore: for<'s> Store<'s> {}

pub trait Store<'a>: StoreEngine<'a> + FromOpts + Snapshot<'a> + Send + Sync + 'static {}

pub trait Snapshot<'a>
where
    Self: Send + Sync + Sized + 'static,
{
    type Snapshot: Iterable;

    fn snapshot(&'a self) -> Self::Snapshot;
}

pub trait Iterable {
    type Iterator: Iterator;
}

pub trait FromOpts: Sized {
    fn from_opts(opts: StoreEngineOpts) -> Result<Self, StoreInitialisationError>;
}

pub trait StoreEngine<'a>: Sized {
    type Error: Into<StoreError>;

    fn put(&self, key: &'a [u8], value: &'a [u8]) -> Result<(), Self::Error>;

    fn get(&self, key: &'a [u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    fn delete(&self, key: &'a [u8]) -> Result<bool, Self::Error>;
}

pub struct StoreEntry<'s> {
    pub key: &'s [u8],
    pub value: &'s [u8],
}

pub struct DatabaseStore<E> {
    engine: Arc<E>,
}

impl<E> DatabaseStore<E> {
    pub fn new(engine: E) -> Self {
        DatabaseStore {
            engine: Arc::new(engine),
        }
    }
}

impl<E> Clone for DatabaseStore<E> {
    fn clone(&self) -> Self {
        DatabaseStore {
            engine: self.engine.clone(),
        }
    }
}

impl<'a, E> StoreEngine<'a> for DatabaseStore<E>
where
    E: StoreEngine<'a>,
{
    type Error = E::Error;

    fn put(&self, key: &'a [u8], value: &'a [u8]) -> Result<(), Self::Error> {
        self.engine.put(key, value)
    }

    fn get(&self, key: &'a [u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.engine.get(key)
    }

    fn delete(&self, key: &'a [u8]) -> Result<bool, Self::Error> {
        self.engine.delete(key)
    }
}
