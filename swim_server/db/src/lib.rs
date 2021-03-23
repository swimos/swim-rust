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

use crate::stores::plane::{PlaneStore, PlaneStoreInner};
use bincode::Error;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

pub mod engines;
mod mock;
pub mod stores;

#[derive(Debug)]
pub enum StoreError {
    Error(String),
}

impl From<bincode::Error> for StoreError {
    fn from(_: Error) -> Self {
        unimplemented!()
    }
}

pub struct StoreEngineOpts;

pub enum StoreInitialisationError {}

pub enum SnapshotError {}

pub trait SwimStore {
    type PlaneStore: Store;

    fn plane_store<I: ToString>(&mut self, address: I) -> Result<Self::PlaneStore, StoreError>;
}

pub trait Store:
    for<'a> StoreEngine<'a> + FromOpts + for<'a> Snapshot<'a> + Send + Sync + Destroy
{
    fn address(&self) -> String;
}

pub trait Destroy {
    fn destroy(self);
}

pub trait Snapshot<'a>
where
    Self: Send + Sync + Sized,
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

pub trait StoreEngine<'a> {
    type Key;
    type Value;
    type Error: Into<StoreError>;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error>;

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error>;
}

struct ServerStore<'s> {
    inner: HashMap<String, Weak<PlaneStoreInner<'s>>>,
}

impl<'s> SwimStore for ServerStore<'s> {
    type PlaneStore = PlaneStore<'s>;

    fn plane_store<I: ToString>(&mut self, address: I) -> Result<Self::PlaneStore, StoreError> {
        let ServerStore { inner } = self;
        let address = address.to_string();

        let result = match inner.get(&address) {
            Some(store) => match store.upgrade() {
                Some(store) => Ok(store),
                None => Arc::new(PlaneStoreInner::open(&address)),
            },
            None => Arc::new(PlaneStoreInner::open(&address)),
        };

        match result {
            Ok(store) => {
                let weak = Arc::downgrade(&store);
                inner.insert(address, weak);
                Ok(PlaneStore::from_inner(store))
            }
            Err(_) => {}
        }
    }
}
