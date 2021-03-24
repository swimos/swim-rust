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

use crate::engines::lmdbx::{LmdbxDatabase, LmdbxOpts, LmdbxSnapshot};
use crate::engines::rocks::RocksDatabase;
use crate::{FromOpts, Iterable, StoreEngine, StoreError, StoreInitialisationError};
use std::fmt::{Debug, Formatter};
use std::path::Path;

#[cfg(feature = "libmdbx")]
pub mod lmdbx;
#[cfg(feature = "rocks-db")]
pub mod rocks;

#[derive(Clone)]
pub enum StoreDelegate {
    #[cfg(feature = "libmdbx")]
    Lmdbx(LmdbxDatabase),
    #[cfg(feature = "rocks-db")]
    Rocksdb(RocksDatabase),
}

impl FromOpts for StoreDelegate {
    type Opts = StoreDelegateConfig;

    fn from_opts<I: AsRef<Path>>(
        path: I,
        opts: &Self::Opts,
    ) -> Result<Self, StoreInitialisationError> {
        match opts {
            StoreDelegateConfig::Lmdbx(opts) => {
                LmdbxDatabase::from_opts(path, opts).map(StoreDelegate::Lmdbx)
            }
            StoreDelegateConfig::Rocksdb(opts) => {
                RocksDatabase::from_opts(path, opts).map(StoreDelegate::Rocksdb)
            }
        }
    }
}

#[derive(Clone)]
pub enum StoreDelegateConfig {
    #[cfg(feature = "libmdbx")]
    Lmdbx(LmdbxOpts),
    #[cfg(feature = "rocks-db")]
    Rocksdb(rocksdb::Options),
}

impl Debug for StoreDelegateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreDelegateConfig").finish()
    }
}

macro_rules! gated_arm {
    ($self:ident, $($op:tt)*) => {
        match $self {
            #[cfg(feature = "libmdbx")]
            StoreDelegate::Lmdbx(db) => db.$($op)*.map_err(Into::into),
            #[cfg(feature = "rocks-db")]
            StoreDelegate::Rocksdb(db) => db.$($op)*.map_err(Into::into),
        }
    };
}

impl<'i> StoreEngine<'i> for StoreDelegate {
    type Key = &'i [u8];
    type Value = &'i [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        gated_arm!(self, put(key, value))
    }

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        gated_arm!(self, get(key))
    }

    fn delete(&self, key: Self::Key) -> Result<bool, Self::Error> {
        gated_arm!(self, delete(key))
    }
}

pub enum StoreSnapshot<'a> {
    #[cfg(feature = "libmdbx")]
    Lmdbx(LmdbxSnapshot),
    #[cfg(feature = "rocks-db")]
    Rocksdb(rocksdb::Snapshot<'a>),
}

impl<'a> Iterable for StoreSnapshot<'a> {
    type Iterator = StoreSnapshotIterator;
}

pub struct StoreSnapshotIterator;
impl Iterator for StoreSnapshotIterator {
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
