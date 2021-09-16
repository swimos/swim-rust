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

pub mod engines;
pub mod iterator;
pub mod keyspaces;

use std::error::Error;
use std::fmt::Debug;
use std::path::Path;
use std::{error::Error as StdError, io};

use thiserror::Error;

use crate::engines::FromKeyspaces;
use crate::iterator::OwnedEngineRefIterator;
use crate::keyspaces::{KeyspaceByteEngine, KeyspaceRangedSnapshotLoad, KeyspaceResolver};

pub use rocksdb::{ColumnFamily, MergeOperands, Options, SliceTransform};
use serde::{Deserialize, Serialize};

pub type KvBytes = (Box<[u8]>, Box<[u8]>);

/// Store errors.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The provided key was not found in the store.
    #[error("The specified key was not found")]
    KeyNotFound,
    /// The delegate byte engine failed to initialised.
    #[error("The delegate store engine failed to initialise: {0}")]
    InitialisationFailure(String),
    /// An IO error produced by the delegate byte engine.
    #[error("IO error: {0}")]
    Io(io::Error),
    /// An error produced when attempting to encode a value.
    #[error("Encoding error: {0}")]
    Encoding(String),
    /// An error produced when attempting to decode a value.
    #[error("Decoding error: {0}")]
    Decoding(String),
    /// A raw error produced by the delegate byte engine.
    #[error("Delegate store error: {0}")]
    Delegate(Box<dyn Error + Send + Sync>),
    /// A raw error produced by the delegate byte engine that isnt send or sync
    #[error("Delegate store error: {0}")]
    DelegateMessage(String),
    /// An operation was attempted on the byte engine when it was closing.
    #[error("An operation was attempted on the delegate store engine when it was closing")]
    Closing,
    /// The requested keyspace was not found.
    #[error("The requested keyspace was not found")]
    KeyspaceNotFound,
}

impl StoreError {
    pub fn downcast_ref<E: StdError + 'static>(&self) -> Option<&E> {
        match self {
            StoreError::Delegate(d) => {
                if let Some(ref downcasted) = d.downcast_ref() {
                    return Some(downcasted);
                }
                None
            }
            _ => None,
        }
    }
}

impl PartialEq for StoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StoreError::KeyNotFound, StoreError::KeyNotFound) => true,
            (StoreError::InitialisationFailure(l), StoreError::InitialisationFailure(r)) => l.eq(r),
            (StoreError::Io(l), StoreError::Io(r)) => l.kind().eq(&r.kind()),
            (StoreError::Encoding(l), StoreError::Encoding(r)) => l.eq(r),
            (StoreError::Decoding(l), StoreError::Decoding(r)) => l.eq(r),
            (StoreError::Delegate(l), StoreError::Delegate(r)) => l.to_string().eq(&r.to_string()),
            (StoreError::DelegateMessage(l), StoreError::DelegateMessage(r)) => l.eq(r),
            (StoreError::Closing, StoreError::Closing) => true,
            (StoreError::KeyspaceNotFound, StoreError::KeyspaceNotFound) => true,
            _ => false,
        }
    }
}

/// A Swim server store.
///
/// This trait only serves to compose the multiple traits that are required for a store.
pub trait Store:
    KeyspaceByteEngine
    + KeyspaceRangedSnapshotLoad
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

/// Information regarding a delegate store engine that is useful for displaying along with debug
/// information or an error report.
#[derive(Debug, PartialEq, Clone)]
pub struct EngineInfo {
    /// The path that the store engine is operating from.
    pub path: String,
    /// The type of store engine: RocksDB/NoStore etc.
    pub kind: String,
}

pub fn serialize_then<S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    f(engine, serialize(obj)?).map_err(Into::into)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(|e| StoreError::Encoding(e.to_string()))
}

pub fn deserialize<'de, D: Deserialize<'de>>(obj: &'de [u8]) -> Result<D, StoreError> {
    bincode::deserialize(obj).map_err(|e| StoreError::Decoding(e.to_string()))
}

pub fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<u64, StoreError> {
    bincode::deserialize::<u64>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}
