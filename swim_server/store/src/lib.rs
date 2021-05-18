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
mod transient;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;
use std::path::Path;
use std::{error::Error as StdError, io};

use thiserror::Error;

use crate::engines::FromKeyspaces;
use crate::iterator::OwnedEngineRefIterator;
use crate::keyspaces::{KeyType, KeyspaceByteEngine, KeyspaceRangedSnapshotLoad, KeyspaceResolver};

pub use rocksdb::{ColumnFamily, MergeOperands, Options, SliceTransform};
pub use transient::TransientDatabase;

pub type KvBytes = (Box<[u8]>, Box<[u8]>);

/// Store errors.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The provided key was not found in the store.
    #[error("The specified key was not found")]
    KeyNotFound,
    /// An error produced when attempting to execute a snapshot read.
    #[error("An error was produced when attempting to create a snapshot: {0}")]
    Snapshot(String),
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
            (StoreError::Snapshot(l), StoreError::Snapshot(r)) => l.eq(r),
            (StoreError::InitialisationFailure(l), StoreError::InitialisationFailure(r)) => l.eq(r),
            (StoreError::Io(l), StoreError::Io(r)) => l.kind().eq(&r.kind()),
            (StoreError::Encoding(l), StoreError::Encoding(r)) => l.eq(r),
            (StoreError::Decoding(l), StoreError::Decoding(r)) => l.eq(r),
            (StoreError::Delegate(l), StoreError::Delegate(r)) => l.to_string().eq(&r.to_string()),
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
    FromKeyspaces
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

    /// Returns information about the delegate store
    fn store_info(&self) -> StoreInfo;
}

/// A trait for defining engine snapshots.
pub trait Snapshot<K, V> {
    /// The type of the snapshot. An iterator that will yield a deserialized key-value pair.
    type Snapshot: IntoIterator<Item = (K, V)>;

    /// Execute a snapshot on the store engine.
    ///
    /// Returns `Ok(None)` if no records matched `prefix` or `Ok(Some)` if matches were found.
    ///
    /// # Errors
    /// Errors if an error is encountered when attempting to execute the snapshot on the store
    /// engine or if deserializing a key or value fails.
    fn snapshot(&self) -> Result<Option<Self::Snapshot>, StoreError>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct StoreInfo {
    pub path: String,
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

pub fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<KeyType, StoreError> {
    bincode::deserialize::<KeyType>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}
