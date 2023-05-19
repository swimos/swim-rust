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

use crate::{KvBytes, StoreError};

/// An key representing a position that an iterator should seek to.
pub enum IteratorKey<'p> {
    /// Seek to the first key.
    Start,
    /// Seek to the last key.
    End,
    /// Seek to a key or the one that lexicographically follows it.
    ToKey(&'p [u8]),
}

/// An iterator over a keyspace.
pub trait EngineIterator {
    /// Seek to the first key.
    fn seek_first(&mut self) -> Result<bool, StoreError> {
        self.seek_to(IteratorKey::Start)
    }

    /// Seek to the last key.
    fn seek_end(&mut self) -> Result<bool, StoreError> {
        self.seek_to(IteratorKey::End)
    }

    /// Seek to a key or the one that lexicographically follows it.
    fn seek_to(&mut self, key: IteratorKey) -> Result<bool, StoreError>;

    /// Seek to the next key that lexicographically follows the current.
    fn seek_next(&mut self);

    /// Returns the current key if the cursor points to a valid element.
    fn key(&self) -> Option<&[u8]>;

    /// Returns the current value if the cursor points to a valid element.
    fn value(&self) -> Option<&[u8]>;

    /// Returns `Ok(true)` if the iterator is currently valid and has not reached the end of its
    /// range. `Ok(false)` if the iterator has not encountered any errors but has reached the end of
    /// its range.  Or `Err` with a cause the error.
    fn valid(&self) -> Result<bool, StoreError>;
}

/// An iterator over a range of keys in a keyspace that begin with a specified prefix.
pub trait EnginePrefixIterator {
    /// Returns a tuple containing an optional key and value for the lexicographically following
    /// cursor, or an error. If `None` is returned, the iterator has reached the end of its range.
    fn next(&mut self) -> Option<Result<KvBytes, StoreError>>;
}

// todo: add iterator direction
#[derive(Default)]
pub struct EngineIterOpts;
