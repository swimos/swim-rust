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

use crate::engines::keyspaces::KeyspaceResolver;
use crate::StoreError;

pub trait OwnedEngineRefIterator: for<'t> EngineRefIterator<'t, 't> {}
impl<D> OwnedEngineRefIterator for D where D: for<'t> EngineRefIterator<'t, 't> {}

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

    /// Returns `Ok` if the iterator is currently valid and has not reached the end of its range.
    /// Or `Err` with a cause the error.
    fn valid(&self) -> Result<bool, StoreError>;
}

/// An iterator over a range of keys in a keyspace that begin with a specified prefix.
pub trait EnginePrefixIterator {
    /// Seek to the next key that lexicographically follows the current and begins with the
    /// specified prefix.
    fn seek_next(&mut self);

    /// Returns a tuple containing an optional key and value for the current cursor. Generally,
    /// for one of the elements to be `None`, its considered that the database is inconsistent.
    fn next_pair(&mut self) -> (Option<&[u8]>, Option<&[u8]>);

    /// Returns the current key if the cursor points to a valid element.
    fn key(&mut self) -> Option<&[u8]>;

    /// Returns the current value if the cursor points to a valid element.
    fn value(&self) -> Option<&[u8]>;

    /// Returns `Ok` if the iterator is currently valid and has not reached the end of its range.
    /// Or `Err` with a cause the error.
    fn valid(&self) -> Result<bool, StoreError>;
}

// todo: add iterator direction
#[derive(Default)]
pub struct EngineIterOpts;

/// A trait for creating iterators over a keyspace.
pub trait EngineRefIterator<'a: 'b, 'b>: KeyspaceResolver {
    type EngineIterator: EngineIterator;
    type EnginePrefixIterator: EnginePrefixIterator;

    /// Returns an iterator for all of the elements in the keyspace `space` with the default
    /// iterator options.
    fn iterator(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
    ) -> Result<Self::EngineIterator, StoreError> {
        self.iterator_opt(space, EngineIterOpts::default())
    }

    /// Returns an iterator for all of the elements in the keyspace `space` with the provided
    /// options.
    fn iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError>;

    /// Returns an iterator for all of the elements in the keyspace `space` that have keys that are
    /// prefixed by `prefix` and with the default iterator options.
    fn prefix_iterator(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        self.prefix_iterator_opt(space, EngineIterOpts::default(), prefix)
    }

    /// Returns an iterator for all of the elements in the keyspace `space` that have keys that are
    /// prefixed by `prefix` and with the provided iterator options.
    fn prefix_iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        opts: EngineIterOpts,
        prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError>;
}
