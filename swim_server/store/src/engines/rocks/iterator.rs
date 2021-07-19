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

use crate::iterator::{EngineIterOpts, EngineIterator, EnginePrefixIterator};
use crate::{EngineRefIterator, IteratorKey, KvPair, RocksEngine, StoreError};
use rocksdb::{DBIteratorWithThreadMode, DBRawIterator, DBWithThreadMode, SingleThreaded};

impl<'a: 'b, 'b> EngineRefIterator<'a, 'b> for RocksEngine {
    type EngineIterator = RocksIterator<'b>;
    type EnginePrefixIterator = RocksPrefixIterator<'b>;

    fn iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        _opts: EngineIterOpts,
    ) -> Result<Self::EngineIterator, StoreError> {
        let mut iter = self.delegate.raw_iterator_cf(space);
        iter.seek_to_first();

        Ok(RocksIterator { iter })
    }

    fn prefix_iterator_opt(
        &'a self,
        space: &'b Self::ResolvedKeyspace,
        _opts: EngineIterOpts,
        prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        let it = self.delegate.prefix_iterator_cf(space, prefix);
        Ok(RocksPrefixIterator { delegate: it })
    }
}

pub struct RocksPrefixIterator<'p> {
    delegate: DBIteratorWithThreadMode<'p, DBWithThreadMode<SingleThreaded>>,
}

impl<'d> EnginePrefixIterator for RocksPrefixIterator<'d> {
    fn next(&mut self) -> KvPair {
        self.delegate.next()
    }

    fn valid(&self) -> Result<bool, StoreError> {
        if self.delegate.valid() {
            Ok(true)
        } else {
            match self.delegate.status() {
                Ok(_) => Ok(false),
                Err(e) => Err(StoreError::from(e)),
            }
        }
    }
}

pub struct RocksIterator<'d> {
    iter: DBRawIterator<'d>,
}

impl<'d> EngineIterator for RocksIterator<'d> {
    fn seek_to(&mut self, key: IteratorKey) -> Result<bool, StoreError> {
        match key {
            IteratorKey::Start => self.iter.seek_to_first(),
            IteratorKey::End => self.iter.seek_to_last(),
            IteratorKey::ToKey(key) => self.iter.seek(key),
        }
        self.valid()
    }

    fn seek_next(&mut self) {
        self.iter.next();
    }

    fn key(&self) -> Option<&[u8]> {
        self.iter.key()
    }

    fn value(&self) -> Option<&[u8]> {
        self.iter.value()
    }

    fn valid(&self) -> Result<bool, StoreError> {
        if !self.iter.valid() {
            self.iter.status().map_err(StoreError::from)?;
        }
        Ok(self.iter.valid())
    }
}
