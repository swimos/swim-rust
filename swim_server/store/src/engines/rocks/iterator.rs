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
use crate::{EngineRefIterator, IteratorKey, RocksDatabase, StoreError};
use rocksdb::DBRawIterator;

impl<'a: 'b, 'b> EngineRefIterator<'a, 'b> for RocksDatabase {
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
        opts: EngineIterOpts,
        prefix: &'b [u8],
    ) -> Result<Self::EnginePrefixIterator, StoreError> {
        let mut it = self.iterator_opt(space, opts)?;
        it.seek_to(IteratorKey::ToKey(prefix))?;

        Ok(RocksPrefixIterator {
            complete: false,
            delegate: it,
            needle: prefix,
        })
    }
}

pub struct RocksPrefixIterator<'p> {
    complete: bool,
    delegate: RocksIterator<'p>,
    needle: &'p [u8],
}

impl<'d> EnginePrefixIterator for RocksPrefixIterator<'d> {
    fn seek_next(&mut self) {
        self.delegate.seek_next();
    }

    fn next_pair(&mut self) -> (Option<&[u8]>, Option<&[u8]>) {
        let RocksPrefixIterator {
            complete,
            delegate,
            needle,
        } = self;

        if *complete {
            return (None, None);
        }

        match delegate.key() {
            Some(key) if key.starts_with(needle.as_ref()) => (Some(key), delegate.value()),
            None => {
                *complete = true;
                (None, delegate.value())
            }
            _ => {
                *complete = true;
                (None, delegate.value())
            }
        }
    }

    fn key(&mut self) -> Option<&[u8]> {
        let RocksPrefixIterator {
            complete,
            delegate,
            needle,
        } = self;

        if *complete {
            return None;
        }

        let key = delegate.key()?;
        if !key.starts_with(needle.as_ref()) {
            *complete = true;
            None
        } else {
            Some(key)
        }
    }

    fn value(&self) -> Option<&[u8]> {
        self.delegate.value()
    }

    fn valid(&self) -> Result<bool, StoreError> {
        Ok(!self.complete || self.delegate.valid()?)
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
