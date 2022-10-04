// Copyright 2015-2021 Swim Inc.
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

use crate::RocksEngine;
use rocksdb::{DBIteratorWithThreadMode, DBRawIterator, DBWithThreadMode, SingleThreaded};
use store_common::{
    EngineIterOpts, EngineIterator, EnginePrefixIterator, EngineRefIterator, IteratorKey, KvBytes,
};
use store_common::{RangeConsumer, StoreError};

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
    fn next(&mut self) -> Option<Result<KvBytes, StoreError>> {
        self.delegate
            .next()
            .map(|r| r.map_err(|e| StoreError::Delegate(Box::new(e))))
    }
}

pub struct RocksIterator<'d> {
    iter: DBRawIterator<'d>,
}

pub struct RocksRawPrefixIterator<'d> {
    first: bool,
    iter: DBRawIterator<'d>,
}

impl<'d> RocksRawPrefixIterator<'d> {
    pub fn new(iter: DBRawIterator<'d>) -> Self {
        RocksRawPrefixIterator { first: true, iter }
    }
}

impl<'d> RangeConsumer for RocksRawPrefixIterator<'d> {
    fn consume_next<'a>(&'a mut self) -> Result<Option<(&'a [u8], &'a [u8])>, StoreError> {
        let RocksRawPrefixIterator { iter, first } = self;
        if *first {
            *first = false;
        } else {
            iter.next();
        }
        if let Some(kv) = iter.item() {
            Ok(Some(kv))
        } else {
            if let Err(e) = iter.status() {
                Err(StoreError::Delegate(Box::new(e)))
            } else {
                Ok(None)
            }
        }
    }
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
            self.iter
                .status()
                .map_err(|e| StoreError::Delegate(Box::new(e)))?;
        }
        Ok(self.iter.valid())
    }
}
