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

use rocksdb::{DBIteratorWithThreadMode, DBRawIterator, DBWithThreadMode, SingleThreaded};
use store_common::{EngineIterator, EnginePrefixIterator, IteratorKey, KeyValue, KvBytes};
use store_common::{RangeConsumer, StoreError};

pub struct RocksPrefixIterator<'p> {
    delegate: DBIteratorWithThreadMode<'p, DBWithThreadMode<SingleThreaded>>,
}

impl<'p> From<DBIteratorWithThreadMode<'p, DBWithThreadMode<SingleThreaded>>>
    for RocksPrefixIterator<'p>
{
    fn from(delegate: DBIteratorWithThreadMode<'p, DBWithThreadMode<SingleThreaded>>) -> Self {
        RocksPrefixIterator { delegate }
    }
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

impl<'d> From<DBRawIterator<'d>> for RocksIterator<'d> {
    fn from(iter: DBRawIterator<'d>) -> Self {
        RocksIterator { iter }
    }
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
    fn consume_next(&mut self) -> Result<Option<KeyValue<'_>>, StoreError> {
        let RocksRawPrefixIterator { iter, first } = self;
        if *first {
            *first = false;
        } else {
            iter.next();
        }
        if let Some(kv) = iter.item() {
            Ok(Some(kv))
        } else if let Err(e) = iter.status() {
            Err(StoreError::Delegate(Box::new(e)))
        } else {
            Ok(None)
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
