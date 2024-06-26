// Copyright 2015-2024 Swim Inc.
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

use rocksdb::DBRawIterator;
use swimos_api::{
    error::StoreError,
    persistence::{KeyValue, RangeConsumer},
};

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
