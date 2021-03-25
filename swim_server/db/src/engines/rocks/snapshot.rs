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

use crate::engines::rocks::RocksDatabase;
use crate::{RangedSnapshot, StoreError};
use std::vec::IntoIter;

pub type RocksSnapshotIter = IntoIter<(Vec<u8>, Vec<u8>)>;

impl RangedSnapshot for RocksDatabase {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type RangedSnapshot = RocksRangedSnapshot;
    type Prefix = Vec<u8>;

    fn ranged_snapshot(
        &self,
        prefix: Self::Prefix,
    ) -> Result<Option<Self::RangedSnapshot>, StoreError> {
        let db = &*self.delegate;
        let mut raw = db.raw_iterator();
        let mut data = Vec::new();

        raw.seek(prefix.clone());

        loop {
            if raw.valid() {
                match (raw.key(), raw.value()) {
                    (Some(key), Some(value)) => {
                        if !key.starts_with(&prefix) {
                            break;
                        } else {
                            data.push((key.to_vec(), value.to_vec()));
                            raw.next();
                        }
                    }
                    _ => panic!(),
                }
            } else {
                break;
            }
        }

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RocksRangedSnapshot { data }))
        }
    }
}

pub struct RocksRangedSnapshot {
    data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl IntoIterator for RocksRangedSnapshot {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = RocksSnapshotIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
