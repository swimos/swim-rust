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

use crate::engines::lmdbx::{LmdbxDatabase, LmdbxDatabaseInner};
use crate::{RangedSnapshot, StoreError};
use heed::Error;
use std::vec::IntoIter;

pub type LmdbxSnapshotIter = IntoIter<(Vec<u8>, Vec<u8>)>;

impl RangedSnapshot for LmdbxDatabase {
    type RangedSnapshot = LmdbxRangedSnapshot;
    type Prefix = Vec<u8>;

    fn ranged_snapshot<'i, F, K, V>(
        &self,
        prefix: Self::Prefix,
        map_fn: F,
    ) -> Result<Option<Self::RangedSnapshot>, StoreError>
    where
        F: Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        let LmdbxDatabaseInner { delegate, env, .. } = &*self.inner;
        let tx = env.read_txn()?;

        let mut it = delegate
            .prefix_iter(&tx, &*prefix)
            .map_err(|e| StoreError::Error(e.to_string()))?;

        let data = it.try_fold(Vec::new(), |mut vec, result| match result {
            Ok((key, value)) => {
                let (key, value) = map_fn(key, value)?;
                vec.push((key, value));

                Ok(vec)
            }
            Err(e) => Err(StoreError::Error(e.to_string())),
        })?;

        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(LmdbxRangedSnapshot { data }))
        }
    }
}

pub struct LmdbxRangedSnapshot {
    data: Vec<(Vec<u8>, Vec<u8>)>,
}

impl IntoIterator for LmdbxRangedSnapshot {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = LmdbxSnapshotIter;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
